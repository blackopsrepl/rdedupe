use crate::error::{io, Error, Result};
use crate::fileinfo::{ensure_stable, open_expected, open_observed};
use crate::model::Observation;
use crate::scheduler::{BufferPool, Cancellation};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Compares two files byte for byte. Returns whether they are equal and how many bytes were
/// read. Both files must remain unchanged (by `fstat`) for the whole comparison; otherwise
/// an [`crate::Error::Unstable`] error is returned rather than a verdict.
pub fn equal(
    a: &Path,
    b: &Path,
    buffer_bytes: usize,
    cancel: &Cancellation,
) -> Result<(bool, u64)> {
    let (left, left_before) = open_observed(a)?;
    let (right, right_before) = open_observed(b)?;
    if left_before.size != right_before.size {
        return Ok((false, 0));
    }
    compare_open(
        (a, left, &left_before),
        (b, right, &right_before),
        buffer_bytes,
        cancel,
    )
}

/// Outcome of comparing one member against a representative in [`equal_many`].
#[derive(Debug)]
pub enum Verdict {
    Equal,
    Different,
    /// The member could not be compared (unreadable, or changed since inventory).
    Failed(Error),
}

/// Compares one representative against several members of its size class in a single pass
/// over the representative: every file is read at most once, chunk by chunk in lockstep,
/// and a member drops out at its first differing chunk. Every member must still match its
/// inventory observation, before and after; a member that does not is
/// [`Verdict::Failed`], never "different". An error on the representative itself fails
/// the whole call. Returns one verdict per member and the bytes read.
pub fn equal_many(
    representative: (&Path, &Observation),
    members: &[(&Path, &Observation)],
    pool: &BufferPool,
    cancel: &Cancellation,
) -> Result<(Vec<Verdict>, u64)> {
    let mut verdicts: Vec<Option<Verdict>> = members.iter().map(|_| None).collect();
    let size = representative.1.size;
    // Each pooled buffer is split into two halves, one per open file, so a batch of
    // `readers + 1` members plus the representative fits in the pool's `readers + 1`
    // buffers. One byte more than the file needs is enough to see its end in one read.
    let chunk = (pool.buffer_size() / 2).min(
        usize::try_from(size)
            .unwrap_or(usize::MAX)
            .saturating_add(1),
    );
    let mut leases: Vec<_> = (0..(members.len() + 1).div_ceil(2))
        .map(|_| pool.take())
        .collect();
    let mut halves = leases.iter_mut().flat_map(|lease| {
        let (a, b) = lease.split_at_mut(pool.buffer_size() / 2);
        [&mut a[..chunk], &mut b[..chunk]]
    });
    let buffer = halves.next().expect("representative buffer");
    struct Live<'a, 'b> {
        index: usize,
        path: &'a Path,
        file: File,
        before: Observation,
        buffer: &'b mut [u8],
    }
    let mut live = Vec::with_capacity(members.len());
    for (index, (path, expected)) in members.iter().enumerate() {
        if expected.size != size {
            verdicts[index] = Some(Verdict::Different);
            continue;
        }
        match open_expected(path, expected) {
            Ok((file, before)) => live.push(Live {
                index,
                path,
                file,
                before,
                buffer: halves.next().expect("member buffer"),
            }),
            Err(error) => verdicts[index] = Some(Verdict::Failed(error)),
        }
    }
    let mut total = 0u64;
    if !live.is_empty() {
        let (mut file, before) = open_expected(representative.0, representative.1)?;
        loop {
            cancel.check()?;
            let read = fill(&mut file, buffer).map_err(|e| io("read", representative.0, e))?;
            total += read as u64;
            live.retain_mut(|member| {
                match fill(&mut member.file, member.buffer) {
                    Ok(got) => {
                        total += got as u64;
                        if got == read && member.buffer[..got] == buffer[..read] {
                            return true;
                        }
                        verdicts[member.index] = Some(
                            match ensure_stable(member.path, &member.before, &member.file) {
                                Ok(()) => Verdict::Different,
                                Err(error) => Verdict::Failed(error),
                            },
                        );
                    }
                    Err(error) => {
                        verdicts[member.index] =
                            Some(Verdict::Failed(io("read", member.path, error)))
                    }
                }
                false
            });
            if live.is_empty() || read < chunk {
                break;
            }
        }
        ensure_stable(representative.0, &before, &file)?;
        for member in live {
            verdicts[member.index] = Some(
                match ensure_stable(member.path, &member.before, &member.file) {
                    Ok(()) => Verdict::Equal,
                    Err(error) => Verdict::Failed(error),
                },
            );
        }
    }
    Ok((
        verdicts
            .into_iter()
            .map(|v| v.expect("every member has a verdict"))
            .collect(),
        total,
    ))
}

fn compare_open(
    (a, mut left, left_before): (&Path, File, &Observation),
    (b, mut right, right_before): (&Path, File, &Observation),
    buffer_bytes: usize,
    cancel: &Cancellation,
) -> Result<(bool, u64)> {
    // One byte more than the file needs is enough to see its end in a single read; small
    // files must not pay for zeroing two full-size buffers on every comparison.
    let needed = usize::try_from(left_before.size)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    let half = (buffer_bytes / 2).max(4096).min(needed);
    let mut a_buf = vec![0; half];
    let mut b_buf = vec![0; half];
    let mut total = 0;
    let equal = loop {
        cancel.check()?;
        let an = fill(&mut left, &mut a_buf).map_err(|e| io("read", a, e))?;
        let bn = fill(&mut right, &mut b_buf).map_err(|e| io("read", b, e))?;
        total += (an + bn) as u64;
        if an != bn || a_buf[..an] != b_buf[..bn] {
            break false;
        }
        if an < half {
            break true;
        }
    };
    ensure_stable(a, left_before, &left)?;
    ensure_stable(b, right_before, &right)?;
    Ok((equal, total))
}

/// Reads until `buffer` is full or end of file, retrying interrupted and short reads, so a
/// short read can never be mistaken for a content difference or for end of file.
pub fn fill(reader: &mut impl Read, buffer: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::fill;
    use std::io::{self, Read};

    /// Returns at most `step` bytes per call and an `Interrupted` error every other call.
    struct Choppy<'a> {
        data: &'a [u8],
        step: usize,
        calls: usize,
    }

    impl Read for Choppy<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls % 2 == 0 {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "injected"));
            }
            let count = self.step.min(buffer.len()).min(self.data.len());
            buffer[..count].copy_from_slice(&self.data[..count]);
            self.data = &self.data[count..];
            Ok(count)
        }
    }

    #[test]
    fn short_and_interrupted_reads_fill_the_buffer() {
        let data: Vec<u8> = (0..10_000u32).map(|v| v as u8).collect();
        for step in [1, 7, 4096] {
            let mut reader = Choppy {
                data: &data,
                step,
                calls: 0,
            };
            let mut out = vec![0; 8192];
            assert_eq!(fill(&mut reader, &mut out).unwrap(), 8192);
            assert_eq!(out, data[..8192]);
            let mut rest = vec![0; 8192];
            assert_eq!(fill(&mut reader, &mut rest).unwrap(), 10_000 - 8192);
            assert_eq!(rest[..10_000 - 8192], data[8192..]);
        }
    }

    #[test]
    fn hard_errors_are_not_end_of_file() {
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("injected EIO"))
            }
        }
        assert!(fill(&mut Failing, &mut [0; 16]).is_err());
    }
}
