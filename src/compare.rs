use crate::error::{io, Error, Result};
use crate::fileinfo::{ensure_stable, open_expected, open_observed};
use crate::model::Observation;
use crate::scheduler::Cancellation;
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

/// Like [`equal`], for two objects of one size class that must still be exactly as
/// inventoried. A change since inventory (including a size change) is an
/// [`crate::Error::Unstable`] error, never a "different" verdict.
pub fn equal_expected(
    a: (&Path, &Observation),
    b: (&Path, &Observation),
    buffer_bytes: usize,
    cancel: &Cancellation,
) -> Result<(bool, u64)> {
    if a.1.size != b.1.size {
        // Different inventoried sizes: different content, without reading.
        return Ok((false, 0));
    }
    let (left, left_before) = open_expected(a.0, a.1)?;
    let (right, right_before) = open_expected(b.0, b.1)?;
    if left_before.size != right_before.size {
        return Err(Error::Unstable(
            "objects of one size class differ in size".into(),
        ));
    }
    compare_open(
        (a.0, left, &left_before),
        (b.0, right, &right_before),
        buffer_bytes,
        cancel,
    )
}

fn compare_open(
    (a, mut left, left_before): (&Path, File, &Observation),
    (b, mut right, right_before): (&Path, File, &Observation),
    buffer_bytes: usize,
    cancel: &Cancellation,
) -> Result<(bool, u64)> {
    let half = (buffer_bytes / 2).max(4096);
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
