use crate::compare::fill;
use crate::error::{io, Error, Result};
use crate::fileinfo::{ensure_stable, open_expected};
use crate::model::Observation;
use crate::scheduler::Cancellation;
use std::io::{Seek, SeekFrom};
use std::path::Path;

/// Identifier of the sampling layout; part of every fingerprint and of the cache format.
pub const LAYOUT: &str = "xxh3-128-sample-layout-v1";

/// Whether sampled digests resist deliberately crafted collisions. They do not (xxh3 is
/// chosen for speed), so a sampled bucket that shows more than two equality classes is
/// always refined with the collision-resistant full-content digest, whatever the file
/// size; otherwise crafted collisions could force one comparison per class.
pub const SAMPLES_COLLISION_RESISTANT: bool = false;

/// Computes candidate-partitioning digests. Digests only ever *separate* candidates; equal
/// digests are always followed by a complete byte comparison. Both methods must fail with
/// [`Error::Unstable`] when the object no longer matches its inventory observation.
pub trait Fingerprinter: Sync {
    /// Cheap sampled digest used for every same-size candidate.
    fn fingerprint(
        &self,
        path: &Path,
        expected: &Observation,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)>;

    /// Full-content digest used to split a sampled-digest bucket with more than two
    /// members before byte comparison, so files that differ only in unsampled bytes cost
    /// one extra read each instead of one comparison per equality class.
    fn refine(
        &self,
        path: &Path,
        expected: &Observation,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)> {
        full_digest(path, expected, buffer, cancel)
    }
}

/// Bytes in each sampled window. Fixed by the layout so fingerprints never depend on the
/// configured buffer size (a cache written under one `--buffer-kib` stays valid under any).
pub const WINDOW: u64 = 64 * 1024;

/// xxh3-128 over the layout, the size and up to three [`WINDOW`]-byte windows (start,
/// middle, end), read through the caller's buffer in chunks. Files no larger than one
/// window are read exactly once. Refinement ([`full_digest`]) uses BLAKE3.
#[derive(Default)]
pub struct SampleFingerprinter;

impl Fingerprinter for SampleFingerprinter {
    fn fingerprint(
        &self,
        path: &Path,
        expected: &Observation,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)> {
        let (mut file, before) = open_expected(path, expected)?;
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(LAYOUT.as_bytes());
        hasher.update(&before.size.to_le_bytes());
        let mut read_bytes = 0;
        let mut positions = vec![0];
        if before.size > WINDOW {
            positions.push((before.size - WINDOW) / 2);
            positions.push(before.size - WINDOW);
        }
        positions.sort_unstable();
        positions.dedup();
        for position in positions {
            cancel.check()?;
            file.seek(SeekFrom::Start(position))
                .map_err(|e| io("seek", path, e))?;
            hasher.update(&position.to_le_bytes());
            let mut wanted = (before.size - position).min(WINDOW) as usize;
            while wanted > 0 {
                let chunk = wanted.min(buffer.len());
                let filled =
                    fill(&mut file, &mut buffer[..chunk]).map_err(|e| io("read", path, e))?;
                if filled != chunk {
                    return Err(Error::Unstable(format!(
                        "{path:?} ended early while fingerprinting"
                    )));
                }
                hasher.update(&buffer[..filled]);
                read_bytes += filled as u64;
                wanted -= filled;
            }
        }
        ensure_stable(path, &before, &file)?;
        let mut digest = [0u8; 32];
        digest[..16].copy_from_slice(&hasher.digest128().to_le_bytes());
        Ok((digest, read_bytes))
    }
}

/// BLAKE3 (collision resistant) of the complete content, streamed through `buffer`.
pub fn full_digest(
    path: &Path,
    expected: &Observation,
    buffer: &mut [u8],
    cancel: &Cancellation,
) -> Result<([u8; 32], u64)> {
    let (mut file, before) = open_expected(path, expected)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"rdedupe-full-content-v1");
    let mut read_bytes = 0u64;
    loop {
        cancel.check()?;
        let filled = fill(&mut file, buffer).map_err(|e| io("read", path, e))?;
        hasher.update(&buffer[..filled]);
        read_bytes += filled as u64;
        if filled < buffer.len() {
            break;
        }
    }
    if read_bytes != before.size {
        return Err(Error::Unstable(format!(
            "{path:?} changed size while being read"
        )));
    }
    ensure_stable(path, &before, &file)?;
    Ok((*hasher.finalize().as_bytes(), read_bytes))
}

/// Test seam: maps every file to the same digests, forcing every same-size candidate into
/// one bucket so that only complete comparison can separate equality classes.
pub struct CollisionFingerprinter;

impl Fingerprinter for CollisionFingerprinter {
    fn fingerprint(
        &self,
        path: &Path,
        expected: &Observation,
        _buffer: &mut [u8],
        _cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)> {
        open_expected(path, expected)?;
        Ok(([0; 32], 0))
    }

    fn refine(
        &self,
        path: &Path,
        expected: &Observation,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)> {
        self.fingerprint(path, expected, buffer, cancel)
    }
}
