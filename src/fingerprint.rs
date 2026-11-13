use crate::compare::fill;
use crate::error::{io, Error, Result};
use crate::fileinfo::{ensure_stable, open_observed};
use crate::scheduler::Cancellation;
use std::io::{Seek, SeekFrom};
use std::path::Path;

/// Identifier of the sampling layout; part of every fingerprint and of the cache format.
pub const LAYOUT: &str = "blake3-sample-layout-v1";

/// Computes a candidate-partitioning fingerprint. Fingerprints only ever *separate*
/// candidates; equal fingerprints are always followed by a complete byte comparison.
pub trait Fingerprinter: Sync {
    fn fingerprint(
        &self,
        path: &Path,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)>;
}

/// Bytes in each sampled window. Fixed by the layout so fingerprints never depend on the
/// configured buffer size (a cache written under one `--buffer-kib` stays valid under any).
pub const WINDOW: u64 = 64 * 1024;

/// BLAKE3 over the size and up to three [`WINDOW`]-byte windows (start, middle, end), read
/// through the caller's buffer in chunks. Files no larger than one window are read exactly
/// once.
#[derive(Default)]
pub struct Blake3Fingerprinter;

impl Fingerprinter for Blake3Fingerprinter {
    fn fingerprint(
        &self,
        path: &Path,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)> {
        let (mut file, before) = open_observed(path)?;
        let mut hasher = blake3::Hasher::new();
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
        Ok((*hasher.finalize().as_bytes(), read_bytes))
    }
}

/// Test seam: maps every file to the same fingerprint, forcing every same-size candidate
/// into one bucket so that only complete comparison can separate equality classes.
pub struct CollisionFingerprinter;

impl Fingerprinter for CollisionFingerprinter {
    fn fingerprint(
        &self,
        path: &Path,
        _buffer: &mut [u8],
        _cancel: &Cancellation,
    ) -> Result<([u8; 32], u64)> {
        open_observed(path)?;
        Ok(([0; 32], 0))
    }
}
