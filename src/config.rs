use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Descriptors the process needs besides fingerprint readers: stdin/stdout/stderr, the
/// output file, the SQLite database with its WAL, shared-memory and temporary files, two
/// comparison inputs, the directory walker (capped at [`WALK_OPEN_DIRECTORIES`]) and one
/// directory handle for `fsync`.
pub const FIXED_DESCRIPTORS: usize = 3 + 1 + 4 + 2 + WALK_OPEN_DIRECTORIES + 1;
/// Maximum directory handles the walker keeps open at once.
pub const WALK_OPEN_DIRECTORIES: usize = 2;
/// Allowance for allocator, stack, SQLite statements and page-sized records that are not
/// part of the page cache or the read buffers.
pub const FIXED_MEMORY_BYTES: u64 = 4 * 1024 * 1024;
/// Smallest SQLite page cache the store accepts.
pub const MIN_PAGE_CACHE_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Budget {
    /// Total algorithmic working-set budget: page cache + read buffers + fixed allowance.
    pub memory_bytes: u64,
    /// Size of each read buffer.
    pub buffer_bytes: usize,
    /// Concurrent fingerprint readers.
    pub readers: usize,
    /// Upper bound on simultaneously open descriptors.
    pub descriptors: usize,
    /// Upper bound on the scratch database size.
    pub temp_bytes: u64,
    pub scratch_dir: Option<PathBuf>,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            memory_bytes: 64 * 1024 * 1024,
            buffer_bytes: 256 * 1024,
            readers: 2,
            descriptors: 32,
            temp_bytes: 8 * 1024 * 1024 * 1024,
            scratch_dir: None,
        }
    }
}

impl Budget {
    /// Bytes held by read buffers at peak: one per reader plus one comparison buffer.
    pub fn buffer_total(&self) -> u64 {
        (self.readers as u64 + 1) * self.buffer_bytes as u64
    }

    /// SQLite page-cache size derived from the memory budget after buffers and allowance.
    pub fn page_cache_bytes(&self) -> u64 {
        self.memory_bytes
            .saturating_sub(self.buffer_total())
            .saturating_sub(FIXED_MEMORY_BYTES)
    }

    pub fn required_descriptors(&self) -> usize {
        FIXED_DESCRIPTORS + self.readers
    }

    pub fn validate(&self) -> Result<()> {
        if !(4096..=8 * 1024 * 1024).contains(&self.buffer_bytes) {
            return Err(Error::Config(
                "buffer size must be between 4 KiB and 8 MiB".into(),
            ));
        }
        if self.readers == 0 || self.readers > 64 {
            return Err(Error::Config("readers must be between 1 and 64".into()));
        }
        if self.page_cache_bytes() < MIN_PAGE_CACHE_BYTES {
            return Err(Error::Config(format!(
                "memory budget {} bytes cannot hold {} bytes of read buffers, a {} byte allowance and the minimum {} byte page cache",
                self.memory_bytes,
                self.buffer_total(),
                FIXED_MEMORY_BYTES,
                MIN_PAGE_CACHE_BYTES
            )));
        }
        if self.descriptors < self.required_descriptors() {
            return Err(Error::Config(format!(
                "descriptor budget {} is below the {} descriptors required for {} readers",
                self.descriptors,
                self.required_descriptors(),
                self.readers
            )));
        }
        if self.temp_bytes < 16 * 1024 * 1024 {
            return Err(Error::Config(
                "temporary-storage budget must be at least 16 MiB".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanConfig {
    pub roots: Vec<PathBuf>,
    pub pattern: String,
    pub one_file_system: bool,
    pub budget: Budget,
    /// Fingerprint cache file to refresh after the scan.
    pub cache: Option<PathBuf>,
    /// Ignore an existing cache file (it is still rewritten).
    pub fresh: bool,
    /// Reuse cached fingerprints whose validators match. Makes the scan heuristic: groups
    /// remain byte-verified, but stale cache entries may hide duplicates, so plans refuse it.
    pub trust_cache: bool,
    /// Print stage progress to stderr.
    pub progress: bool,
}
