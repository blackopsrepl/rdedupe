use crate::paths::EncodedPath;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub type FileId = i64;
pub type GroupId = i64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
}

/// Metadata observed for one filesystem object. Equality of two observations of the same
/// path is the change-detection test used during reads and before every mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Observation {
    pub identity: FileIdentity,
    pub size: u64,
    pub allocated: u64,
    pub modified_ns: i128,
    pub changed_ns: i128,
    pub mode: u32,
    pub links: u64,
    /// Owner and group (0 where the platform has none).
    #[serde(default)]
    pub uid: u32,
    #[serde(default)]
    pub gid: u32,
    /// Creation (birth) time where the filesystem records it. A file that merely reuses a
    /// freed inode number has a different one.
    #[serde(default)]
    pub created_ns: Option<i128>,
}

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub id: FileId,
    pub path: PathBuf,
    pub observation: Observation,
    pub alias_of: Option<FileId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    Complete,
    Partial,
    Cancelled,
    Failed,
}

/// Whether duplicate groups rest only on current, complete byte comparison (`Exact`) or
/// whether trusted cache fingerprints may have eliminated candidates (`Heuristic`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Exactness {
    Exact,
    Heuristic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Unresolved {
    pub path: EncodedPath,
    pub operation: String,
    pub message: String,
    pub retry_safe: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Metrics {
    pub paths: u64,
    pub objects: u64,
    pub aliases: u64,
    pub symlinks_skipped: u64,
    pub special_files_skipped: u64,
    /// rdedupe-owned directories (marked quarantines, the scratch workspace) not scanned.
    pub owned_dirs_skipped: u64,
    /// Scratch directories left by dead rdedupe processes and removed by this run.
    pub abandoned_scratch_removed: u64,
    pub candidate_objects: u64,
    pub fingerprint_bytes_read: u64,
    pub comparison_bytes_read: u64,
    /// Bytes read to split sampled-digest buckets of more than two members.
    pub full_hash_bytes_read: u64,
    pub cached_fingerprints_reused: u64,
    pub eliminated_by_size: u64,
    pub eliminated_by_fingerprint: u64,
    /// Candidates that shared a digest bucket but matched no other member byte for byte.
    pub eliminated_by_comparison: u64,
    pub groups: u64,
    pub participants: u64,
    pub eligible_copies: u64,
    pub logical_redundant_bytes: u64,
    pub estimated_allocated_bytes: u64,
    pub sqlite_page_cache_bytes: u64,
    /// Size of the on-disk inventory database at the end of the scan. Larger than
    /// `sqlite_page_cache_bytes` means the inventory did not fit in memory and was paged.
    #[serde(default)]
    pub inventory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupMember {
    pub record: String,
    pub group: GroupId,
    pub path: EncodedPath,
    pub identity: FileIdentity,
    pub size: u64,
    pub allocated: u64,
    pub links: u64,
    pub is_keeper: bool,
    pub protected: bool,
    pub reason: String,
    /// Metadata aspects in which this member differs from the keeper (empty for keepers).
    pub metadata_differs_from_keeper: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSummary {
    pub schema: String,
    pub record: String,
    pub binary_version: String,
    pub completeness: Completeness,
    pub exactness: Exactness,
    pub metrics: Metrics,
    pub unresolved: u64,
    pub cache_provenance: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    Quarantine,
    Extent,
}

/// Result of executing one plan action, as streamed to the caller.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Quarantined,
    Shared,
    /// Current evidence no longer matches the plan; nothing was changed.
    SkippedStale,
    /// An earlier action in the same group was refused, so the group was stopped.
    SkippedGroup,
    /// The mutation started but did not complete; the journal records its progress.
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionOutcome {
    pub group: u64,
    pub target: EncodedPath,
    pub kind: ActionKind,
    pub status: ActionStatus,
    pub logical_bytes: u64,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplySummary {
    pub actions: u64,
    pub quarantined: u64,
    pub shared: u64,
    pub skipped: u64,
    pub failed: u64,
    pub logical_bytes_moved_or_shared: u64,
}

impl ApplySummary {
    pub fn record(&mut self, outcome: &ActionOutcome) {
        self.actions += 1;
        match outcome.status {
            ActionStatus::Quarantined => {
                self.quarantined += 1;
                self.logical_bytes_moved_or_shared += outcome.logical_bytes;
            }
            ActionStatus::Shared => {
                self.shared += 1;
                self.logical_bytes_moved_or_shared += outcome.logical_bytes;
            }
            ActionStatus::SkippedStale | ActionStatus::SkippedGroup => self.skipped += 1,
            ActionStatus::Failed => self.failed += 1,
        }
    }
    pub fn complete(&self) -> bool {
        self.skipped == 0 && self.failed == 0
    }
}
