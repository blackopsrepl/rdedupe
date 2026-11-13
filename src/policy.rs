use crate::error::{Error, Result};
use crate::model::FileRecord;
use crate::paths::stable_cmp;
use crate::store::Store;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};

/// Deterministic keeper policy. Protection and preference match whole path components, so
/// a protected `/data/keep` never covers `/data/keeper`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Policy {
    /// Members under these canonical roots are never action targets.
    pub protect: Vec<PathBuf>,
    /// Keeper preference order among unprotected roots (earlier wins).
    pub prefer: Vec<PathBuf>,
    /// Prefer the most recently modified member before falling back to native path order.
    pub prefer_newest: bool,
}

impl Policy {
    pub fn is_protected(&self, path: &Path) -> bool {
        self.protect.iter().any(|root| path.starts_with(root))
    }

    fn rank(&self, path: &Path) -> usize {
        self.prefer
            .iter()
            .position(|root| path.starts_with(root))
            .unwrap_or(usize::MAX)
    }

    /// Total order on group members: protected first, then preferred root order, then
    /// (optionally) newest modification time, then native path bytes. The keeper is the
    /// minimum.
    pub fn compare(&self, a: &FileRecord, b: &FileRecord) -> Ordering {
        self.is_protected(&b.path)
            .cmp(&self.is_protected(&a.path))
            .then_with(|| self.rank(&a.path).cmp(&self.rank(&b.path)))
            .then_with(|| {
                if self.prefer_newest {
                    b.observation.modified_ns.cmp(&a.observation.modified_ns)
                } else {
                    Ordering::Equal
                }
            })
            .then_with(|| stable_cmp(&a.path, &b.path))
    }

    /// Streams the group once and returns the keeper with the reason it was chosen.
    pub fn select_streaming(&self, store: &Store, group: i64) -> Result<(FileRecord, String)> {
        let mut best: Option<FileRecord> = None;
        store.for_each_member(group, |member| {
            if best
                .as_ref()
                .is_none_or(|current| self.compare(member, current) == Ordering::Less)
            {
                best = Some(member.clone());
            }
            Ok(())
        })?;
        let keeper = best.ok_or_else(|| Error::Plan("empty duplicate group".into()))?;
        let reason = if self.is_protected(&keeper.path) {
            "protected root"
        } else if self.rank(&keeper.path) != usize::MAX {
            "preferred root"
        } else if self.prefer_newest {
            "newest modification time, then native path order"
        } else {
            "native path order"
        }
        .to_string();
        Ok((keeper, reason))
    }
}
