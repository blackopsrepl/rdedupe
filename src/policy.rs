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
    /// A member without a modification time (only possible off Unix) sorts as the oldest.
    pub prefer_newest: bool,
    /// Make empty files eligible copies. Off by default: empty files are often meaningful
    /// markers (`__init__.py`, `.gitkeep`, lock files) and removing them reclaims nothing.
    #[serde(default)]
    pub include_empty: bool,
}

/// How a member's metadata differs from its keeper's: permission bits, owner, group, and
/// extended attributes (including ACLs where the platform stores them as attributes).
/// Content is identical; removing the member keeps only the keeper's metadata.
pub fn metadata_differences(
    member: &FileRecord,
    keeper: &FileRecord,
    keeper_xattrs: Option<[u8; 32]>,
) -> Vec<&'static str> {
    let mut out = Vec::new();
    let (m, k) = (&member.observation, &keeper.observation);
    if m.mode & 0o7777 != k.mode & 0o7777 {
        out.push("permissions");
    }
    if m.uid != k.uid {
        out.push("owner");
    }
    if m.gid != k.gid {
        out.push("group");
    }
    if crate::platform::xattr_digest(&member.path) != keeper_xattrs {
        out.push("extended attributes");
    }
    out
}

impl Policy {
    /// Why a non-keeper member is retained, or `None` when it is an eligible copy.
    pub fn retained_because(&self, member: &FileRecord) -> Option<&'static str> {
        if self.is_protected(&member.path) {
            Some("protected root")
        } else if member.observation.size == 0 && !self.include_empty {
            Some("empty file (use --include-empty to act on empty files)")
        } else {
            None
        }
    }

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
