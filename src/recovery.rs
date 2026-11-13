//! Restore, purge and inspection of action journals.
//!
//! Every decision is made from current filesystem facts, never from the last journal state
//! alone: a `prepared` record whose rename did happen (crash before `applied`) is restorable,
//! and an `applied` record whose object is gone is reported, not assumed.

use crate::compare;
use crate::error::{io, Error, Result};
use crate::journal::{for_each_limited, Journal, JournalEntry, NoHooks, State};
use crate::model::{ActionKind, FileIdentity};
use crate::paths::EncodedPath;
use crate::quarantine;
use crate::scheduler::Cancellation;
use serde::Serialize;
use std::path::Path;

/// Where a quarantine action's object is now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Location {
    /// The validated object is at its original path; the quarantine slot is empty.
    InPlace,
    /// The validated object is in quarantine; the original path is free.
    InQuarantine,
    /// The validated object is in quarantine, but another entry now occupies the original path.
    InQuarantineOriginalOccupied,
    /// The validated object is linked under both names: a link-then-unlink move (used where
    /// no-replace rename is unavailable) was interrupted between its two steps.
    BothNames,
    /// Something other than the validated object is in the quarantine slot.
    QuarantineAltered,
    /// The validated object is nowhere; another entry occupies the original path.
    Replaced,
    /// Neither path holds anything (for example after a purge).
    Absent,
}

fn identity_at(path: &Path) -> Result<Option<FileIdentity>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(crate::platform::observation(&metadata).identity)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io("metadata", path, error)),
    }
}

pub fn locate(entry: &JournalEntry) -> Result<Location> {
    let expected = entry.target_observation.identity;
    let target = identity_at(&entry.target.to_path()?)?;
    let quarantined = match &entry.quarantine {
        Some(path) => identity_at(&path.to_path()?)?,
        None => None,
    };
    Ok(match (quarantined, target) {
        (Some(q), Some(t)) if q == expected && t == expected => Location::BothNames,
        (Some(q), None) if q == expected => Location::InQuarantine,
        (Some(q), Some(_)) if q == expected => Location::InQuarantineOriginalOccupied,
        (Some(_), _) => Location::QuarantineAltered,
        (None, Some(t)) if t == expected => Location::InPlace,
        (None, Some(_)) => Location::Replaced,
        (None, None) => Location::Absent,
    })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RecoverySummary {
    pub actions: u64,
    pub restored: u64,
    pub purged: u64,
    pub already_in_place: u64,
    pub not_quarantined: u64,
    pub missing: u64,
    pub conflicts: u64,
    pub truncated_tail_dropped: bool,
}

impl RecoverySummary {
    pub fn clean(&self) -> bool {
        self.missing == 0 && self.conflicts == 0
    }
}

/// One finding produced while restoring or purging.
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub action: u64,
    pub target: EncodedPath,
    pub location: Location,
    pub result: &'static str,
}

/// Visits the first record of every quarantine action in the journal as it was when opened.
/// Records of one action are contiguous because `apply` executes actions sequentially;
/// `restored`/`purged` records appended by earlier recovery runs are skipped because every
/// decision is re-derived from filesystem facts, which makes recovery idempotent.
fn each_action(
    journal: &mut Journal,
    mut f: impl FnMut(&mut Journal, &JournalEntry) -> Result<()>,
) -> Result<bool> {
    let path = journal.path().to_path_buf();
    let limit = journal.initial_len();
    let mut last_action = 0;
    let tail = for_each_limited(&path, limit, |entry| {
        if entry.kind != ActionKind::Quarantine
            || matches!(entry.state, State::Restored | State::Purged)
            || entry.action == last_action
        {
            return Ok(());
        }
        last_action = entry.action;
        f(journal, entry)
    })?;
    Ok(tail.truncated_tail || journal.dropped_torn_tail())
}

fn record(journal: &mut Journal, entry: &JournalEntry, state: State, message: &str) -> Result<()> {
    let mut next = entry.clone();
    next.state = state;
    next.message = Some(message.into());
    journal.append(next, &NoHooks)
}

/// Moves every quarantined object back to its original path, never replacing an entry that
/// now exists there. Idempotent.
pub fn restore(journal_path: &Path, mut report: impl FnMut(&Finding)) -> Result<RecoverySummary> {
    let mut journal = Journal::open_existing(journal_path)?;
    let mut summary = RecoverySummary::default();
    let truncated = each_action(&mut journal, |journal, entry| {
        summary.actions += 1;
        let location = locate(entry)?;
        let result = match location {
            Location::InQuarantine => {
                let source = entry
                    .quarantine
                    .as_ref()
                    .ok_or_else(|| Error::Journal("quarantine record lacks a path".into()))?
                    .to_path()?;
                quarantine::restore(&source, &entry.target.to_path()?)?;
                record(
                    journal,
                    entry,
                    State::Restored,
                    "moved back from quarantine",
                )?;
                quarantine::prune(&source);
                summary.restored += 1;
                "restored"
            }
            Location::InPlace => {
                summary.already_in_place += 1;
                "already in place"
            }
            Location::BothNames => {
                // The original name still links the object; drop only the extra name.
                let extra = entry
                    .quarantine
                    .as_ref()
                    .ok_or_else(|| Error::Journal("quarantine record lacks a path".into()))?
                    .to_path()?;
                std::fs::remove_file(&extra)
                    .map_err(|e| io("remove extra quarantine name", &extra, e))?;
                crate::platform::sync_parent(&extra)?;
                record(
                    journal,
                    entry,
                    State::Restored,
                    "removed the extra quarantine name",
                )?;
                quarantine::prune(&extra);
                summary.restored += 1;
                "restored (extra name removed)"
            }
            Location::InQuarantineOriginalOccupied | Location::QuarantineAltered => {
                summary.conflicts += 1;
                "conflict: refused"
            }
            Location::Replaced | Location::Absent => {
                summary.missing += 1;
                "missing: not restorable"
            }
        };
        report(&Finding {
            action: entry.action,
            target: entry.target.clone(),
            location,
            result,
        });
        Ok(())
    })?;
    summary.truncated_tail_dropped = truncated;
    Ok(summary)
}

/// Permanently removes quarantined objects. Before each removal the keeper must still exist
/// and be byte-identical to the quarantined object; otherwise the purge of that object is
/// refused, because it might be the last copy.
pub fn purge(
    journal_path: &Path,
    buffer_bytes: usize,
    cancel: &Cancellation,
    mut report: impl FnMut(&Finding),
) -> Result<RecoverySummary> {
    let mut journal = Journal::open_existing(journal_path)?;
    let mut summary = RecoverySummary::default();
    let truncated = each_action(&mut journal, |journal, entry| {
        cancel.check()?;
        summary.actions += 1;
        let location = locate(entry)?;
        let result = match location {
            Location::InQuarantine | Location::InQuarantineOriginalOccupied => {
                let object = entry
                    .quarantine
                    .as_ref()
                    .ok_or_else(|| Error::Journal("quarantine record lacks a path".into()))?
                    .to_path()?;
                let keeper = entry.keeper.to_path()?;
                let keeper_ok = match compare::equal(&object, &keeper, buffer_bytes, cancel) {
                    Ok((equal, _)) => {
                        equal && identity_at(&keeper)? != Some(entry.target_observation.identity)
                    }
                    Err(Error::Cancelled) => return Err(Error::Cancelled),
                    Err(_) => false,
                };
                if keeper_ok {
                    std::fs::remove_file(&object)
                        .map_err(|e| io("purge quarantine", &object, e))?;
                    crate::platform::sync_parent(&object)?;
                    record(
                        journal,
                        entry,
                        State::Purged,
                        "removed after keeper verification",
                    )?;
                    quarantine::prune(&object);
                    summary.purged += 1;
                    "purged"
                } else {
                    summary.conflicts += 1;
                    "refused: keeper missing or no longer identical"
                }
            }
            Location::InPlace | Location::BothNames => {
                summary.not_quarantined += 1;
                "not quarantined"
            }
            Location::QuarantineAltered => {
                summary.conflicts += 1;
                "refused: quarantine object altered"
            }
            Location::Replaced | Location::Absent => {
                summary.missing += 1;
                "absent"
            }
        };
        report(&Finding {
            action: entry.action,
            target: entry.target.clone(),
            location,
            result,
        });
        Ok(())
    })?;
    summary.truncated_tail_dropped = truncated;
    Ok(summary)
}

/// One line of `rdedupe recover` output: the last recorded state of a contiguous run of
/// records for one action, together with the facts found now.
#[derive(Debug, Clone, Serialize)]
pub struct Inspection {
    pub action: u64,
    pub group: u64,
    pub kind: ActionKind,
    pub last_state: State,
    pub bytes_completed: u64,
    pub target: EncodedPath,
    pub location: Option<Location>,
    pub assessment: &'static str,
}

fn assess(entry: &JournalEntry, location: Option<Location>) -> &'static str {
    match (entry.kind, entry.state, location) {
        (ActionKind::Extent, State::Committed, _) => "extent sharing completed",
        (ActionKind::Extent, _, _) => {
            "extent sharing incomplete; files are independent and unchanged in content, re-plan to continue"
        }
        (_, State::Prepared | State::Failed, Some(Location::InPlace)) => {
            "not moved; nothing to recover"
        }
        (_, State::Prepared | State::Failed, Some(Location::InQuarantine)) => {
            "moved but not recorded as applied (interrupted); restorable"
        }
        (_, _, Some(Location::InQuarantine)) => "quarantined; restorable or purgeable",
        (_, _, Some(Location::InPlace)) => "in place (restored or never moved)",
        (_, _, Some(Location::BothNames)) => {
            "interrupted move left the object under both names; restore removes the extra name"
        }
        (_, _, Some(Location::InQuarantineOriginalOccupied)) => {
            "quarantined; original path reoccupied, restore will refuse"
        }
        (_, _, Some(Location::QuarantineAltered)) => {
            "indeterminate: quarantine slot holds a different object; inspect manually"
        }
        (_, State::Purged, Some(Location::Absent)) => "purged",
        (_, _, Some(Location::Replaced)) => {
            "indeterminate: original object gone and path reoccupied; inspect manually"
        }
        (_, _, Some(Location::Absent)) => "indeterminate: object absent; inspect manually",
        (_, _, None) => "unknown",
    }
}

/// Streams one [`Inspection`] per contiguous run of action records. Read-only.
pub fn inspect(journal: &Path, mut f: impl FnMut(&Inspection) -> Result<()>) -> Result<bool> {
    let mut current: Option<JournalEntry> = None;
    let mut emit = |entry: &JournalEntry| -> Result<()> {
        let location = match entry.kind {
            ActionKind::Quarantine => Some(locate(entry)?),
            ActionKind::Extent => None,
        };
        f(&Inspection {
            action: entry.action,
            group: entry.group,
            kind: entry.kind,
            last_state: entry.state,
            bytes_completed: entry.bytes_completed,
            target: entry.target.clone(),
            location,
            assessment: assess(entry, location),
        })
    };
    let tail = crate::journal::for_each(journal, |entry| {
        let recovery_record = matches!(entry.state, State::Restored | State::Purged);
        if let Some(previous) = &current {
            if previous.action != entry.action || recovery_record {
                emit(previous)?;
            }
        }
        current = Some(entry.clone());
        Ok(())
    })?;
    if let Some(last) = &current {
        emit(last)?;
    }
    Ok(tail.truncated_tail)
}
