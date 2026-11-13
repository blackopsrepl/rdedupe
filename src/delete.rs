//! Plan execution.
//!
//! For every action, immediately before mutating anything: the target and keeper must still
//! resolve to the same canonical paths, match the plan's identity and metadata observations,
//! be distinct objects, and be byte-identical *now*. Only then is the intent journaled
//! (synced) and the mutation performed. A refused action changes nothing and stops the rest
//! of its group; a journal failure stops the whole run.

use crate::compare;
use crate::error::{io, Error, Result};
use crate::fileinfo::{open_observed, open_observed_rw};
use crate::journal::{Journal, JournalEntry, JournalHooks, NoHooks, State, SCHEMA};
use crate::model::{ActionKind, ActionOutcome, ActionStatus, ApplySummary};
use crate::plan::{for_each_action, PlanAction};
use crate::quarantine;
use crate::scheduler::Cancellation;
use std::path::Path;

pub fn execute(
    plan: &Path,
    journal: &Path,
    buffer_bytes: usize,
    cancel: &Cancellation,
    sink: impl FnMut(&ActionOutcome) -> Result<()>,
) -> Result<ApplySummary> {
    execute_with_hooks(plan, journal, buffer_bytes, cancel, &NoHooks, sink)
}

pub fn execute_with_hooks(
    plan: &Path,
    journal_path: &Path,
    buffer_bytes: usize,
    cancel: &Cancellation,
    hooks: &dyn JournalHooks,
    mut sink: impl FnMut(&ActionOutcome) -> Result<()>,
) -> Result<ApplySummary> {
    if !(4096..=8 * 1024 * 1024).contains(&buffer_bytes) {
        return Err(Error::Config(
            "buffer size must be between 4 KiB and 8 MiB".into(),
        ));
    }
    // Verify before creating the journal so an invalid plan leaves no artifacts.
    crate::plan::verify_plan(plan)?;
    let mut journal = Journal::create(journal_path)?;
    let mut summary = ApplySummary::default();
    let mut stopped_group: Option<u64> = None;
    for_each_action(plan, |number, header, action| {
        cancel.check()?;
        let outcome = if stopped_group == Some(action.group) {
            outcome(
                &action,
                header.action,
                ActionStatus::SkippedGroup,
                0,
                Some("an earlier action in this group was refused".into()),
            )
        } else {
            match validate_current(&action, buffer_bytes, cancel) {
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(error) => outcome(
                    &action,
                    header.action,
                    ActionStatus::SkippedStale,
                    0,
                    Some(error.to_string()),
                ),
                Ok(()) => match header.action {
                    ActionKind::Quarantine => {
                        quarantine_one(&mut journal, hooks, number, &action, journal_path)?
                    }
                    ActionKind::Extent => extent_one(&mut journal, hooks, number, &action, cancel)?,
                },
            }
        };
        if !matches!(
            outcome.status,
            ActionStatus::Quarantined | ActionStatus::Shared
        ) {
            stopped_group = Some(action.group);
        }
        summary.record(&outcome);
        sink(&outcome)
    })?;
    Ok(summary)
}

fn outcome(
    action: &PlanAction,
    kind: ActionKind,
    status: ActionStatus,
    logical_bytes: u64,
    message: Option<String>,
) -> ActionOutcome {
    ActionOutcome {
        group: action.group,
        target: action.target.clone(),
        kind,
        status,
        logical_bytes,
        message,
    }
}

fn entry(number: u64, action: &PlanAction, kind: ActionKind, state: State) -> JournalEntry {
    JournalEntry {
        schema: SCHEMA.into(),
        sequence: 0,
        action: number,
        group: action.group,
        state,
        kind,
        target: action.target.clone(),
        keeper: action.keeper.clone(),
        quarantine: None,
        target_observation: action.target_observation.clone(),
        bytes_completed: 0,
        message: None,
    }
}

fn quarantine_one(
    journal: &mut Journal,
    hooks: &dyn JournalHooks,
    number: u64,
    action: &PlanAction,
    journal_path: &Path,
) -> Result<ActionOutcome> {
    let target = action.target.to_path()?;
    let destination = match quarantine::destination(&target, journal_path, number) {
        Ok(path) => path,
        Err(error) => {
            return Ok(outcome(
                action,
                ActionKind::Quarantine,
                ActionStatus::SkippedStale,
                0,
                Some(error.to_string()),
            ))
        }
    };
    let mut base = entry(number, action, ActionKind::Quarantine, State::Prepared);
    base.quarantine = Some(crate::paths::EncodedPath::from_path(&destination));
    journal.append(base.clone(), hooks)?;
    match quarantine::move_into(&target, &destination, action.target_observation.identity) {
        Ok(()) => {
            let size = action.target_observation.size;
            let mut applied = base.clone();
            applied.state = State::Applied;
            applied.bytes_completed = size;
            journal.append(applied, hooks)?;
            let mut committed = base;
            committed.state = State::Committed;
            committed.bytes_completed = size;
            journal.append(committed, hooks)?;
            Ok(outcome(
                action,
                ActionKind::Quarantine,
                ActionStatus::Quarantined,
                size,
                None,
            ))
        }
        Err(error) => {
            let mut failed = base;
            failed.state = State::Failed;
            failed.message = Some(error.to_string());
            journal.append(failed, hooks)?;
            Ok(outcome(
                action,
                ActionKind::Quarantine,
                ActionStatus::Failed,
                0,
                Some(error.to_string()),
            ))
        }
    }
}

fn extent_one(
    journal: &mut Journal,
    hooks: &dyn JournalHooks,
    number: u64,
    action: &PlanAction,
    cancel: &Cancellation,
) -> Result<ActionOutcome> {
    let target = action.target.to_path()?;
    let keeper = action.keeper.to_path()?;
    let refused = |message: String| {
        Ok(outcome(
            action,
            ActionKind::Extent,
            ActionStatus::SkippedStale,
            0,
            Some(message),
        ))
    };
    let (source, source_observation) = match open_observed(&keeper) {
        Ok(value) => value,
        Err(error) => return refused(error.to_string()),
    };
    let (destination, destination_observation) = match open_observed_rw(&target) {
        Ok(value) => value,
        Err(error) => return refused(error.to_string()),
    };
    if source_observation.identity != action.keeper_observation.identity
        || destination_observation.identity != action.target_observation.identity
    {
        return refused("keeper or target was replaced after validation".into());
    }
    let base = entry(number, action, ActionKind::Extent, State::Prepared);
    journal.append(base.clone(), hooks)?;
    let size = action.target_observation.size;
    let result = share_ranges(&source, &destination, size, cancel, |done| {
        let mut progress = base.clone();
        progress.state = State::Applied;
        progress.bytes_completed = done;
        journal.append(progress, hooks)
    });
    match result {
        Ok(()) => {
            let mut committed = base;
            committed.state = State::Committed;
            committed.bytes_completed = size;
            journal.append(committed, hooks)?;
            Ok(outcome(
                action,
                ActionKind::Extent,
                ActionStatus::Shared,
                size,
                None,
            ))
        }
        Err(Error::Cancelled) => Err(Error::Cancelled),
        Err(error @ Error::Journal(_)) => Err(error),
        Err(error) => {
            let mut failed = base;
            failed.state = State::Failed;
            failed.message = Some(error.to_string());
            journal.append(failed, hooks)?;
            Ok(outcome(
                action,
                ActionKind::Extent,
                ActionStatus::Failed,
                0,
                Some(error.to_string()),
            ))
        }
    }
}

/// Asks the kernel to compare-and-share `size` bytes in chunks, reporting progress after
/// each chunk. The kernel may share fewer bytes than requested (filesystems cap requests,
/// commonly at 16 MiB); the loop continues from wherever it stopped.
#[cfg(target_os = "linux")]
fn share_ranges(
    source: &std::fs::File,
    destination: &std::fs::File,
    size: u64,
    cancel: &Cancellation,
    mut progress: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    use crate::platform::linux::{dedupe_range, RangeOutcome};
    const CHUNK: u64 = 16 * 1024 * 1024;
    let mut offset = 0;
    while offset < size {
        cancel.check()?;
        let requested = (size - offset).min(CHUNK);
        match dedupe_range(source, destination, offset, requested)? {
            RangeOutcome::Shared(0) => {
                return Err(Error::Capability(format!(
                    "FIDEDUPERANGE made no progress at offset {offset}"
                )))
            }
            RangeOutcome::Shared(bytes) => {
                offset += bytes;
                progress(offset).map_err(|e| Error::Journal(e.to_string()))?;
            }
            RangeOutcome::Differs => {
                return Err(Error::Unstable(format!(
                    "kernel found differing bytes at offset {offset}; nothing more was shared"
                )))
            }
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn share_ranges(
    _source: &std::fs::File,
    _destination: &std::fs::File,
    _size: u64,
    _cancel: &Cancellation,
    _progress: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    Err(Error::Capability(
        "extent sharing (FIDEDUPERANGE) is supported only on Linux".into(),
    ))
}

/// Re-establishes every precondition of `action` from current filesystem facts.
pub fn validate_current(
    action: &PlanAction,
    buffer_bytes: usize,
    cancel: &Cancellation,
) -> Result<()> {
    let target = action.target.to_path()?;
    let keeper = action.keeper.to_path()?;
    for path in [&target, &keeper] {
        let resolved = std::fs::canonicalize(path).map_err(|e| io("canonicalize", path, e))?;
        if &resolved != path {
            return Err(Error::Plan(format!(
                "{path:?} now resolves through a different path ({resolved:?})"
            )));
        }
    }
    let target_now = crate::platform::observe_path(&target)?;
    let keeper_now = crate::platform::observe_path(&keeper)?;
    if target_now != action.target_observation {
        return Err(Error::Plan(format!(
            "target changed since planning: {target:?}"
        )));
    }
    if keeper_now != action.keeper_observation {
        return Err(Error::Plan(format!(
            "keeper changed since planning: {keeper:?}"
        )));
    }
    if target_now.identity == keeper_now.identity {
        return Err(Error::Plan(
            "target and keeper are the same filesystem object".into(),
        ));
    }
    let (equal, _) = compare::equal(&target, &keeper, buffer_bytes, cancel)?;
    if !equal {
        return Err(Error::Plan(
            "target and keeper are no longer byte-identical".into(),
        ));
    }
    Ok(())
}
