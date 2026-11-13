//! Resuming an interrupted `apply` in its own journal (`apply --resume`).
//!
//! The journal's last record for each plan action decides what happens to it:
//!
//! * no record: the action never started and runs normally;
//! * `committed`, `failed`, `restored` or `purged`: the action finished (or was undone)
//!   and is not repeated;
//! * `prepared` or `applied`: the run stopped inside the action. Facts decide: a target
//!   still in place is recorded `failed` and retried; a target already in its quarantine
//!   slot is recorded `applied` and `committed`; a target linked under both names loses
//!   the extra name and is retried; an extent action is retried (sharing identical ranges
//!   again is harmless). Anything else is indeterminate and stops the resume, pointing to
//!   `recover` and `restore`.
//!
//! Every journaled action must name the same target and keeper as the plan action with
//! its number; otherwise the journal belongs to another plan and nothing runs. The
//! per-action index lives on disk under the plan index's scratch space, so resuming a
//! plan of any size needs bounded memory.

use crate::error::{io, Error, Result};
use crate::journal::{for_each_limited, Journal, JournalEntry, JournalHooks, State};
use crate::model::{ActionKind, ActionStatus};
use crate::plan::{IndexSpace, PlanAction};
use crate::recovery::{locate, Location};
use rusqlite::{params, Connection, OptionalExtension};

/// What an earlier run did with one action.
pub enum Prior {
    /// Nothing, or it was reconciled so the action can run now.
    None,
    /// Finished earlier with this status; it is not repeated.
    Finished(ActionStatus),
}

pub struct Previous {
    connection: Connection,
    directory: std::path::PathBuf,
}

impl Drop for Previous {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

impl Previous {
    /// Indexes the last record of every action already in `journal`.
    pub fn load(journal: &Journal, space: &IndexSpace) -> Result<Self> {
        let base = space.scratch_dir.clone().unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&base).map_err(|e| io("create scratch base", &base, e))?;
        let directory = crate::platform::create_scratch_dir(&base, "rdedupe-plan-index")?;
        let previous = Self {
            connection: Connection::open(directory.join("resume.sqlite3"))?,
            directory,
        };
        let connection = &previous.connection;
        connection.pragma_update(None, "synchronous", "OFF")?;
        connection.pragma_update(None, "journal_mode", "OFF")?;
        let page_size: i64 = connection.pragma_query_value(None, "page_size", |r| r.get(0))?;
        connection.pragma_update(
            None,
            "max_page_count",
            (space.temp_bytes / page_size as u64).max(64) as i64,
        )?;
        connection.execute_batch(
            "CREATE TABLE last (action INTEGER PRIMARY KEY, record BLOB NOT NULL); BEGIN;",
        )?;
        {
            let mut upsert = connection.prepare(
                "INSERT INTO last(action, record) VALUES(?1, ?2) ON CONFLICT(action) DO UPDATE SET record=excluded.record",
            )?;
            for_each_limited(journal.path(), journal.initial_len(), |entry| {
                upsert.execute(params![entry.action as i64, serde_json::to_vec(entry)?])?;
                Ok(())
            })?;
        }
        connection.execute_batch("COMMIT;")?;
        Ok(previous)
    }

    fn last(&self, number: u64) -> Result<Option<JournalEntry>> {
        let record: Option<Vec<u8>> = self
            .connection
            .prepare_cached("SELECT record FROM last WHERE action=?1")?
            .query_row(params![number as i64], |r| r.get(0))
            .optional()?;
        record
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Error::from))
            .transpose()
    }

    /// Reconciles plan action `number` with the interrupted run before it executes.
    pub fn prepare(
        &self,
        journal: &mut Journal,
        hooks: &dyn JournalHooks,
        number: u64,
        action: &PlanAction,
    ) -> Result<Prior> {
        let Some(last) = self.last(number)? else {
            return Ok(Prior::None);
        };
        if last.target != action.target || last.keeper != action.keeper {
            return Err(Error::Journal(format!(
                "the journal does not belong to this plan: its action {number} names other files"
            )));
        }
        let append = |journal: &mut Journal, state: State, message: &str| {
            let mut next = last.clone();
            next.state = state;
            next.message = Some(message.into());
            if state != State::Failed {
                next.bytes_completed = last.target_observation.size;
            }
            journal.append(next, hooks)
        };
        let done = match last.kind {
            ActionKind::Quarantine => ActionStatus::Quarantined,
            ActionKind::Extent => ActionStatus::Shared,
        };
        Ok(match last.state {
            State::Committed | State::Purged => Prior::Finished(done),
            State::Failed => Prior::Finished(ActionStatus::Failed),
            State::Restored => Prior::Finished(ActionStatus::SkippedStale),
            State::Prepared | State::Applied => match last.kind {
                ActionKind::Extent => {
                    append(journal, State::Failed, "interrupted; retried by resume")?;
                    Prior::None
                }
                ActionKind::Quarantine => match locate(&last)? {
                    Location::InPlace => {
                        append(
                            journal,
                            State::Failed,
                            "interrupted before the move; retried by resume",
                        )?;
                        Prior::None
                    }
                    Location::InQuarantine => {
                        append(journal, State::Applied, "move completed before the interruption")?;
                        append(journal, State::Committed, "completed by resume")?;
                        Prior::Finished(done)
                    }
                    Location::BothNames => {
                        let extra = last
                            .quarantine
                            .as_ref()
                            .ok_or_else(|| Error::Journal("quarantine record lacks a path".into()))?
                            .to_path()?;
                        if !crate::quarantine::remove_name(&extra, &last.target_observation)? {
                            return Err(Error::Journal(format!(
                                "action {number}: the extra quarantine name changed; run `rdedupe recover`"
                            )));
                        }
                        append(
                            journal,
                            State::Failed,
                            "interrupted mid-move; extra name removed, retried by resume",
                        )?;
                        Prior::None
                    }
                    other => {
                        return Err(Error::Journal(format!(
                            "action {number} is indeterminate ({other:?}); run `rdedupe recover --journal` and `rdedupe restore --journal` before resuming"
                        )))
                    }
                },
            },
        })
    }
}
