//! Append-only, synced JSONL action journal.
//!
//! Each line is one [`JournalEntry`]. An entry is fsynced before the next filesystem
//! mutation begins, so after a crash the journal always contains the intent of any action
//! that may have started. Recovery never trusts the journal alone: it reconciles each
//! recorded action against the actual filesystem (see [`crate::recovery`]).
//!
//! A final line without a terminating newline is a record that was being written when the
//! process stopped; it is ignored and reported. Any other malformed line is corruption and
//! stops every operation on the journal.

use crate::error::{io, Error, Result};
use crate::model::{ActionKind, Observation};
use crate::paths::EncodedPath;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub const SCHEMA: &str = "rdedupe-journal-v1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Intent recorded; the mutation may or may not have happened.
    Prepared,
    /// The mutation (or, for extents, `bytes_completed` of it) happened.
    Applied,
    /// The action finished.
    Committed,
    /// The action stopped with an error after `prepared`; facts decide what happened.
    Failed,
    /// `rdedupe restore` moved the object back.
    Restored,
    /// `rdedupe purge` removed the quarantined object.
    Purged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub schema: String,
    pub sequence: u64,
    /// Plan action number (1-based); all records of one action share it.
    pub action: u64,
    pub group: u64,
    pub state: State,
    pub kind: ActionKind,
    pub target: EncodedPath,
    pub keeper: EncodedPath,
    pub quarantine: Option<EncodedPath>,
    pub target_observation: Observation,
    pub bytes_completed: u64,
    pub message: Option<String>,
}

/// Deterministic fault-injection seam for tests: called after every record is durable.
pub trait JournalHooks {
    fn after_sync(&self, _entry: &JournalEntry) -> Result<()> {
        Ok(())
    }
}

pub struct NoHooks;
impl JournalHooks for NoHooks {}

pub struct Journal {
    file: File,
    path: PathBuf,
    next: u64,
    /// Length of the journal when it was opened (after dropping a torn tail).
    initial_len: u64,
    /// Opening found and removed a torn final record.
    dropped_torn_tail: bool,
}

impl Journal {
    /// Creates a new journal (never reusing an existing file) and locks it exclusively.
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)
            .map_err(|e| io("create journal", path, e))?;
        crate::platform::lock_exclusive(&file, path)?;
        crate::platform::sync_parent(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            next: 1,
            initial_len: 0,
            dropped_torn_tail: false,
        })
    }

    /// Opens an existing journal for appending recovery records, locking it exclusively.
    /// Fails while another process (an `apply`, `restore` or `purge`) holds it.
    pub fn open_existing(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| io("open journal", path, e))?;
        crate::platform::lock_exclusive(&file, path)?;
        let mut next = 1;
        let tail = for_each(path, |entry| {
            next = next.max(entry.sequence + 1);
            Ok(())
        })?;
        let journal = Self {
            file,
            path: path.to_path_buf(),
            next,
            initial_len: tail.valid_bytes,
            dropped_torn_tail: tail.truncated_tail,
        };
        if tail.truncated_tail {
            // A torn final record was being written when the process stopped. Records are
            // synced before their mutation starts, so its mutation never began; drop the
            // fragment so later records start on a clean line.
            journal
                .file
                .set_len(tail.valid_bytes)
                .and_then(|()| journal.file.sync_all())
                .map_err(|e| io("drop torn journal record", path, e))?;
        }
        Ok(journal)
    }

    pub fn append(&mut self, mut entry: JournalEntry, hooks: &dyn JournalHooks) -> Result<()> {
        entry.schema = SCHEMA.into();
        entry.sequence = self.next;
        self.next += 1;
        let mut line = serde_json::to_vec(&entry)?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .map_err(|e| io("write journal", &self.path, e))?;
        self.file
            .sync_all()
            .map_err(|e| io("sync journal", &self.path, e))?;
        hooks.after_sync(&entry)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Length of the journal when it was opened; pass to [`for_each_limited`] to stream only
    /// the records that existed before this process appended any.
    pub fn initial_len(&self) -> u64 {
        self.initial_len
    }

    /// True when opening found a torn final record and removed it.
    pub fn dropped_torn_tail(&self) -> bool {
        self.dropped_torn_tail
    }
}

/// What reading the journal found besides well-formed records.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tail {
    /// The last line had no terminating newline and was ignored.
    pub truncated_tail: bool,
    pub records: u64,
    /// Byte length of the journal up to and including the last complete line.
    pub valid_bytes: u64,
}

/// Streams every well-formed record in order. Memory use is one line at a time.
pub fn for_each(path: &Path, f: impl FnMut(&JournalEntry) -> Result<()>) -> Result<Tail> {
    for_each_limited(path, u64::MAX, f)
}

/// Like [`for_each`] but reads at most `limit` bytes, so records appended while iterating
/// (by recovery itself) are not revisited.
pub fn for_each_limited(
    path: &Path,
    limit: u64,
    mut f: impl FnMut(&JournalEntry) -> Result<()>,
) -> Result<Tail> {
    let file = File::open(path).map_err(|e| io("open journal", path, e))?;
    let mut reader = BufReader::new(std::io::Read::take(file, limit));
    let mut line = Vec::new();
    let mut tail = Tail::default();
    let mut number = 0u64;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| io("read journal", path, e))?;
        if read == 0 {
            return Ok(tail);
        }
        number += 1;
        if line.last() != Some(&b'\n') {
            tail.truncated_tail = true;
            return Ok(tail);
        }
        tail.valid_bytes += read as u64;
        line.pop();
        if line.is_empty() {
            continue;
        }
        let entry: JournalEntry = serde_json::from_slice(&line)
            .map_err(|e| Error::Journal(format!("corrupt record on line {number}: {e}")))?;
        if entry.schema != SCHEMA {
            return Err(Error::Journal(format!(
                "unsupported journal schema {:?} on line {number}",
                entry.schema
            )));
        }
        tail.records += 1;
        f(&entry)?;
    }
}
