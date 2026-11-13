//! Versioned, checksummed and sealed JSONL action plans.
//!
//! Line 1 is a [`PlanHeader`], then one [`PlanAction`] per line in group/path order, then a
//! [`PlanFooter`] with the action count, a BLAKE3 checksum over every preceding line, and a
//! seal: a keyed BLAKE3 of the checksum under the user's private key ([`crate::keys`]). The
//! checksum detects corruption; the seal rejects plans edited by anyone without the key.
//! Neither replaces validation: every action is still checked structurally, and `apply`
//! re-validates it against the current filesystem before touching anything.

use crate::error::{io, Error, Result};
use crate::model::{ActionKind, Completeness, Exactness, Observation};
use crate::paths::{validate_imported, EncodedPath};
use crate::pipeline::ScanResult;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const SCHEMA: &str = "rdedupe-plan-v1";
pub const FOOTER_SCHEMA: &str = "rdedupe-plan-footer-v1";
/// Longest accepted plan line; a longer line is rejected instead of buffered.
pub const MAX_LINE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanHeader {
    pub schema: String,
    pub binary_version: String,
    pub roots: Vec<EncodedPath>,
    pub protect: Vec<EncodedPath>,
    pub prefer: Vec<EncodedPath>,
    pub prefer_newest: bool,
    #[serde(default)]
    pub include_empty: bool,
    pub completeness: Completeness,
    pub exactness: Exactness,
    pub action: ActionKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanAction {
    pub group: u64,
    pub target: EncodedPath,
    pub target_observation: Observation,
    pub keeper: EncodedPath,
    pub keeper_observation: Observation,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanFooter {
    schema: String,
    actions: u64,
    checksum: String,
    seal: String,
}

const SEAL_DOMAIN: &str = "rdedupe-plan-v1";

/// The seal for a plan whose checksum is `checksum_hex`, under this user's key.
pub fn seal_for(checksum_hex: &str) -> Result<String> {
    Ok(crate::keys::seal(
        &crate::keys::load_or_create()?,
        SEAL_DOMAIN,
        checksum_hex.as_bytes(),
    ))
}

/// Writes the plan for a complete, exact scan. Refuses partial, cancelled, failed or
/// heuristic scans, and never overwrites an existing file.
pub fn write_plan(scan: &ScanResult, output: &Path, action: ActionKind) -> Result<u64> {
    if scan.summary.completeness != Completeness::Complete {
        return Err(Error::Partial);
    }
    if scan.summary.exactness != Exactness::Exact {
        return Err(Error::Heuristic);
    }
    let header = PlanHeader {
        schema: SCHEMA.into(),
        binary_version: env!("CARGO_PKG_VERSION").into(),
        roots: scan
            .config
            .roots
            .iter()
            .map(|p| EncodedPath::from_path(p))
            .collect(),
        protect: scan
            .policy
            .protect
            .iter()
            .map(|p| EncodedPath::from_path(p))
            .collect(),
        prefer: scan
            .policy
            .prefer
            .iter()
            .map(|p| EncodedPath::from_path(p))
            .collect(),
        prefer_newest: scan.policy.prefer_newest,
        include_empty: scan.policy.include_empty,
        completeness: scan.summary.completeness.clone(),
        exactness: scan.summary.exactness,
        action,
    };
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .map_err(|e| io("create plan", output, e))?;
    let mut out = BufWriter::new(file);
    let mut hash = blake3::Hasher::new();
    let mut count = 0;
    let result = (|| -> Result<()> {
        write_hashed(&mut out, &header, &mut hash)?;
        scan.store.for_each_group(|group| {
            let (keeper, keeper_reason) = scan.policy.select_streaming(&scan.store, group)?;
            let keeper_xattrs = crate::platform::xattr_digest(&keeper.path);
            scan.store.for_each_member(group, |target| {
                if target.id == keeper.id || scan.policy.retained_because(target).is_some() {
                    return Ok(());
                }
                let differs = crate::policy::metadata_differences(target, &keeper, keeper_xattrs);
                let mut reason =
                    format!("keeper chosen by {keeper_reason}; target is an exact copy");
                if !differs.is_empty() {
                    reason.push_str(&format!(
                        "; its {} differ from the keeper's and are not kept",
                        differs.join(", ")
                    ));
                }
                let item = PlanAction {
                    group: group as u64,
                    target: EncodedPath::from_path(&target.path),
                    target_observation: target.observation.clone(),
                    keeper: EncodedPath::from_path(&keeper.path),
                    keeper_observation: keeper.observation.clone(),
                    reason,
                };
                write_hashed(&mut out, &item, &mut hash)?;
                count += 1;
                Ok(())
            })
        })?;
        let checksum = hash.finalize().to_hex().to_string();
        let footer = PlanFooter {
            schema: FOOTER_SCHEMA.into(),
            actions: count,
            seal: seal_for(&checksum)?,
            checksum,
        };
        serde_json::to_writer(&mut out, &footer)?;
        out.write_all(b"\n")?;
        let file = out
            .into_inner()
            .map_err(|e| Error::Output(e.into_error()))?;
        file.sync_all().map_err(|e| io("sync plan", output, e))?;
        crate::platform::sync_parent(output)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }
    result.map(|()| count)
}

fn write_hashed(
    writer: &mut impl Write,
    value: &impl Serialize,
    hash: &mut blake3::Hasher,
) -> Result<()> {
    let line = serde_json::to_vec(value)?;
    hash.update(&line);
    hash.update(b"\n");
    writer.write_all(&line)?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Reads one line of at most [`MAX_LINE_BYTES`]. Returns `None` at end of file.
fn read_line(reader: &mut impl BufRead, line: &mut Vec<u8>, path: &Path) -> Result<Option<()>> {
    line.clear();
    let mut limited = std::io::Read::take(&mut *reader, MAX_LINE_BYTES + 1);
    let read = limited
        .read_until(b'\n', line)
        .map_err(|e| io("read plan", path, e))?;
    if read == 0 {
        return Ok(None);
    }
    if line.last() != Some(&b'\n') {
        return Err(Error::Plan(if read as u64 > MAX_LINE_BYTES {
            "plan line exceeds the maximum length".into()
        } else {
            "plan is truncated (last line has no newline)".into()
        }));
    }
    line.pop();
    Ok(Some(()))
}

fn is_footer(line: &[u8]) -> Option<PlanFooter> {
    serde_json::from_slice::<PlanFooter>(line)
        .ok()
        .filter(|footer| footer.schema == FOOTER_SCHEMA)
}

/// Stateless per-action checks, repeated during execution as defense in depth.
fn check_action(
    action: &PlanAction,
    roots: &[PathBuf],
    protected: &[PathBuf],
) -> Result<(PathBuf, PathBuf)> {
    let target = action.target.to_path()?;
    let keeper = action.keeper.to_path()?;
    validate_imported(&target, roots)?;
    validate_imported(&keeper, roots)?;
    if target == keeper {
        return Err(Error::Plan(format!("target equals keeper: {target:?}")));
    }
    if protected.iter().any(|p| target.starts_with(p)) {
        return Err(Error::Plan(format!(
            "plan targets a protected path: {target:?}"
        )));
    }
    Ok((target, keeper))
}

/// Disk-backed index used to detect duplicate targets and keeper chains in plans of any
/// size without holding them in memory.
struct Index {
    connection: rusqlite::Connection,
    _directory: tempfile_dir::Dir,
}

mod tempfile_dir {
    /// A private scratch directory removed on drop.
    pub struct Dir(pub std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// Where and how large the plan-verification index may grow.
#[derive(Debug, Clone)]
pub struct IndexSpace {
    /// Base directory (default: the system temporary directory).
    pub scratch_dir: Option<std::path::PathBuf>,
    /// Maximum index size in bytes; exceeding it fails verification with a budget error.
    pub temp_bytes: u64,
}

impl Default for IndexSpace {
    fn default() -> Self {
        Self {
            scratch_dir: None,
            temp_bytes: 8192 * 1024 * 1024,
        }
    }
}

impl Index {
    fn create(space: &IndexSpace) -> Result<Self> {
        let base = space.scratch_dir.clone().unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&base).map_err(|e| io("create scratch base", &base, e))?;
        crate::platform::remove_abandoned_scratch(&base, "rdedupe-plan-index")?;
        let directory = crate::platform::create_scratch_dir(&base, "rdedupe-plan-index")?;
        let directory = tempfile_dir::Dir(directory);
        let connection = rusqlite::Connection::open(directory.0.join("index.sqlite3"))?;
        // Disposable, single-connection index: no sync, no journal, no shared memory.
        connection.pragma_update(None, "synchronous", "OFF")?;
        connection.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        connection.pragma_update(None, "journal_mode", "OFF")?;
        connection.pragma_update(None, "cache_size", -4096)?;
        connection.pragma_update(None, "temp_store", "FILE")?;
        let page_size: i64 = connection.pragma_query_value(None, "page_size", |r| r.get(0))?;
        connection.pragma_update(
            None,
            "max_page_count",
            (space.temp_bytes / page_size as u64).max(64) as i64,
        )?;
        connection.execute_batch(
            "CREATE TABLE targets(target BLOB PRIMARY KEY, keeper BLOB NOT NULL, line BLOB NOT NULL) WITHOUT ROWID;
             CREATE INDEX targets_keeper ON targets(keeper);
             BEGIN;",
        )?;
        Ok(Self {
            connection,
            _directory: directory,
        })
    }
}

/// Fully validates a plan file: schema, completeness, exactness, scope, protection, target
/// uniqueness, absence of keeper chains (no keeper is also a target), action count and
/// checksum. Reads the file in one pass with bounded memory.
pub fn verify_plan(path: &Path) -> Result<PlanHeader> {
    verify_plan_in(path, &IndexSpace::default())
}

/// [`verify_plan`] with the verification index in `space`.
pub fn verify_plan_in(path: &Path, space: &IndexSpace) -> Result<PlanHeader> {
    let file = File::open(path).map_err(|e| io("open plan", path, e))?;
    verify_from(&mut BufReader::new(file), path, space).map(|(header, _, _)| header)
}

/// Returns the header, the raw header line and the index of verified action lines.
fn verify_from(
    reader: &mut impl BufRead,
    path: &Path,
    space: &IndexSpace,
) -> Result<(PlanHeader, Vec<u8>, Index)> {
    let mut line = Vec::new();
    read_line(reader, &mut line, path)?.ok_or_else(|| Error::Plan("empty plan".into()))?;
    let header: PlanHeader = serde_json::from_slice(&line)
        .map_err(|e| Error::Plan(format!("invalid plan header: {e}")))?;
    let header_line = line.clone();
    if header.schema != SCHEMA {
        return Err(Error::Plan(format!(
            "unsupported plan schema {:?}",
            header.schema
        )));
    }
    if header.completeness != Completeness::Complete {
        return Err(Error::Partial);
    }
    if header.exactness != Exactness::Exact {
        return Err(Error::Heuristic);
    }
    let roots = plan_roots(&header)?;
    let protected = protected_roots(&header)?;
    for root in &roots {
        validate_imported(root, std::slice::from_ref(root))?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(&line);
    hash.update(b"\n");
    let index = Index::create(space)?;
    let mut actions = 0u64;
    let mut footer = None;
    while read_line(reader, &mut line, path)?.is_some() {
        if footer.is_some() {
            return Err(Error::Plan("data after plan footer".into()));
        }
        if let Some(value) = is_footer(&line) {
            footer = Some(value);
            continue;
        }
        let action: PlanAction = serde_json::from_slice(&line)
            .map_err(|e| Error::Plan(format!("invalid action {}: {e}", actions + 1)))?;
        check_action(&action, &roots, &protected)?;
        index
            .connection
            .execute(
                "INSERT INTO targets(target, keeper, line) VALUES(?1, ?2, ?3)",
                rusqlite::params![
                    action.target.data.as_bytes(),
                    action.keeper.data.as_bytes(),
                    &blake3::hash(&line).as_bytes()[..]
                ],
            )
            .map_err(|_| Error::Plan(format!("duplicate target in action {}", actions + 1)))?;
        hash.update(&line);
        hash.update(b"\n");
        actions += 1;
        if actions % 4096 == 0 {
            index.connection.execute_batch("COMMIT; BEGIN")?;
        }
    }
    index.connection.execute_batch("COMMIT")?;
    let footer = footer.ok_or_else(|| Error::Plan("missing plan footer".into()))?;
    let chained: Option<i64> = index
        .connection
        .query_row(
            "SELECT 1 FROM targets AS t JOIN targets AS k ON k.target = t.keeper LIMIT 1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if chained.is_some() {
        return Err(Error::Plan(
            "a keeper is also a target (keeper chain or cycle)".into(),
        ));
    }
    if footer.actions != actions {
        return Err(Error::Plan(format!(
            "footer declares {} actions but the plan has {actions}",
            footer.actions
        )));
    }
    if footer.checksum != hash.finalize().to_hex().as_str() {
        return Err(Error::Plan("plan checksum mismatch".into()));
    }
    if !crate::keys::verify(
        &crate::keys::existing()?,
        SEAL_DOMAIN,
        footer.checksum.as_bytes(),
        &footer.seal,
    ) {
        return Err(Error::Plan(
            "plan seal mismatch: the plan was edited, or was not created by this user".into(),
        ));
    }
    Ok((header, header_line, index))
}

/// Verifies the plan, then streams its actions from the same open file. Before each action
/// is handed to `function`, its exact line is matched against the verified index and the
/// stateless checks run again, so a plan modified after verification stops before the first
/// altered action executes.
pub fn for_each_action(
    path: &Path,
    function: impl FnMut(u64, &PlanHeader, PlanAction) -> Result<()>,
) -> Result<PlanHeader> {
    for_each_action_in(path, &IndexSpace::default(), function)
}

/// [`for_each_action`] with the verification index in `space`.
pub fn for_each_action_in(
    path: &Path,
    space: &IndexSpace,
    mut function: impl FnMut(u64, &PlanHeader, PlanAction) -> Result<()>,
) -> Result<PlanHeader> {
    let file = File::open(path).map_err(|e| io("open plan", path, e))?;
    let mut reader = BufReader::new(file);
    let (header, header_line, index) = verify_from(&mut reader, path, space)?;
    let roots = plan_roots(&header)?;
    let protected = protected_roots(&header)?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|e| io("rewind plan", path, e))?;
    let mut line = Vec::new();
    read_line(&mut reader, &mut line, path)?;
    if line != header_line {
        return Err(Error::Plan("plan header changed after verification".into()));
    }
    let mut number = 0;
    while read_line(&mut reader, &mut line, path)?.is_some() {
        if is_footer(&line).is_some() {
            break;
        }
        number += 1;
        let changed = || Error::Plan(format!("plan action {number} changed after verification"));
        let action: PlanAction = serde_json::from_slice(&line).map_err(|_| changed())?;
        let verified: Option<i64> = index
            .connection
            .query_row(
                "SELECT 1 FROM targets WHERE target=?1 AND keeper=?2 AND line=?3",
                rusqlite::params![
                    action.target.data.as_bytes(),
                    action.keeper.data.as_bytes(),
                    &blake3::hash(&line).as_bytes()[..]
                ],
                |r| r.get(0),
            )
            .optional()?;
        if verified.is_none() {
            return Err(changed());
        }
        check_action(&action, &roots, &protected)?;
        function(number, &header, action)?;
    }
    Ok(header)
}

pub fn plan_roots(header: &PlanHeader) -> Result<Vec<PathBuf>> {
    header.roots.iter().map(EncodedPath::to_path).collect()
}

fn protected_roots(header: &PlanHeader) -> Result<Vec<PathBuf>> {
    header.protect.iter().map(EncodedPath::to_path).collect()
}
