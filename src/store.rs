//! Disk-backed scratch inventory.
//!
//! Every collection whose size grows with the input (paths, unresolved entries,
//! fingerprints, comparison classes, groups, cache entries) lives in a private SQLite
//! database inside an owned scratch workspace. The SQLite page cache is capped from the
//! memory budget, the database size is capped from the temporary-storage budget, and every
//! consumer reads it through keyset-paginated cursors of a fixed page size, so no phase ever
//! materializes the whole inventory or a whole duplicate group in memory.

use crate::config::Budget;
use crate::error::{io, Result};
use crate::model::{Completeness, FileIdentity, FileRecord, Metrics, Observation, Unresolved};
use crate::paths::{bytes_path, path_bytes, EncodedPath};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Row};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

/// Rows fetched per cursor page. Fixed so that memory per page is independent of input size.
pub const PAGE: u64 = 256;
/// Writes per scratch transaction.
const BATCH: u64 = 4096;

const RECORD_COLUMNS: &str =
    "files.id,files.path,files.device,files.inode,files.size,files.allocated,files.modified_ns,files.changed_ns,files.mode,files.links,files.alias_of";

pub struct Store {
    conn: Connection,
    workspace: PathBuf,
    keep: bool,
    pending: Cell<u64>,
}

fn record_from_row(row: &Row<'_>) -> rusqlite::Result<FileRecord> {
    let path: Vec<u8> = row.get(1)?;
    let modified: String = row.get(6)?;
    let changed: String = row.get(7)?;
    Ok(FileRecord {
        id: row.get(0)?,
        path: bytes_path(&path),
        observation: Observation {
            identity: FileIdentity {
                device: row.get::<_, i64>(2)? as u64,
                inode: row.get::<_, i64>(3)? as u64,
            },
            size: row.get::<_, i64>(4)? as u64,
            allocated: row.get::<_, i64>(5)? as u64,
            modified_ns: modified.parse().unwrap_or_default(),
            changed_ns: changed.parse().unwrap_or_default(),
            mode: row.get::<_, i64>(8)? as u32,
            links: row.get::<_, i64>(9)? as u64,
        },
        alias_of: row.get(10)?,
    })
}

/// A cached fingerprint and the validators under which it was computed.
pub struct CacheEntry {
    pub observation: Observation,
    pub digest: [u8; 32],
}

impl Store {
    pub fn create(budget: &Budget) -> Result<Self> {
        budget.validate()?;
        let base = budget
            .scratch_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        fs::create_dir_all(&base).map_err(|e| io("create scratch base", &base, e))?;
        let workspace = crate::platform::create_unique_dir(&base, "rdedupe-scratch")?;
        let database = workspace.join("inventory.sqlite3");
        let conn = Connection::open_with_flags(
            &database,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        let store = Self {
            conn,
            workspace,
            keep: false,
            pending: Cell::new(0),
        };
        // The scratch database is disposable: durability is irrelevant, bounded memory and
        // bounded disk are what matter.
        // `synchronous` first, so not even the switch to WAL syncs.
        store.conn.pragma_update(None, "synchronous", "OFF")?;
        store.conn.pragma_update(None, "journal_mode", "WAL")?;
        store.conn.pragma_update(None, "temp_store", "FILE")?;
        let temp_dir = crate::paths::display_path(&store.workspace);
        if !temp_dir.contains('\\') {
            // Keep SQLite's sort files inside the owned workspace when the path is plain.
            let _ = store
                .conn
                .pragma_update(None, "temp_store_directory", &temp_dir);
        }
        let cache_kib = (budget.page_cache_bytes() / 1024).max(1024) as i64;
        store.conn.pragma_update(None, "cache_size", -cache_kib)?;
        store.conn.pragma_update(None, "cache_spill", 1)?;
        let page_size: i64 = store
            .conn
            .pragma_query_value(None, "page_size", |row| row.get(0))?;
        let max_pages = (budget.temp_bytes / (page_size as u64)).max(64);
        store
            .conn
            .pragma_update(None, "max_page_count", max_pages as i64)?;
        store.conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path BLOB NOT NULL UNIQUE, device INTEGER NOT NULL, inode INTEGER NOT NULL, size INTEGER NOT NULL, allocated INTEGER NOT NULL, modified_ns TEXT NOT NULL, changed_ns TEXT NOT NULL, mode INTEGER NOT NULL, links INTEGER NOT NULL, alias_of INTEGER);
             CREATE INDEX files_identity ON files(device, inode, path);
             CREATE INDEX files_candidates ON files(size, alias_of, path);
             CREATE TABLE unresolved (id INTEGER PRIMARY KEY, path BLOB NOT NULL, operation TEXT NOT NULL, message TEXT NOT NULL, retry_safe INTEGER NOT NULL);
             CREATE INDEX unresolved_order ON unresolved(path, id);
             CREATE TABLE fingerprints (file_id INTEGER PRIMARY KEY, digest BLOB NOT NULL, refined BLOB NOT NULL, phase INTEGER NOT NULL DEFAULT 0, path BLOB NOT NULL);
             CREATE INDEX fingerprints_bucket ON fingerprints(phase, refined, path);
             CREATE INDEX fingerprints_flagged ON fingerprints(phase, path);
             CREATE TABLE cache_entries (path BLOB PRIMARY KEY, device INTEGER NOT NULL, inode INTEGER NOT NULL, size INTEGER NOT NULL, modified_ns TEXT NOT NULL, changed_ns TEXT NOT NULL, digest BLOB NOT NULL);
             CREATE TABLE comparison_classes (class_id INTEGER NOT NULL, file_id INTEGER NOT NULL UNIQUE, representative INTEGER NOT NULL, path BLOB NOT NULL);
             CREATE INDEX comparison_reps ON comparison_classes(representative, class_id);
             CREATE INDEX comparison_members ON comparison_classes(class_id);
             CREATE TABLE groups (group_id INTEGER NOT NULL, file_id INTEGER NOT NULL UNIQUE, path BLOB NOT NULL, PRIMARY KEY(group_id, file_id));
             CREATE INDEX groups_members ON groups(group_id, path);
             CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO metadata VALUES('completeness', 'complete');",
        )?;
        Ok(store)
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    /// Keeps the scratch workspace after drop (diagnostics only).
    pub fn preserve(mut self) -> PathBuf {
        self.keep = true;
        self.workspace.clone()
    }

    /// Starts a write batch; writes inside are committed every [`BATCH`] operations.
    pub fn begin(&self) -> Result<()> {
        if self.conn.is_autocommit() {
            self.conn.execute_batch("BEGIN")?;
        }
        Ok(())
    }

    /// Executes one statement through the prepared-statement cache: the engine runs the
    /// same few statements once per candidate, and re-parsing them dominated small files.
    fn exec(&self, sql: &str, params: impl rusqlite::Params) -> Result<usize> {
        Ok(self.conn.prepare_cached(sql)?.execute(params)?)
    }

    /// Like `Connection::query_row`, through the prepared-statement cache.
    fn row<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        f: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        self.conn.prepare_cached(sql)?.query_row(params, f)
    }

    fn wrote(&self) -> Result<()> {
        let pending = self.pending.get() + 1;
        if pending >= BATCH && !self.conn.is_autocommit() {
            self.conn.execute_batch("COMMIT; BEGIN")?;
            self.pending.set(0);
        } else {
            self.pending.set(pending);
        }
        Ok(())
    }

    /// Commits the current write batch, if any.
    pub fn commit(&self) -> Result<()> {
        if !self.conn.is_autocommit() {
            self.conn.execute_batch("COMMIT")?;
        }
        self.pending.set(0);
        Ok(())
    }

    // ----- discovery -------------------------------------------------------------------

    pub fn insert_file(&self, path: &Path, obs: &Observation) -> Result<()> {
        self.exec(
            "INSERT OR IGNORE INTO files(path,device,inode,size,allocated,modified_ns,changed_ns,mode,links) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                path_bytes(path),
                obs.identity.device as i64,
                obs.identity.inode as i64,
                obs.size as i64,
                obs.allocated as i64,
                obs.modified_ns.to_string(),
                obs.changed_ns.to_string(),
                obs.mode as i64,
                obs.links as i64
            ],
        )?;
        self.wrote()
    }

    /// Marks every path whose filesystem object was already inventoried under a smaller
    /// native path as an alias of that path. Deterministic regardless of traversal order.
    pub fn finalize_aliases(&self) -> Result<()> {
        self.commit()?;
        self.conn.execute_batch(
            "UPDATE files SET alias_of = (SELECT f2.id FROM files f2 WHERE f2.device = files.device AND f2.inode = files.inode ORDER BY f2.path LIMIT 1);
             UPDATE files SET alias_of = NULL WHERE alias_of = id;",
        )?;
        Ok(())
    }

    pub fn insert_unresolved(&self, item: &Unresolved) -> Result<()> {
        let path = item.path.to_path()?;
        self.exec(
            "INSERT INTO unresolved(path,operation,message,retry_safe) VALUES(?1,?2,?3,?4)",
            params![
                path_bytes(&path),
                item.operation,
                item.message,
                item.retry_safe as i32
            ],
        )?;
        self.wrote()?;
        self.set_completeness(Completeness::Partial)
    }

    pub fn set_completeness(&self, value: Completeness) -> Result<()> {
        let current = self.completeness()?;
        // Completeness only degrades: failed > cancelled > partial > complete.
        let rank = |c: &Completeness| match c {
            Completeness::Complete => 0,
            Completeness::Partial => 1,
            Completeness::Cancelled => 2,
            Completeness::Failed => 3,
        };
        if rank(&value) <= rank(&current) {
            return Ok(());
        }
        let text = match value {
            Completeness::Complete => "complete",
            Completeness::Partial => "partial",
            Completeness::Cancelled => "cancelled",
            Completeness::Failed => "failed",
        };
        self.exec(
            "UPDATE metadata SET value=?1 WHERE key='completeness'",
            params![text],
        )?;
        Ok(())
    }

    pub fn completeness(&self) -> Result<Completeness> {
        let value: String = self.row(
            "SELECT value FROM metadata WHERE key='completeness'",
            [],
            |r| r.get(0),
        )?;
        Ok(match value.as_str() {
            "complete" => Completeness::Complete,
            "partial" => Completeness::Partial,
            "cancelled" => Completeness::Cancelled,
            _ => Completeness::Failed,
        })
    }

    pub fn contains_path(&self, path: &Path) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM files WHERE path=?1",
                params![path_bytes(path)],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    // ----- candidates ------------------------------------------------------------------

    /// Sizes shared by more than one independent object, ascending, after `after`.
    pub fn candidate_sizes_after(&self, after: Option<u64>) -> Result<Vec<u64>> {
        let mut statement = self.conn.prepare_cached(
            "SELECT size FROM files WHERE alias_of IS NULL AND size > ?1 GROUP BY size HAVING COUNT(*) > 1 ORDER BY size LIMIT ?2",
        )?;
        let after = after.map(|v| v as i64).unwrap_or(-1);
        let rows = statement
            .query_map(params![after, PAGE as i64], |r| {
                Ok(r.get::<_, i64>(0)? as u64)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Independent objects of one size, in native path order, after `after`.
    pub fn candidates_after(&self, size: u64, after: Option<&[u8]>) -> Result<Vec<FileRecord>> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM files WHERE size=?1 AND alias_of IS NULL AND path > ?2 ORDER BY path LIMIT ?3"
        );
        let mut statement = self.conn.prepare_cached(&sql)?;
        let rows = statement
            .query_map(
                params![size as i64, after.unwrap_or(&[]), PAGE as i64],
                record_from_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn put_fingerprint(&self, record: &FileRecord, digest: &[u8; 32]) -> Result<()> {
        self.exec(
            "INSERT OR REPLACE INTO fingerprints(file_id,digest,refined,path) VALUES(?1,?2,?2,?3)",
            params![record.id, &digest[..], path_bytes(&record.path)],
        )?;
        self.wrote()
    }

    /// Flags every member of a comparison bucket for full-content refinement.
    pub fn flag_for_refinement(&self, digest: &[u8]) -> Result<()> {
        self.exec(
            "UPDATE fingerprints SET phase=1 WHERE phase=0 AND refined=?1",
            params![digest],
        )?;
        self.wrote()
    }

    /// Candidates flagged for refinement, in native path order, after `after`.
    pub fn flagged_after(&self, after: Option<&[u8]>) -> Result<Vec<FileRecord>> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM fingerprints JOIN files ON files.id = fingerprints.file_id WHERE fingerprints.phase=1 AND fingerprints.path > ?1 ORDER BY fingerprints.path LIMIT ?2"
        );
        let mut statement = self.conn.prepare_cached(&sql)?;
        let rows = statement
            .query_map(params![after.unwrap_or(&[]), PAGE as i64], record_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Candidates whose final bucket key (sampled or refined digest) is unique: separated
    /// from every other candidate without a byte comparison.
    pub fn singleton_buckets(&self) -> Result<u64> {
        Ok(self.row(
            "SELECT COUNT(*) FROM (SELECT refined FROM fingerprints GROUP BY phase, refined HAVING COUNT(*) = 1)",
            [],
            |r| r.get::<_, i64>(0),
        )? as u64)
    }

    /// Replaces a flagged candidate's comparison bucket key with its refined
    /// (full-content) digest.
    pub fn set_refined(&self, record: &FileRecord, refined: &[u8; 32]) -> Result<()> {
        self.exec(
            "UPDATE fingerprints SET refined=?2, phase=2 WHERE file_id=?1",
            params![record.id, &refined[..]],
        )?;
        self.wrote()
    }

    /// Withdraws a candidate from comparison (it became unresolved).
    pub fn remove_fingerprint(&self, record: &FileRecord) -> Result<()> {
        self.exec(
            "DELETE FROM fingerprints WHERE file_id=?1",
            params![record.id],
        )?;
        self.wrote()
    }

    /// Comparison buckets of one phase (0: sampled digests, 2: refined digests) shared by
    /// more than one object, after `after`.
    pub fn colliding_digests_after(
        &self,
        phase: i64,
        after: Option<&[u8]>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut statement = self.conn.prepare_cached(
            "SELECT refined FROM fingerprints WHERE phase=?3 AND refined > ?1 GROUP BY refined HAVING COUNT(*) > 1 ORDER BY refined LIMIT ?2",
        )?;
        let rows = statement
            .query_map(params![after.unwrap_or(&[]), PAGE as i64, phase], |r| {
                r.get(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Members of one comparison bucket of `phase` in native path order, after `after`.
    pub fn bucket_after(
        &self,
        phase: i64,
        digest: &[u8],
        after: Option<&[u8]>,
    ) -> Result<Vec<FileRecord>> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM fingerprints JOIN files ON files.id = fingerprints.file_id WHERE fingerprints.phase=?4 AND fingerprints.refined=?1 AND fingerprints.path > ?2 ORDER BY fingerprints.path LIMIT ?3"
        );
        let mut statement = self.conn.prepare_cached(&sql)?;
        let rows = statement
            .query_map(
                params![digest, after.unwrap_or(&[]), PAGE as i64, phase],
                record_from_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn reset_comparison_classes(&self) -> Result<()> {
        self.exec("DELETE FROM comparison_classes", [])?;
        Ok(())
    }

    /// Representatives of the current bucket's equality classes, by class id, after `after`.
    pub fn representatives_after(&self, after: i64) -> Result<Vec<(i64, FileRecord)>> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS}, comparison_classes.class_id FROM comparison_classes JOIN files ON files.id = comparison_classes.file_id WHERE representative=1 AND class_id > ?1 ORDER BY class_id LIMIT ?2"
        );
        let mut statement = self.conn.prepare_cached(&sql)?;
        let rows = statement
            .query_map(params![after, PAGE as i64], |r| {
                Ok((r.get::<_, i64>(11)?, record_from_row(r)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn add_comparison_class(&self, record: &FileRecord) -> Result<i64> {
        let id: i64 = self.row(
            "SELECT COALESCE(MAX(class_id), 0) + 1 FROM comparison_classes",
            [],
            |r| r.get(0),
        )?;
        self.exec(
            "INSERT INTO comparison_classes(class_id,file_id,representative,path) VALUES(?1,?2,1,?3)",
            params![id, record.id, path_bytes(&record.path)],
        )?;
        self.wrote()?;
        Ok(id)
    }

    pub fn add_comparison_member(&self, class_id: i64, record: &FileRecord) -> Result<()> {
        self.exec(
            "INSERT INTO comparison_classes(class_id,file_id,representative,path) VALUES(?1,?2,0,?3)",
            params![class_id, record.id, path_bytes(&record.path)],
        )?;
        self.wrote()
    }

    /// Turns every equality class of the current bucket with more than one member into a
    /// duplicate group. Returns (groups created, singleton classes eliminated).
    pub fn materialize_comparison_groups(&self) -> Result<(u64, u64)> {
        let mut grouped = 0;
        let mut eliminated = 0;
        let mut last = 0i64;
        loop {
            let classes = {
                let mut statement = self.conn.prepare_cached(
                    "SELECT class_id, COUNT(*) FROM comparison_classes WHERE class_id > ?1 GROUP BY class_id ORDER BY class_id LIMIT ?2",
                )?;
                let rows = statement
                    .query_map(params![last, PAGE as i64], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)? as u64))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            };
            let Some(&(tail, _)) = classes.last() else {
                break;
            };
            for (class_id, count) in classes {
                if count > 1 {
                    let next: i64 = self.row(
                        "SELECT COALESCE(MAX(group_id), 0) + 1 FROM groups",
                        [],
                        |r| r.get(0),
                    )?;
                    self.exec(
                        "INSERT INTO groups(group_id,file_id,path) SELECT ?1, file_id, path FROM comparison_classes WHERE class_id=?2",
                        params![next, class_id],
                    )?;
                    self.wrote()?;
                    grouped += 1;
                } else {
                    eliminated += 1;
                }
            }
            last = tail;
        }
        Ok((grouped, eliminated))
    }

    /// Renumbers groups 1..N in order of each group's smallest native path, so group
    /// numbers depend only on the inventory, never on scheduling or cache state.
    pub fn renumber_groups(&self) -> Result<()> {
        self.commit()?;
        self.conn.execute_batch(
            "CREATE TABLE group_map (old INTEGER PRIMARY KEY, new INTEGER NOT NULL);
             INSERT INTO group_map(old, new) SELECT group_id, ROW_NUMBER() OVER (ORDER BY MIN(path)) FROM groups GROUP BY group_id;
             UPDATE groups SET group_id = -(SELECT new FROM group_map WHERE old = groups.group_id);
             UPDATE groups SET group_id = -group_id;
             DROP TABLE group_map;",
        )?;
        Ok(())
    }

    // ----- groups ----------------------------------------------------------------------

    pub fn group_ids_after(&self, after: i64) -> Result<Vec<i64>> {
        let mut statement = self.conn.prepare_cached(
            "SELECT DISTINCT group_id FROM groups WHERE group_id > ?1 ORDER BY group_id LIMIT ?2",
        )?;
        let ids = statement
            .query_map(params![after, PAGE as i64], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(ids)
    }

    /// Calls `f` for every group id in ascending order.
    pub fn for_each_group(&self, mut f: impl FnMut(i64) -> Result<()>) -> Result<()> {
        let mut last = 0;
        loop {
            let ids = self.group_ids_after(last)?;
            let Some(&tail) = ids.last() else {
                return Ok(());
            };
            for id in ids {
                f(id)?;
            }
            last = tail;
        }
    }

    pub fn group_members_after(&self, group: i64, after: Option<&[u8]>) -> Result<Vec<FileRecord>> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM groups JOIN files ON files.id = groups.file_id WHERE groups.group_id=?1 AND groups.path > ?2 ORDER BY groups.path LIMIT ?3"
        );
        let mut statement = self.conn.prepare_cached(&sql)?;
        let rows = statement
            .query_map(
                params![group, after.unwrap_or(&[]), PAGE as i64],
                record_from_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Calls `f` for every member of `group` in native path order.
    pub fn for_each_member(
        &self,
        group: i64,
        mut f: impl FnMut(&FileRecord) -> Result<()>,
    ) -> Result<()> {
        let mut last: Option<Vec<u8>> = None;
        loop {
            let members = self.group_members_after(group, last.as_deref())?;
            let Some(tail) = members.last() else {
                return Ok(());
            };
            let tail = path_bytes(&tail.path).to_vec();
            for member in &members {
                f(member)?;
            }
            last = Some(tail);
        }
    }

    pub fn group_len(&self, group: i64) -> Result<u64> {
        Ok(self.row(
            "SELECT COUNT(*) FROM groups WHERE group_id=?1",
            params![group],
            |r| r.get::<_, i64>(0),
        )? as u64)
    }

    // ----- whole inventory ---------------------------------------------------------------

    pub fn files_after(&self, after: Option<&[u8]>) -> Result<Vec<(FileRecord, Option<Vec<u8>>)>> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS}, fingerprints.digest FROM files LEFT JOIN fingerprints ON fingerprints.file_id = files.id WHERE files.path > ?1 ORDER BY files.path LIMIT ?2"
        );
        let mut statement = self.conn.prepare_cached(&sql)?;
        let rows = statement
            .query_map(params![after.unwrap_or(&[]), PAGE as i64], |r| {
                Ok((record_from_row(r)?, r.get::<_, Option<Vec<u8>>>(11)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ----- cache ---------------------------------------------------------------------------

    pub fn put_cache_entry(&self, path: &Path, entry: &CacheEntry) -> Result<()> {
        let o = &entry.observation;
        self.exec(
            "INSERT OR REPLACE INTO cache_entries(path,device,inode,size,modified_ns,changed_ns,digest) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                path_bytes(path),
                o.identity.device as i64,
                o.identity.inode as i64,
                o.size as i64,
                o.modified_ns.to_string(),
                o.changed_ns.to_string(),
                &entry.digest[..]
            ],
        )?;
        self.wrote()
    }

    pub fn clear_cache_entries(&self) -> Result<()> {
        self.exec("DELETE FROM cache_entries", [])?;
        Ok(())
    }

    /// Returns the cached digest for `record` only when every validator matches the current
    /// observation (device, inode, size, mtime and ctime).
    pub fn cached_digest(&self, record: &FileRecord) -> Result<Option<[u8; 32]>> {
        let o = &record.observation;
        let digest: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT digest FROM cache_entries WHERE path=?1 AND device=?2 AND inode=?3 AND size=?4 AND modified_ns=?5 AND changed_ns=?6",
                params![
                    path_bytes(&record.path),
                    o.identity.device as i64,
                    o.identity.inode as i64,
                    o.size as i64,
                    o.modified_ns.to_string(),
                    o.changed_ns.to_string()
                ],
                |r| r.get(0),
            )
            .optional()?;
        Ok(digest.and_then(|d| d.try_into().ok()))
    }

    // ----- unresolved and metrics --------------------------------------------------------

    pub fn unresolved_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM unresolved", [], |r| {
                r.get::<_, i64>(0)
            })? as u64)
    }

    /// Calls `f` for every unresolved entry in native path order.
    pub fn for_each_unresolved(&self, mut f: impl FnMut(&Unresolved) -> Result<()>) -> Result<()> {
        let mut last: (Vec<u8>, i64) = (Vec::new(), 0);
        loop {
            let rows = {
                let mut statement = self.conn.prepare_cached(
                    "SELECT path, operation, message, retry_safe, id FROM unresolved WHERE (path, id) > (?1, ?2) ORDER BY path, id LIMIT ?3",
                )?;
                let rows = statement
                    .query_map(params![last.0, last.1, PAGE as i64], |r| {
                        let p: Vec<u8> = r.get(0)?;
                        Ok((
                            Unresolved {
                                path: EncodedPath::from_path(&bytes_path(&p)),
                                operation: r.get(1)?,
                                message: r.get(2)?,
                                retry_safe: r.get::<_, i64>(3)? != 0,
                            },
                            p,
                            r.get::<_, i64>(4)?,
                        ))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            };
            let Some((_, path, id)) = rows.last() else {
                return Ok(());
            };
            last = (path.clone(), *id);
            for (item, _, _) in &rows {
                f(item)?;
            }
        }
    }

    pub fn base_metrics(&self, page_cache_bytes: u64) -> Result<Metrics> {
        let paths = self
            .conn
            .query_row("SELECT COUNT(*) FROM files", [], |r| r.get::<_, i64>(0))?
            as u64;
        let aliases = self.row(
            "SELECT COUNT(*) FROM files WHERE alias_of IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0),
        )? as u64;
        let candidates = self.row(
            "SELECT COALESCE(SUM(n), 0) FROM (SELECT COUNT(*) n FROM files WHERE alias_of IS NULL GROUP BY size HAVING COUNT(*) > 1)",
            [],
            |r| r.get::<_, i64>(0),
        )? as u64;
        Ok(Metrics {
            paths,
            objects: paths - aliases,
            aliases,
            candidate_objects: candidates,
            eliminated_by_size: paths - aliases - candidates,
            sqlite_page_cache_bytes: page_cache_bytes,
            ..Metrics::default()
        })
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.workspace);
        }
    }
}
