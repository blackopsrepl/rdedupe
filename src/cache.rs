//! Disposable fingerprint cache.
//!
//! The cache stores, per path, the sampled fingerprint together with the validators it was
//! computed under (device, inode, size, mtime, ctime). It never establishes equality: groups
//! always come from complete byte comparison.
//!
//! * In the default **exact** mode the cache is only written (refreshed), never consulted,
//!   because a stale entry could otherwise eliminate a genuine candidate (a "stale
//!   negative"), and metadata validators cannot prove unchanged content.
//! * Validators (device, inode, size, mtime, ctime) cannot prove content is unchanged:
//!   an inode number can be reused by a new file, timestamps can be restored or coarse
//!   (FAT, some network filesystems), and network filesystems may report stale metadata.
//!   That is why trusting the cache makes a scan heuristic.
//! * With `--trust-cache` the scan is **heuristic**: fingerprints whose validators all match
//!   are reused instead of re-read. Reported groups are still byte-verified, but stale
//!   entries may hide duplicates, so plans refuse heuristic scans.

use crate::error::{io, Result};
use crate::fingerprint::LAYOUT;
use crate::model::{FileIdentity, Observation};
use crate::paths::{path_bytes, EncodedPath};
use crate::store::{CacheEntry, Store};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

const SCHEMA: &str = "rdedupe-cache-v2";

#[derive(Serialize, Deserialize)]
struct Header {
    schema: String,
    layout: String,
    authority: String,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    path: EncodedPath,
    device: u64,
    inode: u64,
    size: u64,
    modified_ns: i128,
    changed_ns: i128,
    digest: String,
}

/// Outcome of loading a cache file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Loaded {
    Missing,
    Entries(u64),
    /// The file was unreadable, of another version, or corrupt; nothing was loaded.
    Ignored(String),
}

pub fn load(path: &Path, store: &Store, cancel: &crate::scheduler::Cancellation) -> Result<Loaded> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Loaded::Missing),
        Err(error) => return Ok(Loaded::Ignored(format!("unreadable: {error}"))),
    };
    let mut lines = BufReader::new(file).split(b'\n');
    let header = match lines.next() {
        Some(Ok(line)) => serde_json::from_slice::<Header>(&line).ok(),
        _ => None,
    };
    match header {
        Some(h) if h.schema == SCHEMA && h.layout == LAYOUT => {}
        _ => return Ok(Loaded::Ignored("unsupported or corrupt header".into())),
    }
    store.begin()?;
    let mut count = 0;
    for line in lines {
        cancel.check()?;
        let parsed = line
            .ok()
            .and_then(|line| serde_json::from_slice::<Entry>(&line).ok())
            .and_then(|entry| {
                let digest: [u8; 32] = hex::decode(&entry.digest).ok()?.try_into().ok()?;
                Some((entry.path.to_path().ok()?, entry, digest))
            });
        let Some((entry_path, entry, digest)) = parsed else {
            store.commit()?;
            store.clear_cache_entries()?;
            return Ok(Loaded::Ignored(format!(
                "corrupt record after {count} entries"
            )));
        };
        store.put_cache_entry(
            &entry_path,
            &CacheEntry {
                observation: Observation {
                    identity: FileIdentity {
                        device: entry.device,
                        inode: entry.inode,
                    },
                    size: entry.size,
                    allocated: 0,
                    modified_ns: entry.modified_ns,
                    changed_ns: entry.changed_ns,
                    mode: 0,
                    links: 0,
                    uid: 0,
                    gid: 0,
                    created_ns: None,
                },
                digest,
            },
        )?;
        count += 1;
    }
    store.commit()?;
    Ok(Loaded::Entries(count))
}

/// Removes `<cache>.tmp-<pid>` files left by rdedupe processes that no longer exist (an
/// update interrupted by `kill -9` or power loss). The published cache is never touched
/// by an interrupted update: it is replaced only by a complete, synced file.
fn remove_stale_temporaries(path: &Path) -> Result<()> {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Ok(());
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let Some(name) = name.to_str() else {
        return Ok(());
    };
    let lead = format!("{name}.tmp-");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(pid) = file_name
            .to_str()
            .and_then(|n| n.strip_prefix(&lead))
            .and_then(|p| p.parse::<u32>().ok())
        else {
            continue;
        };
        let stale = entry.path();
        if pid != std::process::id()
            && crate::platform::process_is_gone(pid)
            && std::fs::symlink_metadata(&stale).is_ok_and(|m| m.is_file())
        {
            std::fs::remove_file(&stale)
                .map_err(|e| io("remove stale temporary file", &stale, e))?;
        }
    }
    Ok(())
}

/// What occupies the cache path before a scan. Checked before any work and again right
/// before publishing, so `--cache` can never destroy a file that is not an rdedupe cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// Nothing is there; the cache is published without replacing anything.
    Empty,
    /// A single-link regular file with an rdedupe cache header, with this identity.
    Cache(FileIdentity),
}

/// Inspects the cache path. Refuses (with an error, before scanning) anything that is not
/// absent or an existing rdedupe cache: a user file, a symlink, a directory, or a file with
/// other hard links (which would be an alias of data).
pub fn claim(path: &Path) -> Result<Slot> {
    let refuse = |why: String| {
        Err(crate::Error::Config(format!(
            "refusing --cache {}: {why}; it is left untouched",
            crate::paths::display_path(path)
        )))
    };
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Slot::Empty),
        Err(error) => return Err(io("inspect cache", path, error)),
    };
    if !metadata.file_type().is_file() {
        return refuse("it exists and is not a regular file".into());
    }
    let observation = crate::platform::observation(&metadata);
    if observation.links != 1 {
        return refuse("it has other hard links".into());
    }
    let mut first = Vec::new();
    let file = std::fs::File::open(path).map_err(|e| io("open cache", path, e))?;
    BufReader::new(std::io::Read::take(file, 4096))
        .read_until(b'\n', &mut first)
        .map_err(|e| io("read cache", path, e))?;
    let is_cache = serde_json::from_slice::<serde_json::Value>(first.trim_ascii_end())
        .ok()
        .and_then(|v| {
            v.get("schema")?
                .as_str()
                .map(|s| s.starts_with("rdedupe-cache-v"))
        })
        .unwrap_or(false);
    if !is_cache {
        return refuse("it exists and is not an rdedupe cache".into());
    }
    Ok(Slot::Cache(observation.identity))
}

/// Publishes the cache with the fingerprints computed (or reused) by this scan. The slot
/// must still be what [`claim`] found: an absent path is filled without replacement, and
/// an existing cache is replaced only if it is still the same file.
pub fn write(path: &Path, store: &Store, slot: Slot) -> Result<u64> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    std::fs::create_dir_all(parent).map_err(|e| io("create cache directory", parent, e))?;
    remove_stale_temporaries(path)?;
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".tmp-{}", std::process::id()));
    let temporary = parent.join(name);
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|e| io("create cache", &temporary, e))?;
    let mut out = BufWriter::new(file);
    let mut written = 0;
    let result = (|| -> Result<()> {
        serde_json::to_writer(
            &mut out,
            &Header {
                schema: SCHEMA.into(),
                layout: LAYOUT.into(),
                authority:
                    "candidate-scheduling-only; groups always require complete byte comparison"
                        .into(),
            },
        )?;
        out.write_all(b"\n")?;
        let mut last: Option<Vec<u8>> = None;
        loop {
            let rows = store.files_after(last.as_deref())?;
            let Some((tail, _)) = rows.last() else {
                break;
            };
            last = Some(path_bytes(&tail.path).to_vec());
            for (record, digest) in rows {
                let Some(digest) = digest else { continue };
                let o = &record.observation;
                serde_json::to_writer(
                    &mut out,
                    &Entry {
                        path: EncodedPath::from_path(&record.path),
                        device: o.identity.device,
                        inode: o.identity.inode,
                        size: o.size,
                        modified_ns: o.modified_ns,
                        changed_ns: o.changed_ns,
                        digest: hex::encode(digest),
                    },
                )?;
                out.write_all(b"\n")?;
                written += 1;
            }
        }
        let file = out
            .into_inner()
            .map_err(|e| crate::Error::Output(e.into_error()))?;
        file.sync_all()
            .map_err(|e| io("sync cache", &temporary, e))?;
        if claim(path)? != slot {
            return Err(crate::Error::Config(format!(
                "{} changed during the scan; the cache was not written",
                crate::paths::display_path(path)
            )));
        }
        match slot {
            Slot::Empty => {
                std::fs::hard_link(&temporary, path).map_err(|e| io("publish cache", path, e))?;
                std::fs::remove_file(&temporary)
                    .map_err(|e| io("remove temporary cache", &temporary, e))?;
            }
            Slot::Cache(_) => {
                std::fs::rename(&temporary, path).map_err(|e| io("publish cache", path, e))?
            }
        }
        crate::platform::sync_parent(path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.map(|()| written)
}
