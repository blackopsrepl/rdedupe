//! Disposable fingerprint cache.
//!
//! The cache stores, per path, the sampled fingerprint together with the validators it was
//! computed under (device, inode, size, mtime, ctime). It never establishes equality: groups
//! always come from complete byte comparison.
//!
//! * In the default **exact** mode the cache is only written (refreshed), never consulted,
//!   because a stale entry could otherwise eliminate a genuine candidate (a "stale
//!   negative"), and metadata validators cannot prove unchanged content.
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

pub fn load(path: &Path, store: &Store) -> Result<Loaded> {
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
                },
                digest,
            },
        )?;
        count += 1;
    }
    store.commit()?;
    Ok(Loaded::Entries(count))
}

/// Atomically replaces the cache with the fingerprints computed (or reused) by this scan.
pub fn write(path: &Path, store: &Store) -> Result<u64> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    std::fs::create_dir_all(parent).map_err(|e| io("create cache directory", parent, e))?;
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
        std::fs::rename(&temporary, path).map_err(|e| io("publish cache", path, e))?;
        crate::platform::sync_parent(path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.map(|()| written)
}
