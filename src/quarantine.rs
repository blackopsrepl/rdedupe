//! Same-filesystem private quarantine.
//!
//! A target is quarantined by renaming it (without replacement) into
//! `<parent>/.rdedupe-quarantine/<journal namespace>/`, which is always on the target's own
//! filesystem. The quarantine root carries a marker file so that later scans skip exactly
//! the directories rdedupe owns and nothing else with a similar name.

use crate::error::{io, Error, Result};
use crate::model::FileIdentity;
use crate::paths::EncodedPath;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub const QUARANTINE_DIR: &str = ".rdedupe-quarantine";
pub const MARKER_FILE: &str = "RDEDUPE-QUARANTINE";
const MARKER_CONTENT: &[u8] = b"rdedupe-quarantine-v1\nThis directory is owned by rdedupe. Use `rdedupe restore` or `rdedupe purge` with the journal that created it.\n";

/// True only for a directory named [`QUARANTINE_DIR`] whose marker file has the exact
/// rdedupe content. A user directory that merely shares the name is still scanned.
pub fn is_owned_quarantine_dir(path: &Path) -> bool {
    path.file_name().is_some_and(|n| n == QUARANTINE_DIR)
        && fs::read(path.join(MARKER_FILE)).is_ok_and(|bytes| bytes == MARKER_CONTENT)
}

fn private_dir(path: &Path) -> Result<()> {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io("create quarantine directory", path, error)),
    }
    let metadata = fs::symlink_metadata(path).map_err(|e| io("inspect quarantine", path, e))?;
    if !metadata.is_dir() {
        return Err(Error::Capability(format!(
            "{path:?} exists and is not a directory"
        )));
    }
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|e| io("restrict quarantine directory", path, e))?;
    Ok(())
}

/// Prepares the quarantine directory for `target` and returns a fresh destination name.
pub fn destination(target: &Path, journal: &Path, sequence: u64) -> Result<PathBuf> {
    let parent = target
        .parent()
        .ok_or_else(|| Error::Plan("target has no parent".into()))?;
    let root = parent.join(QUARANTINE_DIR);
    private_dir(&root)?;
    let marker = root.join(MARKER_FILE);
    if !crate::platform::entry_exists(&marker)? {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
            .map_err(|e| io("create quarantine marker", &marker, e))?;
        file.write_all(MARKER_CONTENT)
            .and_then(|()| file.sync_all())
            .map_err(|e| io("write quarantine marker", &marker, e))?;
    } else if !is_owned_quarantine_dir(&root) {
        return Err(Error::Capability(format!(
            "{root:?} exists but is not an rdedupe quarantine"
        )));
    }
    let namespace = blake3::hash(crate::paths::path_bytes(journal))
        .to_hex()
        .to_string();
    let directory = root.join(&namespace[..32]);
    private_dir(&directory)?;
    crate::platform::sync_parent(&directory)?;
    if crate::platform::observe_path(parent)?.identity.device
        != crate::platform::observe_path(&directory)?.identity.device
    {
        return Err(Error::Capability(format!(
            "quarantine {directory:?} is not on the target's filesystem; refusing copy-and-delete"
        )));
    }
    let name = target
        .file_name()
        .map(|n| EncodedPath::from_path(Path::new(n)).data)
        .unwrap_or_default();
    // Hex names keep arbitrary bytes and stay within NAME_MAX for long originals.
    let name = &name[..name.len().min(160)];
    Ok(directory.join(format!("{sequence:016x}-{name}")))
}

/// Moves `target` into quarantine without replacing anything, then verifies that the moved
/// object is the one that was validated. If another object was swapped in between, it is
/// moved back and the action fails.
pub fn move_into(target: &Path, destination: &Path, expected: FileIdentity) -> Result<()> {
    crate::platform::rename_noreplace(target, destination)?;
    let moved = crate::platform::observe_path(destination)?;
    if moved.identity != expected {
        crate::platform::rename_noreplace(destination, target)?;
        return Err(Error::Unstable(format!(
            "{target:?} was replaced between validation and quarantine; moved back"
        )));
    }
    crate::platform::sync_parent(target)?;
    crate::platform::sync_parent(destination)
}

/// Moves a quarantined object back. Never replaces an entry that now exists at `original`.
pub fn restore(source: &Path, original: &Path) -> Result<()> {
    crate::platform::rename_noreplace(source, original).map_err(|error| match error {
        Error::Io { source: e, .. } if e.kind() == std::io::ErrorKind::AlreadyExists => {
            Error::Journal(format!(
                "restore refuses to overwrite {original:?}, which now exists"
            ))
        }
        other => other,
    })?;
    crate::platform::sync_parent(source)?;
    crate::platform::sync_parent(original)
}

/// After an object left quarantine (restored or purged), removes its journal's namespace
/// directory if empty, and then the quarantine root if the marker is all it still holds.
/// Anything else present (other journals' objects, unexpected entries) keeps the directory
/// and its marker, so later scans keep skipping it. Best effort: failures leave a valid,
/// marked quarantine behind and are not errors.
pub fn prune(object: &Path) {
    let Some(namespace) = object.parent() else {
        return;
    };
    if fs::remove_dir(namespace).is_err() {
        return;
    }
    let Some(root) = namespace.parent() else {
        return;
    };
    if !is_owned_quarantine_dir(root) {
        return;
    }
    let only_marker = fs::read_dir(root).is_ok_and(|entries| {
        let names: Vec<_> = entries
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .collect();
        names.len() == 1 && names[0] == MARKER_FILE
    });
    if only_marker && fs::remove_file(root.join(MARKER_FILE)).is_ok() {
        let _ = fs::remove_dir(root);
        let _ = crate::platform::sync_parent(root);
    }
}
