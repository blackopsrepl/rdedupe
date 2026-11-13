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

/// Returns the quarantine root beside `target`, creating it atomically if needed: the
/// marker is written into a fresh temporary directory, which is then renamed into place
/// without replacement. An existing directory is used only if it carries the exact marker;
/// a user directory that merely has the name is refused and never modified.
fn quarantine_root(parent: &Path) -> Result<PathBuf> {
    let root = parent.join(QUARANTINE_DIR);
    if crate::platform::entry_exists(&root)? {
        return owned_or_refuse(&root);
    }
    let staging = crate::platform::create_unique_dir(parent, ".rdedupe-quarantine-staging")?;
    let staged = (|| -> Result<()> {
        let marker = staging.join(MARKER_FILE);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
            .map_err(|e| io("create quarantine marker", &marker, e))?;
        file.write_all(MARKER_CONTENT)
            .and_then(|()| file.sync_all())
            .map_err(|e| io("write quarantine marker", &marker, e))?;
        crate::platform::sync_parent(&marker)
    })();
    let published = staged.and_then(|()| crate::platform::rename_noreplace(&staging, &root));
    if published.is_err() {
        let _ = fs::remove_file(staging.join(MARKER_FILE));
        let _ = fs::remove_dir(&staging);
        // Lost a race with another creator, or something else appeared: re-check.
        if crate::platform::entry_exists(&root)? {
            return owned_or_refuse(&root);
        }
        published?;
    }
    crate::platform::sync_parent(&root)?;
    Ok(root)
}

fn owned_or_refuse(root: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(root).map_err(|e| io("inspect quarantine", root, e))?;
    if metadata.is_dir() && is_owned_quarantine_dir(root) {
        Ok(root.to_path_buf())
    } else {
        Err(Error::Capability(format!(
            "{} exists but is not an rdedupe quarantine; it is left untouched",
            crate::paths::display_path(root)
        )))
    }
}

/// Prepares the quarantine directory for `target` and returns a fresh destination name.
pub fn destination(target: &Path, journal: &Path, sequence: u64) -> Result<PathBuf> {
    let parent = target
        .parent()
        .ok_or_else(|| Error::Plan("target has no parent".into()))?;
    let root = quarantine_root(parent)?;
    let namespace = blake3::hash(crate::paths::path_bytes(journal))
        .to_hex()
        .to_string();
    let directory = root.join(&namespace[..32]);
    match fs::create_dir(&directory) {
        Ok(()) => crate::platform::sync_parent(&directory)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(&directory)
                .map_err(|e| io("inspect quarantine", &directory, e))?;
            if !metadata.is_dir() {
                return Err(Error::Capability(format!(
                    "{} is not a quarantine directory",
                    crate::paths::display_path(&directory)
                )));
            }
        }
        Err(error) => return Err(io("create quarantine directory", &directory, error)),
    }
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

/// Validates a quarantine path read from a journal: it must be exactly where
/// [`destination`] would put this action's object for `target`, inside a marker-owned
/// quarantine root. Journals are untrusted input; nothing else may be renamed or removed.
pub fn validate_recorded(target: &Path, recorded: &Path, sequence: u64) -> Result<()> {
    let refuse = |why: &str| {
        Err(Error::Journal(format!(
            "journal names {} as the quarantine of {}: {why}",
            crate::paths::display_path(recorded),
            crate::paths::display_path(target)
        )))
    };
    let (Some(parent), Some(name)) = (target.parent(), target.file_name()) else {
        return refuse("target has no parent");
    };
    let root = parent.join(QUARANTINE_DIR);
    let Some(namespace) = recorded.parent() else {
        return refuse("no namespace directory");
    };
    if namespace.parent() != Some(root.as_path()) {
        return refuse("not in the target's quarantine directory");
    }
    let ns = namespace
        .file_name()
        .map(|n| n.as_encoded_bytes())
        .unwrap_or_default();
    if ns.len() != 32
        || !ns
            .iter()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return refuse("malformed namespace");
    }
    let hex = EncodedPath::from_path(Path::new(name)).data;
    let expected = format!("{sequence:016x}-{}", &hex[..hex.len().min(160)]);
    if recorded.file_name().map(|n| n.as_encoded_bytes()) != Some(expected.as_bytes()) {
        return refuse("object name does not match the action");
    }
    if crate::platform::entry_exists(&root)? && !is_owned_quarantine_dir(&root) {
        return refuse("quarantine root is not marker-owned");
    }
    Ok(())
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
