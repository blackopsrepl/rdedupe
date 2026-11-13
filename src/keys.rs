//! Per-user authentication key for plans and journals.
//!
//! A plan or journal is a set of instructions to move or delete files, so it must not be
//! accepted just because it is well formed and its checksum matches: anyone able to edit
//! the file could recompute a checksum. rdedupe therefore seals plans and every journal
//! record with a BLAKE3 keyed hash under a random 256-bit key kept in the user's private
//! state directory. Only processes running as a user who can read that key can produce
//! material `apply`, `restore` and `purge` will act on. Structural validation still runs on
//! everything; the key is an additional layer, not a replacement.
//!
//! The directory is `$RDEDUPE_STATE_DIR`, else `$XDG_STATE_HOME/rdedupe`, else
//! `$HOME/.local/state/rdedupe`.

use crate::error::{io, Error, Result};
use std::io::{Read, Write};
use std::path::PathBuf;

const KEY_FILE: &str = "seal.key";

pub fn state_dir() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os("RDEDUPE_STATE_DIR").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(dir));
    }
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(dir).join("rdedupe"));
    }
    match std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        Some(home) => Ok(PathBuf::from(home).join(".local/state/rdedupe")),
        None => Err(Error::Config(
            "cannot locate the rdedupe state directory: set RDEDUPE_STATE_DIR or HOME".into(),
        )),
    }
}

fn read_key(path: &std::path::Path) -> Result<Option<[u8; 32]>> {
    match std::fs::File::open(path) {
        Ok(mut file) => {
            let mut key = [0u8; 32];
            file.read_exact(&mut key)
                .map_err(|e| io("read rdedupe key", path, e))?;
            Ok(Some(key))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io("open rdedupe key", path, error)),
    }
}

/// Loads the key, failing if it does not exist. Used when verifying.
pub fn existing() -> Result<[u8; 32]> {
    let path = state_dir()?.join(KEY_FILE);
    read_key(&path)?.ok_or_else(|| {
        Error::Plan(format!(
            "no rdedupe key at {}: plans and journals can only be used by the user who created them",
            crate::paths::display_path(&path)
        ))
    })
}

/// Loads the key, creating it (atomically, mode 0600) on first use. Used when sealing.
pub fn load_or_create() -> Result<[u8; 32]> {
    let dir = state_dir()?;
    let path = dir.join(KEY_FILE);
    if let Some(key) = read_key(&path)? {
        return Ok(key);
    }
    create_private_dir_all(&dir)?;
    let mut key = [0u8; 32];
    random_bytes(&mut key)?;
    let staging = crate::platform::create_unique_dir(&dir, ".seal-key")?;
    let temporary = staging.join(KEY_FILE);
    let written = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|e| io("create rdedupe key", &temporary, e))?;
        file.write_all(&key)
            .and_then(|()| file.sync_all())
            .map_err(|e| io("write rdedupe key", &temporary, e))?;
        // A hard link publishes the complete key atomically and never replaces one that a
        // concurrent process published first.
        match std::fs::hard_link(&temporary, &path) {
            Ok(()) => crate::platform::sync_parent(&path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(io("publish rdedupe key", &path, error)),
        }
    })();
    let _ = std::fs::remove_file(&temporary);
    let _ = std::fs::remove_dir(&staging);
    written?;
    read_key(&path)?.ok_or_else(|| Error::Config("rdedupe key vanished after creation".into()))
}

fn create_private_dir_all(dir: &std::path::Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .map_err(|e| io("create rdedupe state directory", dir, e))
}

fn random_bytes(out: &mut [u8]) -> Result<()> {
    #[cfg(unix)]
    {
        let path = std::path::Path::new("/dev/urandom");
        std::fs::File::open(path)
            .and_then(|mut file| file.read_exact(out))
            .map_err(|e| io("read random bytes", path, e))
    }
    #[cfg(not(unix))]
    {
        let _ = out;
        Err(Error::Capability(
            "no random source on this platform".into(),
        ))
    }
}

/// Keyed BLAKE3 over a domain label and data, as lowercase hex.
pub fn seal(key: &[u8; 32], domain: &str, data: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(domain.as_bytes());
    hasher.update(&[0]);
    hasher.update(data);
    hasher.finalize().to_hex().to_string()
}

/// Constant-time comparison of two hex seals.
pub fn verify_hex(expected: &str, actual: &str) -> bool {
    match (
        blake3::Hash::from_hex(expected),
        blake3::Hash::from_hex(actual),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Constant-time check of a seal.
pub fn verify(key: &[u8; 32], domain: &str, data: &[u8], seal_hex: &str) -> bool {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(domain.as_bytes());
    hasher.update(&[0]);
    hasher.update(data);
    blake3::Hash::from_hex(seal_hex).is_ok_and(|expected| hasher.finalize() == expected)
}
