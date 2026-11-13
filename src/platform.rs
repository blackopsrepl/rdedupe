#[cfg(target_os = "linux")]
pub mod linux;

use crate::error::{io, Error, Result};
use crate::model::{FileIdentity, Observation};
use std::fs::{File, Metadata};
use std::path::Path;
#[cfg(not(unix))]
use std::time::UNIX_EPOCH;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub fn observation(metadata: &Metadata) -> Observation {
    #[cfg(unix)]
    {
        Observation {
            identity: FileIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
            size: metadata.len(),
            allocated: metadata.blocks().saturating_mul(512),
            modified_ns: metadata.mtime() as i128 * 1_000_000_000 + metadata.mtime_nsec() as i128,
            changed_ns: metadata.ctime() as i128 * 1_000_000_000 + metadata.ctime_nsec() as i128,
            mode: metadata.mode(),
            links: metadata.nlink(),
        }
    }
    #[cfg(not(unix))]
    {
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|v| v.duration_since(UNIX_EPOCH).ok())
            .map(|v| v.as_nanos() as i128)
            .unwrap_or_default();
        Observation {
            identity: FileIdentity {
                device: 0,
                inode: 0,
            },
            size: metadata.len(),
            allocated: metadata.len(),
            modified_ns,
            changed_ns: modified_ns,
            mode: 0,
            links: 1,
        }
    }
}

pub fn observe_path(path: &Path) -> Result<Observation> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| io("metadata", path, e))?;
    Ok(observation(&metadata))
}

pub fn observe_file(file: &File) -> std::io::Result<Observation> {
    file.metadata().map(|m| observation(&m))
}

/// Creates a new private (`0700`) directory `base/prefix-<pid>-<counter>-<nanos>`, retrying
/// with a fresh name if one already exists. Never reuses an existing directory.
pub fn create_unique_dir(base: &Path, prefix: &str) -> Result<std::path::PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    loop {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = base.join(format!("{prefix}-{}-{count}-{nanos}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                        .map_err(|e| io("restrict private directory", &path, e))?;
                }
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io("create private directory", &path, error)),
        }
    }
}

/// Returns whether a directory entry exists at `path` without following a final symlink.
/// Lowers this process's open-descriptor limit (the `RLIMIT_NOFILE` soft limit) to `max`,
/// so a descriptor budget is enforced by the kernel rather than only estimated. Never
/// raises the limit.
pub fn limit_open_files(max: usize) -> Result<()> {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit/setrlimit only read and write the struct passed by pointer.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return Err(io(
                "read descriptor limit",
                "",
                std::io::Error::last_os_error(),
            ));
        }
        let wanted = max as libc::rlim_t;
        if limit.rlim_cur > wanted {
            limit.rlim_cur = wanted;
            // SAFETY: as above.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
                return Err(io(
                    "set descriptor limit",
                    "",
                    std::io::Error::last_os_error(),
                ));
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = max;
        Ok(())
    }
}

pub fn entry_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io("metadata", path, error)),
    }
}

pub fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|e| io("sync directory", parent, e))
}

/// Renames `from` to `to`, failing instead of replacing an existing destination entry.
pub fn rename_noreplace(from: &Path, to: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::rename_noreplace(from, to)
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = |p: &Path| {
            std::ffi::CString::new(p.as_os_str().as_bytes())
                .map_err(|_| Error::Capability("path contains a NUL byte".into()))
        };
        let (source, destination) = (c(from)?, c(to)?);
        // SAFETY: both pointers are valid NUL-terminated strings for the call's duration.
        let rc =
            unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
        if rc == 0 {
            Ok(())
        } else {
            Err(io(
                "rename without replacement",
                to,
                std::io::Error::last_os_error(),
            ))
        }
    }
    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    {
        // No atomic no-replace rename: a hard link is created atomically and fails with
        // EEXIST; the source name is then removed. Both names refer to the same inode in
        // between, so content is never lost. Directories cannot be linked, so creating a
        // quarantine root fails here and its actions are skipped, never forced.
        if std::fs::symlink_metadata(from).is_ok_and(|m| m.is_dir()) {
            return Err(Error::Capability(
                "no-replace rename of a directory is unsupported on this platform".into(),
            ));
        }
        std::fs::hard_link(from, to).map_err(|e| io("link without replacement", to, e))?;
        std::fs::remove_file(from).map_err(|e| io("remove original name", from, e))
    }
    #[cfg(not(unix))]
    {
        let _ = (from, to);
        Err(Error::Capability(
            "no-replace rename is unsupported on this platform".into(),
        ))
    }
}

/// Takes an exclusive, non-blocking advisory lock that the kernel releases on process exit.
/// Retries for up to one second: a lock can stay briefly held after its owner closed the
/// file when a concurrently forked child still shares the open file description until it
/// calls `exec`.
pub fn lock_exclusive(file: &File, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let mut attempts = 0;
        loop {
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc == 0 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            attempts += 1;
            if error.kind() != std::io::ErrorKind::WouldBlock || attempts >= 20 {
                return Err(Error::Journal(format!(
                    "journal {path:?} is in use by another rdedupe process: {error}"
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (file, path);
        Err(Error::Capability(
            "journal locking is unsupported on this platform".into(),
        ))
    }
}
