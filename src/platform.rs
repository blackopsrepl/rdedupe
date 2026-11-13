#[cfg(unix)]
pub mod dirfd;
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
            uid: metadata.uid(),
            gid: metadata.gid(),
            created_ns: metadata
                .created()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i128),
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
            uid: 0,
            gid: 0,
            created_ns: None,
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

/// BLAKE3 over a file's extended attributes (names and values, sorted by name), without
/// following symlinks. `None` when the platform or filesystem cannot list them. ACLs on
/// Linux and macOS are stored as extended attributes, so they are covered too.
pub fn xattr_digest(path: &Path) -> Option<[u8; 32]> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        let list = |buffer: *mut libc::c_char, size: usize| -> isize {
            // SAFETY: `buffer` is null (size query) or valid for `size` bytes.
            unsafe {
                #[cfg(target_os = "linux")]
                {
                    libc::llistxattr(c_path.as_ptr(), buffer, size)
                }
                #[cfg(target_os = "macos")]
                {
                    libc::listxattr(c_path.as_ptr(), buffer, size, libc::XATTR_NOFOLLOW)
                }
            }
        };
        let get = |name: &std::ffi::CStr, buffer: *mut libc::c_void, size: usize| -> isize {
            // SAFETY: as above; `name` is NUL-terminated.
            unsafe {
                #[cfg(target_os = "linux")]
                {
                    libc::lgetxattr(c_path.as_ptr(), name.as_ptr(), buffer, size)
                }
                #[cfg(target_os = "macos")]
                {
                    libc::getxattr(
                        c_path.as_ptr(),
                        name.as_ptr(),
                        buffer,
                        size,
                        0,
                        libc::XATTR_NOFOLLOW,
                    )
                }
            }
        };
        let needed = list(std::ptr::null_mut(), 0);
        if needed < 0 {
            return None;
        }
        let mut hasher = blake3::Hasher::new();
        if needed == 0 {
            // No attributes (the common case): one system call.
            return Some(*hasher.finalize().as_bytes());
        }
        let mut names = vec![0u8; needed as usize];
        let got = list(names.as_mut_ptr().cast(), names.len());
        if got < 0 {
            return None;
        }
        names.truncate(got as usize);
        let mut sorted: Vec<&[u8]> = names.split(|b| *b == 0).filter(|n| !n.is_empty()).collect();
        sorted.sort_unstable();
        for name in sorted {
            let c_name = std::ffi::CString::new(name).ok()?;
            let size = get(&c_name, std::ptr::null_mut(), 0);
            if size < 0 {
                return None;
            }
            let mut value = vec![0u8; size as usize];
            let got = get(&c_name, value.as_mut_ptr().cast(), value.len());
            if got < 0 {
                return None;
            }
            value.truncate(got as usize);
            hasher.update(&(name.len() as u64).to_le_bytes());
            hasher.update(name);
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(&value);
        }
        Some(*hasher.finalize().as_bytes())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = path;
        None
    }
}

/// Marker written into every scratch directory rdedupe creates, so abandoned ones can be
/// recognized and removed without guessing from names alone.
pub const SCRATCH_MARKER: &str = "RDEDUPE-SCRATCH";

/// Creates a private scratch directory (see [`create_unique_dir`]) carrying
/// [`SCRATCH_MARKER`].
pub fn create_scratch_dir(base: &Path, prefix: &str) -> Result<std::path::PathBuf> {
    let path = create_unique_dir(base, prefix)?;
    let marker = path.join(SCRATCH_MARKER);
    std::fs::write(&marker, b"rdedupe scratch; removed on exit\n")
        .map_err(|e| io("create scratch marker", &marker, e))?;
    Ok(path)
}

/// Whether process `pid` is known to have exited. Unknown (for example on platforms
/// without a probe, or when the process exists under another user) counts as alive.
pub fn process_is_gone(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        // SAFETY: signal 0 performs only the existence and permission check.
        let rc = unsafe { libc::kill(pid, 0) };
        rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Removes scratch directories `base/prefix-<pid>-*` left by rdedupe processes that no
/// longer exist (for example after `kill -9` or a power loss). Only directories carrying
/// [`SCRATCH_MARKER`] are touched. Returns how many were removed.
pub fn remove_abandoned_scratch(base: &Path, prefix: &str) -> Result<u64> {
    let entries = match std::fs::read_dir(base) {
        Ok(entries) => entries,
        Err(_) => return Ok(0),
    };
    let mut removed = 0;
    let lead = format!("{prefix}-");
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(rest) = name.to_str().and_then(|n| n.strip_prefix(&lead)) else {
            continue;
        };
        let Some(pid) = rest.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
            continue;
        };
        let path = entry.path();
        let is_dir = std::fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir());
        if pid == std::process::id()
            || !is_dir
            || !path.join(SCRATCH_MARKER).is_file()
            || !process_is_gone(pid)
        {
            continue;
        }
        std::fs::remove_dir_all(&path).map_err(|e| io("remove abandoned scratch", &path, e))?;
        removed += 1;
    }
    Ok(removed)
}

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
