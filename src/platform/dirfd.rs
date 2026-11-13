//! Descriptor-relative directory operations.
//!
//! Quarantine, restore and purge act on one directory entry at a time. Opening the
//! containing directory once (`O_DIRECTORY | O_NOFOLLOW`) and then checking, renaming and
//! unlinking *by name relative to that descriptor* means a parent or ancestor swapped for a
//! symlink after validation cannot redirect the operation elsewhere: every step addresses
//! the same directory object.

use crate::error::{io, Error, Result};
use crate::model::{FileIdentity, Observation};
use std::ffi::{CString, OsStr};
use std::path::{Path, PathBuf};

pub struct DirFd {
    path: PathBuf,
    fd: std::os::fd::OwnedFd,
}

fn c_name(name: &OsStr) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.contains(&b'/') {
        return Err(Error::Plan(format!(
            "not a single path component: {name:?}"
        )));
    }
    CString::new(bytes).map_err(|_| Error::Plan(format!("name contains NUL: {name:?}")))
}

/// Splits `path` into its parent directory and final component.
pub fn split(path: &Path) -> Result<(&Path, &OsStr)> {
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => Ok((parent, name)),
        _ => Err(Error::Plan(format!("{path:?} has no parent directory"))),
    }
}

// `stat` field types differ between platforms (for example `st_mode` is `u16` on macOS),
// so the casts are needed even where they are no-ops.
#[allow(clippy::unnecessary_cast)]
fn observation(stat: &libc::stat) -> Observation {
    Observation {
        identity: FileIdentity {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
        },
        size: stat.st_size as u64,
        allocated: (stat.st_blocks as u64).saturating_mul(512),
        modified_ns: stat.st_mtime as i128 * 1_000_000_000 + stat.st_mtime_nsec as i128,
        changed_ns: stat.st_ctime as i128 * 1_000_000_000 + stat.st_ctime_nsec as i128,
        mode: stat.st_mode as u32,
        links: stat.st_nlink as u64,
        uid: stat.st_uid,
        gid: stat.st_gid,
        created_ns: None,
    }
}

impl DirFd {
    /// Opens a directory without following a final symlink.
    pub fn open(path: &Path) -> Result<Self> {
        use std::os::fd::FromRawFd;
        use std::os::unix::ffi::OsStrExt;
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| Error::Plan(format!("path contains NUL: {path:?}")))?;
        // SAFETY: valid NUL-terminated path; the returned descriptor is owned below.
        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io("open directory", path, std::io::Error::last_os_error()));
        }
        Ok(Self {
            path: path.to_path_buf(),
            // SAFETY: `fd` is a fresh descriptor we own.
            fd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
        })
    }

    fn raw(&self) -> libc::c_int {
        use std::os::fd::AsRawFd;
        self.fd.as_raw_fd()
    }

    /// Metadata of entry `name` without following symlinks; `None` if it does not exist.
    pub fn stat(&self, name: &OsStr) -> Result<Option<Observation>> {
        let c = c_name(name)?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: valid descriptor, NUL-terminated name, writable stat buffer.
        let rc = unsafe {
            libc::fstatat(
                self.raw(),
                c.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(io("metadata", self.path.join(name), error));
        }
        // SAFETY: fstatat succeeded and initialized the buffer.
        Ok(Some(observation(unsafe { &stat.assume_init() })))
    }

    /// Renames `name` here to `to_name` in `to`, failing if `to_name` exists.
    pub fn rename_noreplace(&self, name: &OsStr, to: &DirFd, to_name: &OsStr) -> Result<()> {
        let (from, dest) = (c_name(name)?, c_name(to_name)?);
        let failed = |operation| {
            Err(io(
                operation,
                to.path.join(to_name),
                std::io::Error::last_os_error(),
            ))
        };
        #[cfg(target_os = "linux")]
        {
            // SAFETY: valid descriptors and NUL-terminated names.
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    self.raw(),
                    from.as_ptr(),
                    to.raw(),
                    dest.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            if rc != 0 {
                return failed("rename without replacement");
            }
            Ok(())
        }
        #[cfg(target_os = "macos")]
        {
            // SAFETY: as above.
            let rc = unsafe {
                libc::renameatx_np(
                    self.raw(),
                    from.as_ptr(),
                    to.raw(),
                    dest.as_ptr(),
                    libc::RENAME_EXCL,
                )
            };
            if rc != 0 {
                return failed("rename without replacement");
            }
            Ok(())
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            // No atomic no-replace rename: link (fails if the name exists), then unlink
            // the old name. Both names refer to the same inode in between.
            // SAFETY: as above.
            if unsafe { libc::linkat(self.raw(), from.as_ptr(), to.raw(), dest.as_ptr(), 0) } != 0 {
                return failed("link without replacement");
            }
            // SAFETY: as above.
            if unsafe { libc::unlinkat(self.raw(), from.as_ptr(), 0) } != 0 {
                return Err(io(
                    "remove original name",
                    self.path.join(name),
                    std::io::Error::last_os_error(),
                ));
            }
            Ok(())
        }
    }

    /// Removes file entry `name`.
    pub fn unlink(&self, name: &OsStr) -> Result<()> {
        let c = c_name(name)?;
        // SAFETY: valid descriptor and NUL-terminated name.
        if unsafe { libc::unlinkat(self.raw(), c.as_ptr(), 0) } != 0 {
            return Err(io(
                "purge quarantine",
                self.path.join(name),
                std::io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    /// Makes entry changes in this directory durable.
    pub fn sync(&self) -> Result<()> {
        // SAFETY: valid descriptor.
        if unsafe { libc::fsync(self.raw()) } != 0 {
            return Err(io(
                "sync directory",
                &self.path,
                std::io::Error::last_os_error(),
            ));
        }
        Ok(())
    }
}
