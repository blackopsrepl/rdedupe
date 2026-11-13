//! Metadata observation and change-detecting file opens.

use crate::error::{io, Error, Result};
use crate::model::Observation;
use crate::platform;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub fn observe_path(path: &Path) -> Result<Observation> {
    platform::observe_path(path)
}

/// Opens a regular file for reading without following a final symlink and without
/// blocking on FIFOs, returning the descriptor and its `fstat` observation.
pub fn open_observed(path: &Path) -> Result<(File, Observation)> {
    open_regular(path, false)
}

/// Like [`open_observed`] but with write access (required by extent sharing).
pub fn open_observed_rw(path: &Path) -> Result<(File, Observation)> {
    open_regular(path, true)
}

fn open_regular(path: &Path, write: bool) -> Result<(File, Observation)> {
    let mut options = OpenOptions::new();
    options.read(true).write(write);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|e| io("open", path, e))?;
    let metadata = file.metadata().map_err(|e| io("fstat", path, e))?;
    if !metadata.is_file() {
        return Err(Error::Unstable(format!(
            "{path:?} is no longer a regular file"
        )));
    }
    Ok((file, platform::observation(&metadata)))
}

pub fn ensure_stable(path: &Path, before: &Observation, file: &File) -> Result<()> {
    let after = platform::observe_file(file).map_err(|e| io("fstat", path, e))?;
    if &after != before {
        return Err(Error::Unstable(format!("{path:?}")));
    }
    Ok(())
}
