//! Capability detection: what the platform supports, and what a given filesystem does.

use crate::error::{io, Result};
use std::path::Path;

/// Quarantine needs an atomic no-replace rename that also works on directories (the
/// quarantine root is published that way).
pub const QUARANTINE_PLATFORM: &str = if cfg!(target_os = "linux") {
    "supported (renameat2 RENAME_NOREPLACE; same filesystem only)"
} else if cfg!(target_os = "macos") {
    "supported (renamex_np RENAME_EXCL; same filesystem only)"
} else {
    "unsupported (no atomic no-replace directory rename on this platform)"
};

pub const EXTENT_PLATFORM: &str = if cfg!(target_os = "linux") {
    "FIDEDUPERANGE (filesystem-dependent; probe with --path DIR)"
} else {
    "unsupported"
};

pub struct Probe {
    pub quarantine: String,
    pub extent: String,
}

/// Exercises the real operations on `dir`'s filesystem inside a private scratch directory
/// and removes everything it created.
pub fn filesystem(dir: &Path) -> Result<Probe> {
    let scratch = crate::platform::create_scratch_dir(dir, "rdedupe-probe")?;
    let result = run(&scratch);
    let _ = std::fs::remove_dir_all(&scratch);
    result
}

fn run(scratch: &Path) -> Result<Probe> {
    use crate::platform::dirfd::DirFd;
    use std::ffi::OsStr;
    let quarantine = (|| -> Result<String> {
        std::fs::create_dir(scratch.join("a")).map_err(|e| io("create probe", scratch, e))?;
        std::fs::create_dir(scratch.join("c")).map_err(|e| io("create probe", scratch, e))?;
        let dir = DirFd::open(scratch)?;
        if dir
            .rename_noreplace(OsStr::new("a"), &dir, OsStr::new("c"))
            .is_ok()
        {
            return Ok("unsupported (a rename replaced an existing entry)".into());
        }
        dir.rename_noreplace(OsStr::new("a"), &dir, OsStr::new("b"))?;
        Ok("supported".into())
    })()
    .unwrap_or_else(|error| format!("unsupported ({error})"));
    let extent = extent(scratch);
    Ok(Probe { quarantine, extent })
}

#[cfg(target_os = "linux")]
fn extent(scratch: &Path) -> String {
    let attempt = || -> Result<String> {
        let content = vec![0x5a_u8; 64 * 1024];
        let (a, b) = (scratch.join("x"), scratch.join("y"));
        std::fs::write(&a, &content).map_err(|e| io("create probe", &a, e))?;
        std::fs::write(&b, &content).map_err(|e| io("create probe", &b, e))?;
        let source = std::fs::File::open(&a).map_err(|e| io("open", &a, e))?;
        let destination = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&b)
            .map_err(|e| io("open", &b, e))?;
        source.sync_all().map_err(|e| io("sync probe", &a, e))?;
        destination
            .sync_all()
            .map_err(|e| io("sync probe", &b, e))?;
        Ok(
            match crate::platform::linux::dedupe_range(
                &source,
                &destination,
                0,
                content.len() as u64,
            )? {
                crate::platform::linux::RangeOutcome::Shared(_) => "supported".into(),
                other => format!("unsupported ({other:?})"),
            },
        )
    };
    attempt().unwrap_or_else(|error| format!("unsupported ({error})"))
}

#[cfg(not(target_os = "linux"))]
fn extent(_scratch: &Path) -> String {
    "unsupported (no FIDEDUPERANGE on this platform)".into()
}
