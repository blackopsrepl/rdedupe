use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EncodedPath {
    pub encoding: String,
    pub data: String,
}

#[cfg(unix)]
pub fn path_bytes(path: &Path) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes()
}

#[cfg(unix)]
pub fn bytes_path(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(OsString::from_vec(bytes.to_vec()))
}

#[cfg(not(unix))]
pub fn path_bytes(path: &Path) -> &[u8] {
    path.to_str().unwrap_or("").as_bytes()
}

#[cfg(not(unix))]
pub fn bytes_path(bytes: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
}

impl EncodedPath {
    pub fn from_path(path: &Path) -> Self {
        Self {
            encoding: if cfg!(unix) { "unix-bytes" } else { "utf-8" }.into(),
            data: hex::encode(path_bytes(path)),
        }
    }

    pub fn to_path(&self) -> Result<PathBuf> {
        let expected = if cfg!(unix) { "unix-bytes" } else { "utf-8" };
        if self.encoding != expected {
            return Err(Error::Plan(format!(
                "path encoding {:?} is unsupported on this platform",
                self.encoding
            )));
        }
        let bytes = hex::decode(&self.data)
            .map_err(|e| Error::Plan(format!("invalid path encoding: {e}")))?;
        if bytes.contains(&0) {
            return Err(Error::Plan("path contains NUL".into()));
        }
        Ok(bytes_path(&bytes))
    }
}

/// Terminal-safe rendering of a path for people. Valid UTF-8 stays readable; invalid bytes
/// become `\xNN`; control characters and invisible or bidirectional formatting characters
/// (which can disguise a name, e.g. U+202E) are escaped. Never parsed back.
pub fn display_path(path: &Path) -> String {
    escape_for_terminal(path_bytes(path), true)
}

/// Terminal-safe rendering of free text that may embed raw paths or OS messages (error
/// messages, reasons): the same escaping as [`display_path`] except that backslashes stay
/// as they are, so already-escaped paths inside the text are not escaped twice. The result
/// is always one line.
pub fn display_text(text: &str) -> String {
    escape_for_terminal(text.as_bytes(), false)
}

fn escape_for_terminal(bytes: &[u8], escape_backslash: bool) -> String {
    // Most names and messages are printable ASCII with nothing to escape.
    if bytes
        .iter()
        .all(|&b| (0x20..0x7f).contains(&b) && !(escape_backslash && b == b'\\'))
    {
        return String::from_utf8(bytes.to_vec()).expect("printable ASCII");
    }
    let mut out = String::new();
    for chunk in bytes.utf8_chunks() {
        for ch in chunk.valid().chars() {
            match ch {
                '\\' if escape_backslash => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x80 && c.is_control() => {
                    out.push_str(&format!("\\x{:02x}", c as u32))
                }
                c if c.is_control() || is_invisible_format(c) => {
                    out.push_str(&format!("\\u{{{:04x}}}", c as u32))
                }
                c => out.push(c),
            }
        }
        for &b in chunk.invalid() {
            out.push_str(&format!("\\x{b:02x}"));
        }
    }
    out
}

/// Format characters that render as nothing or reorder text.
fn is_invisible_format(c: char) -> bool {
    matches!(c as u32,
        0x00AD | 0x061C | 0x180E | 0x200B..=0x200F | 0x202A..=0x202E | 0x2060..=0x2069 | 0xFEFF)
}

pub fn stable_cmp(a: &Path, b: &Path) -> std::cmp::Ordering {
    path_bytes(a).cmp(path_bytes(b))
}

/// Whether `path` is byte-identical to its normalized absolute form: no `.`/`..`
/// components, no repeated or trailing separators. Imported paths must be, so that byte
/// comparisons (uniqueness, keeper chains, scope) mean what they say.
pub fn is_normalized_absolute(path: &Path) -> bool {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return false;
    }
    let rebuilt: PathBuf = path.components().collect();
    path_bytes(&rebuilt) == path_bytes(path)
}

pub fn validate_imported(path: &Path, roots: &[PathBuf]) -> Result<()> {
    if !is_normalized_absolute(path) {
        return Err(Error::Plan(format!(
            "path is not normalized absolute: {path:?}"
        )));
    }
    if !roots.iter().any(|root| path.starts_with(root)) {
        return Err(Error::Plan(format!("path is outside plan scope: {path:?}")));
    }
    Ok(())
}

pub fn os_contains(path: &Path, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    path_bytes(path)
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

pub fn os_str_bytes(value: &OsStr) -> &[u8] {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        value.as_bytes()
    }
    #[cfg(not(unix))]
    {
        value.to_str().unwrap_or("").as_bytes()
    }
}
