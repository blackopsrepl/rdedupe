use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{operation} failed for {path:?}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("inventory database error: {0}")]
    Store(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("CSV output error: {0}")]
    Csv(#[from] csv::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("file changed while it was being observed: {0}")]
    Unstable(String),
    #[error("invalid or stale plan: {0}")]
    Plan(String),
    #[error("journal error: {0}")]
    Journal(String),
    #[error("filesystem capability unavailable: {0}")]
    Capability(String),
    #[error("scan was cancelled")]
    Cancelled,
    #[error("scan is partial; mutating operations are blocked")]
    Partial,
    #[error("scan is heuristic (trusted cache); mutating operations are blocked")]
    Heuristic,
    #[error("output error: {0}")]
    Output(#[from] std::io::Error),
}

impl Error {
    /// True when the failure is a closed output pipe (for example `rdedupe scan | head`).
    pub fn is_broken_pipe(&self) -> bool {
        match self {
            Error::Output(error) => error.kind() == std::io::ErrorKind::BrokenPipe,
            Error::Csv(error) => matches!(
                error.kind(),
                csv::ErrorKind::Io(inner) if inner.kind() == std::io::ErrorKind::BrokenPipe
            ),
            Error::Json(error) => error.io_error_kind() == Some(std::io::ErrorKind::BrokenPipe),
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn io(operation: &'static str, path: impl Into<PathBuf>, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.into(),
        source,
    }
}
