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
    #[error("budget: {0}")]
    Budget(String),
    #[error("{completed} action(s) changed the filesystem before the run stopped: {cause}")]
    PartialExecution { completed: u64, cause: Box<Error> },
    #[error("scan was cancelled")]
    Cancelled,
    #[error("scan is partial; mutating operations are blocked")]
    Partial,
    #[error("scan is heuristic (trusted cache); mutating operations are blocked")]
    Heuristic,
    #[error("output error: {0}")]
    Output(#[from] std::io::Error),
}

/// What kind of failure an [`Error`] is, for reports and for deciding what to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    /// Walking or resolving a path failed.
    Traversal,
    /// Reading metadata failed.
    Metadata,
    /// Opening or reading content (or rdedupe's own inputs) failed.
    Read,
    /// Creating, moving, removing, writing or syncing failed: something may have changed.
    Mutation,
    /// The object changed while it was observed.
    Changed,
    /// A memory, descriptor or scratch-space budget was exhausted or impossible.
    Budget,
    /// Some actions completed before the run stopped.
    PartialExecution,
    Config,
    Plan,
    Journal,
    Capability,
    Cancelled,
    /// The scan cannot authorize changes (partial or heuristic).
    NotAuthoritative,
    Output,
    Internal,
}

/// Category of an I/O failure, by the operation rdedupe was performing. Every operation
/// string used with [`io`] is listed (a unit test checks the source).
pub fn operation_category(operation: &str) -> Category {
    match operation {
        "canonicalize" | "canonicalize policy root" | "canonicalize root" | "resolve path" => {
            Category::Traversal
        }
        "metadata" | "fstat" | "inspect cache" | "inspect quarantine" => Category::Metadata,
        "open"
        | "read"
        | "seek"
        | "open cache"
        | "read cache"
        | "open journal"
        | "read journal"
        | "open plan"
        | "read plan"
        | "rewind plan"
        | "open rdedupe key"
        | "read rdedupe key"
        | "read random bytes"
        | "read descriptor limit"
        | "open directory" => Category::Read,
        "sync report" | "create report (existing files are never overwritten)" => Category::Output,
        "set descriptor limit" => Category::Budget,
        _ => Category::Mutation,
    }
}

impl Error {
    pub fn category(&self) -> Category {
        match self {
            Error::Io { operation, .. } => operation_category(operation),
            Error::Store(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::DiskFull =>
            {
                Category::Budget
            }
            Error::Store(_) | Error::Json(_) => Category::Internal,
            Error::Csv(_) | Error::Output(_) => Category::Output,
            Error::Config(_) => Category::Config,
            Error::Budget(_) => Category::Budget,
            Error::Unstable(_) => Category::Changed,
            Error::Plan(_) => Category::Plan,
            Error::Journal(_) => Category::Journal,
            Error::Capability(_) => Category::Capability,
            Error::PartialExecution { .. } => Category::PartialExecution,
            Error::Cancelled => Category::Cancelled,
            Error::Partial | Error::Heuristic => Category::NotAuthoritative,
        }
    }

    /// True when retrying cannot make things worse: the failed step changed nothing on
    /// disk outside rdedupe's own scratch space. False after a failed mutation or a partial
    /// run; reconcile with `rdedupe recover --journal` first.
    pub fn retry_safe(&self) -> bool {
        !matches!(
            self.category(),
            Category::Mutation | Category::PartialExecution | Category::Journal
        )
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `io("operation", ...)` in the source is categorized deliberately: a new
    /// operation that is not a mutation must be added to [`operation_category`].
    #[test]
    fn every_io_operation_has_a_category() {
        let mutations = [
            "create cache",
            "create cache directory",
            "create journal",
            "create plan",
            "create private directory",
            "create quarantine directory",
            "create quarantine marker",
            "create rdedupe key",
            "create probe",
            "sync probe",
            "create scratch marker",
            "create rdedupe state directory",
            "create scratch base",
            "drop torn journal record",
            "link without replacement",
            "publish cache",
            "publish rdedupe key",
            "purge quarantine",
            "remove extra quarantine name",
            "remove original name",
            "remove temporary cache",
            "remove stale temporary file",
            "remove abandoned scratch",
            "rename without replacement",
            "restrict private directory",
            "sync cache",
            "sync directory",
            "sync journal",
            "sync plan",
            "write journal",
            "write quarantine marker",
            "write rdedupe key",
        ];
        let source_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut stack = vec![source_dir];
        let mut seen = 0;
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.ends_with("error.rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap();
                let compact: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
                for piece in compact
                    .split("io( \"")
                    .skip(1)
                    .chain(compact.split("io(\"").skip(1))
                {
                    let operation = piece.split('"').next().unwrap();
                    seen += 1;
                    if operation_category(operation) == Category::Mutation {
                        assert!(
                            mutations.contains(&operation),
                            "{operation:?} in {path:?} falls through to Mutation; categorize it"
                        );
                    }
                }
            }
        }
        assert!(seen > 40, "found only {seen} io operations");
    }
}
