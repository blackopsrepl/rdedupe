//! Exact, resource-bounded, recoverable duplicate-file deduplication.

pub mod accounting;
pub mod cache;
pub mod candidates;
pub mod cli;
pub mod commands;
pub mod compare;
pub mod config;
pub mod delete;
pub mod error;
pub mod fileinfo;
pub mod fingerprint;
pub mod fs;
pub mod journal;
pub mod model;
pub mod paths;
pub mod pipeline;
pub mod plan;
pub mod platform;
pub mod policy;
pub mod quarantine;
pub mod recovery;
pub mod report;
pub mod scheduler;
pub mod store;

pub use config::{Budget, ScanConfig};
pub use error::{Error, Result};
pub use model::{ActionKind, Completeness, Exactness, Metrics, ScanSummary};
pub use pipeline::{scan, ScanResult};
pub use policy::Policy;
