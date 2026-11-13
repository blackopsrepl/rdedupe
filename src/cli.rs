use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "rdedupe",
    version,
    author,
    about = "Exact, resource-bounded, recoverable duplicate-file deduplication",
    long_about = "Finds files with byte-identical content (verified by complete comparison, never by hash alone), \
reports them, and optionally moves redundant copies into a recoverable quarantine or shares their storage \
with filesystem extent deduplication. Mutation always goes through a saved plan that is re-validated \
against the filesystem immediately before every action.",
    after_help = "Workflow:\n  rdedupe scan DIR                         report exact duplicate groups (never mutates)\n  rdedupe plan DIR --output cleanup.plan   save a verified action plan\n  rdedupe apply --plan cleanup.plan --journal cleanup.journal\n  rdedupe restore --journal cleanup.journal   undo quarantine\n  rdedupe purge --journal cleanup.journal     permanently remove quarantined copies\n\nExit codes: 0 complete, 2 partial (unresolved files, skipped or failed actions, recovery conflicts),\n3 confirmation declined, 4 error, 64 invalid invocation, 130 cancelled, 141 output pipe closed.\n\nLegacy form: rdedupe [DIR] [--pattern P] [--csv FILE] [--delete --dry-run]"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
    /// Legacy: directory to scan when no subcommand is given.
    #[arg(value_name = "DIR", default_value = ".")]
    pub legacy_roots: Vec<PathBuf>,
    /// Legacy: substring every inventoried path must contain.
    #[arg(long, default_value = "")]
    pub pattern: String,
    /// Legacy: also write the CSV report to this new file.
    #[arg(long, value_name = "FILE")]
    pub csv: Option<PathBuf>,
    /// Legacy: removed; prints the plan/apply migration. With --dry-run, reports only.
    #[arg(long)]
    pub delete: bool,
    /// Legacy: with --delete, report what is eligible without mutating anything.
    #[arg(long, requires = "delete")]
    pub dry_run: bool,
    /// Legacy: accepted with --delete for compatibility; deletion still refuses.
    #[arg(long, requires = "delete")]
    pub force: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Scan and report exact duplicate groups. Never mutates the scanned tree.
    Scan(ScanArgs),
    /// Scan and save a versioned, checksummed action plan (requires a complete, exact scan).
    Plan(PlanArgs),
    /// Apply a saved plan, re-validating every action against the filesystem first.
    Apply(ApplyArgs),
    /// Move quarantined files back without overwriting anything at their original paths.
    Restore(RestoreArgs),
    /// Permanently remove quarantined files whose keeper is still present and identical.
    Purge(PurgeArgs),
    /// Inspect a journal and report each action's recorded state and current facts.
    Recover(RecoverArgs),
    /// Print version, platform, format and filesystem capabilities.
    Capabilities(CapabilitiesArgs),
}

#[derive(Args, Debug)]
pub struct CapabilitiesArgs {
    /// Probe quarantine and extent support on the filesystem holding this directory, with
    /// throwaway files in a private scratch directory there (removed afterwards).
    #[arg(long, value_name = "DIR")]
    pub path: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
pub struct ScanCommon {
    /// Directories (or files) to scan. Symlinks are never followed.
    #[arg(value_name = "ROOT", required = true)]
    pub roots: Vec<PathBuf>,
    /// Only inventory paths containing this byte substring (matched against the full path).
    #[arg(long, default_value = "")]
    pub pattern: String,
    /// Never choose members under this root as action targets (repeatable).
    #[arg(long, value_name = "DIR")]
    pub protect: Vec<PathBuf>,
    /// Prefer keepers under this root; earlier occurrences win (repeatable).
    #[arg(long, value_name = "DIR")]
    pub prefer: Vec<PathBuf>,
    /// Prefer the most recently modified member as keeper before native path order.
    #[arg(long)]
    pub prefer_newest: bool,
    /// Treat empty files as eligible copies (by default they are reported, never acted on).
    #[arg(long)]
    pub include_empty: bool,
    /// Do not descend into other filesystems.
    #[arg(long)]
    pub one_file_system: bool,
    /// Fingerprint cache file to refresh after the scan.
    #[arg(long, value_name = "FILE")]
    pub cache: Option<PathBuf>,
    /// Reuse validated cache fingerprints instead of re-reading (heuristic; plans refuse it).
    #[arg(long, requires = "cache")]
    pub trust_cache: bool,
    /// Ignore the existing cache contents (the cache is still rewritten).
    #[arg(long, requires = "cache")]
    pub fresh: bool,
    /// Print stage progress to stderr (never to stdout, so machine output stays clean).
    #[arg(long)]
    pub progress: bool,
    #[command(flatten)]
    pub budget: BudgetArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ScanArgs {
    #[command(flatten)]
    pub common: ScanCommon,
    /// Output format.
    #[arg(long, value_enum, default_value = "human")]
    pub format: OutputFormat,
    /// Write the report to this new file instead of stdout (never overwrites).
    #[arg(long, value_name = "FILE")]
    pub output: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct PlanArgs {
    #[command(flatten)]
    pub common: ScanCommon,
    /// New plan file to create (never overwrites).
    #[arg(long, value_name = "FILE")]
    pub output: PathBuf,
    /// Action every eligible copy will receive.
    #[arg(long, value_enum, default_value = "quarantine")]
    pub action: PlanKind,
}

#[derive(Args, Debug, Clone)]
pub struct BudgetArgs {
    /// Working-set memory budget: SQLite page cache plus read buffers plus a fixed allowance.
    #[arg(long, default_value_t = 64, value_name = "MIB")]
    pub memory_mib: u64,
    /// Size of each read buffer.
    #[arg(long, default_value_t = 256, value_name = "KIB")]
    pub buffer_kib: usize,
    /// Concurrent fingerprint readers.
    #[arg(long, default_value_t = 4, value_name = "N")]
    pub readers: usize,
    /// Maximum open file descriptors, enforced as the process RLIMIT_NOFILE soft limit.
    #[arg(long, default_value_t = 32, value_name = "N")]
    pub descriptors: usize,
    /// Maximum scratch database size.
    #[arg(long, default_value_t = 8192, value_name = "MIB")]
    pub temp_mib: u64,
    /// Directory for the private scratch workspace (default: the system temporary directory).
    #[arg(long, value_name = "DIR")]
    pub scratch_dir: Option<PathBuf>,
    /// Queue depth: records per batch handed between stages (16-65536; each is charged
    /// 4 KiB of the memory budget).
    #[arg(long, default_value_t = 256, value_name = "N")]
    pub queue: usize,
}

#[derive(Args, Debug)]
pub struct ApplyArgs {
    /// Plan file created by `rdedupe plan`.
    #[arg(long, value_name = "FILE")]
    pub plan: PathBuf,
    /// New journal file to create; keep it for restore, purge and recover.
    #[arg(long, value_name = "FILE")]
    pub journal: PathBuf,
    /// Skip the interactive confirmation. Never skips validation.
    #[arg(long)]
    pub force: bool,
    /// Read buffer for the byte comparison repeated before every action.
    #[arg(long, default_value_t = 256, value_name = "KIB")]
    pub buffer_kib: usize,
    /// Continue an interrupted apply of the same plan in its existing journal.
    #[arg(long)]
    pub resume: bool,
    /// Where the plan-verification index is created (default: the system temporary directory).
    #[arg(long, value_name = "DIR")]
    pub scratch_dir: Option<PathBuf>,
    /// Maximum size of the plan-verification index.
    #[arg(long, default_value_t = 8192, value_name = "MIB")]
    pub temp_mib: u64,
    /// Result format: one record per action, then a summary.
    #[arg(long, value_enum, default_value = "human")]
    pub format: ResultFormat,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Journal written by `rdedupe apply`.
    #[arg(long, value_name = "FILE")]
    pub journal: PathBuf,
    /// Result format: one record per action, then a summary.
    #[arg(long, value_enum, default_value = "human")]
    pub format: ResultFormat,
}

#[derive(Args, Debug)]
pub struct PurgeArgs {
    /// Journal written by `rdedupe apply`.
    #[arg(long, value_name = "FILE")]
    pub journal: PathBuf,
    /// Skip the interactive confirmation. Keeper verification still runs.
    #[arg(long)]
    pub force: bool,
    /// Read buffer for the keeper verification compare.
    #[arg(long, default_value_t = 256, value_name = "KIB")]
    pub buffer_kib: usize,
    /// Result format: one record per action, then a summary.
    #[arg(long, value_enum, default_value = "human")]
    pub format: ResultFormat,
}

#[derive(Args, Debug)]
pub struct RecoverArgs {
    /// Journal written by `rdedupe apply`.
    #[arg(long, value_name = "FILE")]
    pub journal: PathBuf,
    /// Result format: one record per action.
    #[arg(long, value_enum, default_value = "human")]
    pub format: ResultFormat,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum OutputFormat {
    /// Readable report with escaped paths.
    Human,
    /// One row per group member; lossless hex paths plus an escaped display column.
    Csv,
    /// Versioned machine records: summary, member*, unresolved*, end.
    Jsonl,
    /// NUL-terminated native paths of eligible copies only.
    Nul,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ResultFormat {
    /// Readable lines with escaped paths.
    Human,
    /// Versioned machine records tagged with a `record` field.
    Jsonl,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum PlanKind {
    /// Move eligible copies into a same-filesystem quarantine (restorable).
    Quarantine,
    /// Share storage with the keeper via FIDEDUPERANGE; files stay separate (Linux).
    Extent,
}
