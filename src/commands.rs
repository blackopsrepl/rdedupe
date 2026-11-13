use crate::cli::*;
use crate::config::{Budget, ScanConfig};
use crate::error::{io, Error, Result};
use crate::model::{ActionKind, ActionOutcome, Completeness};
use crate::paths::display_path;
use crate::pipeline::{scan, ScanResult};
use crate::policy::Policy;
use crate::report::Format;
use crate::scheduler::Cancellation;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::{self, IsTerminal, Write};

pub const EXIT_OK: i32 = 0;
pub const EXIT_PARTIAL: i32 = 2;
pub const EXIT_DECLINED: i32 = 3;
pub const EXIT_FAILED: i32 = 4;
/// Invalid invocation (unknown flag, missing argument); the BSD `EX_USAGE` value.
pub const EXIT_USAGE: i32 = 64;
pub const EXIT_CANCELLED: i32 = 130;
pub const EXIT_BROKEN_PIPE: i32 = 141;

pub fn dispatch(cli: Cli, cancel: &Cancellation) -> Result<i32> {
    match cli.command {
        Some(Command::Scan(args)) => scan_command(args, cancel),
        Some(Command::Plan(args)) => plan_command(args, cancel),
        Some(Command::Apply(args)) => apply_command(args, cancel),
        Some(Command::Restore(args)) => restore_command(args),
        Some(Command::Purge(args)) => purge_command(args, cancel),
        Some(Command::Recover(args)) => recover_command(args),
        Some(Command::Capabilities(args)) => capabilities_command(args),
        None => legacy(cli, cancel),
    }
}

fn legacy(cli: Cli, cancel: &Cancellation) -> Result<i32> {
    if cli.delete && !cli.dry_run {
        return Err(Error::Config(
            "--delete no longer unlinks files. Use `rdedupe plan DIR --output FILE` and then \
             `rdedupe apply --plan FILE --journal JOURNAL`, which moves copies into a \
             recoverable quarantine (see `rdedupe restore` and `rdedupe purge`)"
                .into(),
        ));
    }
    let common = ScanCommon {
        roots: cli.legacy_roots,
        pattern: cli.pattern,
        protect: vec![],
        prefer: vec![],
        prefer_newest: false,
        include_empty: false,
        one_file_system: false,
        cache: None,
        trust_cache: false,
        fresh: false,
        progress: false,
        budget: BudgetArgs {
            memory_mib: 64,
            buffer_kib: 256,
            readers: 4,
            descriptors: 32,
            temp_mib: 8192,
            scratch_dir: None,
            queue: 256,
        },
    };
    let result = run_scan(&common, cancel)?;
    if let Some(path) = &cli.csv {
        write_report_file(&result, Format::Csv, path)?;
    }
    crate::report::write(&result, Format::Human, io::stdout().lock())?;
    if cli.dry_run {
        eprintln!("dry run: nothing was changed. To act, use `rdedupe plan` and `rdedupe apply`.");
    }
    Ok(completion_code(&result))
}

fn scan_command(args: ScanArgs, cancel: &Cancellation) -> Result<i32> {
    let result = run_scan(&args.common, cancel)?;
    let format = match args.format {
        OutputFormat::Human => Format::Human,
        OutputFormat::Csv => Format::Csv,
        OutputFormat::Jsonl => Format::Jsonl,
        OutputFormat::Nul => Format::Nul,
    };
    match &args.output {
        Some(path) => write_report_file(&result, format, path)?,
        None => crate::report::write(&result, format, io::stdout().lock())?,
    }
    Ok(completion_code(&result))
}

/// Creates a new report file. `create_new` never follows a final symlink and never truncates
/// an existing entry, so no scanned file can be overwritten through any alias.
fn write_report_file(result: &ScanResult, format: Format, path: &std::path::Path) -> Result<()> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| {
            io(
                "create report (existing files are never overwritten)",
                path,
                e,
            )
        })?;
    let written = crate::report::write(result, format, io::BufWriter::new(&file));
    if written.is_err() {
        let _ = std::fs::remove_file(path);
    }
    written?;
    file.sync_all().map_err(|e| io("sync report", path, e))
}

fn plan_command(args: PlanArgs, cancel: &Cancellation) -> Result<i32> {
    let result = run_scan(&args.common, cancel)?;
    if result.summary.completeness == Completeness::Cancelled {
        return Ok(EXIT_CANCELLED);
    }
    if result.summary.completeness != Completeness::Complete {
        crate::report::write(&result, Format::Human, io::stderr().lock())?;
        return Err(Error::Partial);
    }
    let kind = match args.action {
        PlanKind::Quarantine => ActionKind::Quarantine,
        PlanKind::Extent => ActionKind::Extent,
    };
    let count = crate::plan::write_plan(&result, &args.output, kind)?;
    let metrics = &result.summary.metrics;
    println!(
        "saved {count} {kind:?} actions for {} groups to {} ({} logical bytes)",
        metrics.groups,
        display_path(&args.output),
        metrics.logical_redundant_bytes
    );
    Ok(EXIT_OK)
}

fn confirm(action: &str) -> Result<bool> {
    if !io::stdin().is_terminal() {
        eprintln!(
            "refusing to {action} without confirmation: stdin is not a terminal; pass --force"
        );
        return Ok(false);
    }
    eprint!("Type 'yes' to {action}: ");
    io::stderr().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    Ok(value.trim() == "yes")
}

#[derive(Serialize)]
struct Tagged<'a, T: Serialize> {
    record: &'static str,
    #[serde(flatten)]
    value: &'a T,
}

fn emit<T: Serialize>(
    format: ResultFormat,
    record: &'static str,
    value: &T,
    human: impl FnOnce() -> String,
) -> Result<()> {
    let mut out = io::stdout().lock();
    match format {
        ResultFormat::Jsonl => {
            serde_json::to_writer(&mut out, &Tagged { record, value })?;
            out.write_all(b"\n")?;
        }
        // Human lines embed paths and OS messages: one escaped line each.
        ResultFormat::Human => writeln!(out, "{}", crate::paths::display_text(&human()))?,
    }
    Ok(())
}

fn apply_command(args: ApplyArgs, cancel: &Cancellation) -> Result<i32> {
    let options = crate::delete::ApplyOptions {
        buffer_bytes: args.buffer_kib.saturating_mul(1024),
        index: crate::plan::IndexSpace {
            scratch_dir: args.scratch_dir.clone(),
            temp_bytes: args.temp_mib.saturating_mul(1024 * 1024),
        },
        resume: args.resume,
    };
    let header = crate::plan::verify_plan_in(&args.plan, &options.index)?;
    if !args.force
        && !confirm(&format!(
            "{} this {:?} plan (journal {})",
            if args.resume { "resume" } else { "apply" },
            header.action,
            display_path(&args.journal)
        ))?
    {
        return Ok(EXIT_DECLINED);
    }
    let summary = crate::delete::execute_in(
        &args.plan,
        &args.journal,
        &options,
        cancel,
        &crate::journal::NoHooks,
        |outcome: &ActionOutcome| {
            emit(args.format, "outcome", outcome, || {
                let path = outcome
                    .target
                    .to_path()
                    .map(|p| display_path(&p))
                    .unwrap_or_else(|_| outcome.target.data.clone());
                match &outcome.message {
                    Some(message) => format!("{:?} {path}: {message}", outcome.status),
                    None => format!("{:?} {path}", outcome.status),
                }
            })
        },
    )?;
    emit(args.format, "apply_summary", &summary, || {
        format!(
            "applied {} actions: {} quarantined, {} shared, {} skipped, {} failed; journal {}",
            summary.actions,
            summary.quarantined,
            summary.shared,
            summary.skipped,
            summary.failed,
            display_path(&args.journal)
        )
    })?;
    Ok(if summary.complete() {
        EXIT_OK
    } else {
        EXIT_PARTIAL
    })
}

fn report_finding(format: ResultFormat) -> impl FnMut(&crate::recovery::Finding) {
    move |finding| {
        let _ = emit(format, "finding", finding, || {
            let path = finding
                .target
                .to_path()
                .map(|p| display_path(&p))
                .unwrap_or_default();
            format!(
                "action {} {path}: {:?} → {}",
                finding.action, finding.location, finding.result
            )
        });
    }
}

fn restore_command(args: RestoreArgs) -> Result<i32> {
    let summary = crate::recovery::restore(&args.journal, report_finding(args.format))?;
    emit(args.format, "restore_summary", &summary, || {
        format!(
            "restored {}, already in place {}, missing {}, conflicts {}",
            summary.restored, summary.already_in_place, summary.missing, summary.conflicts
        )
    })?;
    Ok(if summary.clean() {
        EXIT_OK
    } else {
        EXIT_PARTIAL
    })
}

fn purge_command(args: PurgeArgs, cancel: &Cancellation) -> Result<i32> {
    if !args.force && !confirm("permanently purge quarantined copies")? {
        return Ok(EXIT_DECLINED);
    }
    let summary = crate::recovery::purge(
        &args.journal,
        args.buffer_kib.saturating_mul(1024).max(4096),
        cancel,
        report_finding(args.format),
    )?;
    emit(args.format, "purge_summary", &summary, || {
        format!(
            "purged {}, not quarantined {}, absent {}, refused {}",
            summary.purged, summary.not_quarantined, summary.missing, summary.conflicts
        )
    })?;
    Ok(if summary.conflicts == 0 {
        EXIT_OK
    } else {
        EXIT_PARTIAL
    })
}

fn recover_command(args: RecoverArgs) -> Result<i32> {
    let mut indeterminate = 0u64;
    let truncated = crate::recovery::inspect(&args.journal, |item| {
        if item.assessment.starts_with("indeterminate") {
            indeterminate += 1;
        }
        emit(args.format, "inspection", item, || {
            let path = item
                .target
                .to_path()
                .map(|p| display_path(&p))
                .unwrap_or_default();
            format!(
                "action {} group {} {:?} {:?} {path}: {}",
                item.action, item.group, item.kind, item.last_state, item.assessment
            )
        })
    })?;
    if truncated {
        eprintln!("note: the journal ends with a torn record; its action never started");
    }
    Ok(if indeterminate == 0 {
        EXIT_OK
    } else {
        EXIT_PARTIAL
    })
}

fn run_scan(args: &ScanCommon, cancel: &Cancellation) -> Result<ScanResult> {
    crate::platform::limit_open_files(args.budget.descriptors)?;
    let normalize = |items: &Vec<std::path::PathBuf>| {
        items
            .iter()
            .map(|p| std::fs::canonicalize(p).map_err(|e| io("canonicalize policy root", p, e)))
            .collect::<Result<Vec<_>>>()
    };
    scan(
        ScanConfig {
            roots: args.roots.clone(),
            pattern: args.pattern.clone(),
            one_file_system: args.one_file_system,
            budget: Budget {
                memory_bytes: args.budget.memory_mib.saturating_mul(1024 * 1024),
                buffer_bytes: args.budget.buffer_kib.saturating_mul(1024),
                readers: args.budget.readers,
                descriptors: args.budget.descriptors,
                temp_bytes: args.budget.temp_mib.saturating_mul(1024 * 1024),
                scratch_dir: args.budget.scratch_dir.clone(),
                queue_rows: args.budget.queue,
            },
            cache: args.cache.clone(),
            fresh: args.fresh,
            trust_cache: args.trust_cache,
            progress: args.progress,
        },
        Policy {
            protect: normalize(&args.protect)?,
            prefer: normalize(&args.prefer)?,
            prefer_newest: args.prefer_newest,
            include_empty: args.include_empty,
        },
        cancel,
    )
}

pub fn completion_code(result: &ScanResult) -> i32 {
    match result.summary.completeness {
        Completeness::Complete => EXIT_OK,
        Completeness::Partial => EXIT_PARTIAL,
        Completeness::Cancelled => EXIT_CANCELLED,
        Completeness::Failed => EXIT_FAILED,
    }
}

fn capabilities_command(args: crate::cli::CapabilitiesArgs) -> Result<i32> {
    let mut out = io::stdout().lock();
    writeln!(out, "rdedupe {}", env!("CARGO_PKG_VERSION"))?;
    writeln!(out, "platform={}", std::env::consts::OS)?;
    writeln!(out, "plan-schema={}", crate::plan::SCHEMA)?;
    writeln!(out, "journal-schema={}", crate::journal::SCHEMA)?;
    writeln!(out, "machine-schema={}", crate::pipeline::MACHINE_SCHEMA)?;
    writeln!(out, "fingerprint-layout={}", crate::fingerprint::LAYOUT)?;
    writeln!(out, "path-encoding=unix-bytes")?;
    writeln!(out, "quarantine={}", crate::probe::QUARANTINE_PLATFORM)?;
    writeln!(out, "extent={}", crate::probe::EXTENT_PLATFORM)?;
    if let Some(path) = &args.path {
        let probe = crate::probe::filesystem(path)?;
        writeln!(out, "quarantine-probe={}", probe.quarantine)?;
        writeln!(out, "extent-probe={}", probe.extent)?;
    }
    Ok(EXIT_OK)
}
