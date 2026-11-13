//! Streaming result rendering. All formats are produced from the same verified groups and
//! the same keeper policy, page by page; nothing is collected in memory first.

use crate::error::{Error, Result};
use crate::model::{FileRecord, GroupMember};
use crate::paths::{display_path, display_text, path_bytes, EncodedPath};
use crate::pipeline::{ScanResult, MACHINE_SCHEMA};
use serde::Serialize;
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Human,
    Csv,
    Jsonl,
    Nul,
}

pub fn write(scan: &ScanResult, format: Format, mut output: impl Write) -> Result<()> {
    match format {
        Format::Human => write_human(scan, &mut output),
        Format::Csv => write_csv(scan, &mut output),
        Format::Jsonl => write_jsonl(scan, &mut output),
        Format::Nul => write_nul(scan, &mut output),
    }?;
    output.flush().map_err(Error::Output)
}

struct Member<'a> {
    group: i64,
    record: &'a FileRecord,
    /// For a hard-link alias: the member path it is another name of. Aliases follow their
    /// member; they are never keepers or eligible copies (removing one name frees nothing).
    alias_of: Option<&'a std::path::Path>,
    keeper: bool,
    protected: bool,
    reason: String,
    metadata_differs: Vec<&'static str>,
}

fn each_member(scan: &ScanResult, mut f: impl FnMut(&Member<'_>) -> Result<()>) -> Result<()> {
    scan.store.for_each_group(|group| {
        let (keeper, keeper_reason) = scan.policy.select_streaming(&scan.store, group)?;
        let keeper_xattrs = crate::platform::xattr_digest(&keeper.path);
        scan.store.for_each_member(group, |record| {
            let is_keeper = record.id == keeper.id;
            let protected = scan.policy.is_protected(&record.path);
            let metadata_differs = if is_keeper {
                Vec::new()
            } else {
                crate::policy::metadata_differences(record, &keeper, keeper_xattrs)
            };
            let mut reason = if is_keeper {
                format!("keeper: {keeper_reason}")
            } else if let Some(why) = scan.policy.retained_because(record) {
                format!("retained: {why}")
            } else {
                format!("eligible copy of {}", display_path(&keeper.path))
            };
            if !metadata_differs.is_empty() {
                reason.push_str(&format!(
                    "; differs from the keeper in {}",
                    metadata_differs.join(", ")
                ));
            }
            f(&Member {
                group,
                record,
                alias_of: None,
                keeper: is_keeper,
                protected,
                reason,
                metadata_differs,
            })?;
            scan.store.for_each_alias(record, |alias| {
                f(&Member {
                    group,
                    record: alias,
                    alias_of: Some(&record.path),
                    keeper: false,
                    protected: scan.policy.is_protected(&alias.path),
                    reason: format!(
                        "hard-link alias of {}: the same file under another name; never acted on separately",
                        display_path(&record.path)
                    ),
                    metadata_differs: Vec::new(),
                })
            })
        })
    })
}

fn write_human(scan: &ScanResult, out: &mut impl Write) -> Result<()> {
    let summary = &scan.summary;
    writeln!(
        out,
        "rdedupe {} scan: {:?}, {:?}",
        summary.binary_version, summary.completeness, summary.exactness
    )?;
    let mut current = 0;
    each_member(scan, |m| {
        if m.group != current {
            current = m.group;
            writeln!(
                out,
                "\ngroup {} ({} bytes each):",
                m.group, m.record.observation.size
            )?;
        }
        let tag = if m.alias_of.is_some() {
            "LINK"
        } else if m.keeper {
            "KEEP"
        } else if m.protected {
            "PROT"
        } else {
            "COPY"
        };
        writeln!(
            out,
            "  {tag} {} ({})",
            display_path(&m.record.path),
            display_text(&m.reason)
        )?;
        Ok(())
    })?;
    let metrics = &summary.metrics;
    writeln!(
        out,
        "\nsummary: {} paths, {} objects, {} hard-link aliases, {} groups, {} eligible copies",
        metrics.paths, metrics.objects, metrics.aliases, metrics.groups, metrics.eligible_copies
    )?;
    writeln!(
        out,
        "space: {} logical redundant bytes; {} allocated bytes in single-link copies (an estimate, not a promise of reclaimed space)",
        metrics.logical_redundant_bytes, metrics.estimated_allocated_bytes
    )?;
    writeln!(
        out,
        "reads: {} fingerprint bytes, {} full-content bytes, {} comparison bytes; {} objects eliminated by size without reading",
        metrics.fingerprint_bytes_read,
        metrics.full_hash_bytes_read,
        metrics.comparison_bytes_read,
        metrics.eliminated_by_size
    )?;
    if summary.unresolved > 0 {
        writeln!(out, "\n{} unresolved paths:", summary.unresolved)?;
        scan.store.for_each_unresolved(|item| {
            writeln!(
                out,
                "  UNRESOLVED {} [{}]: {}",
                display_path(&item.path.to_path()?),
                item.operation,
                display_text(&item.message)
            )?;
            Ok(())
        })?;
    }
    if !matches!(summary.completeness, crate::model::Completeness::Complete) {
        writeln!(
            out,
            "\nscan is {:?}: groups shown are exact, but files may be missing; plans are refused",
            summary.completeness
        )?;
    }
    Ok(())
}

fn write_csv(scan: &ScanResult, out: &mut impl Write) -> Result<()> {
    let mut csv = csv::Writer::from_writer(out);
    csv.write_record([
        "group",
        "path_encoding",
        "path_hex",
        "path_display",
        "size",
        "allocated",
        "device",
        "inode",
        "links",
        "keeper",
        "protected",
        "reason",
        "alias_of_hex",
    ])?;
    each_member(scan, |m| {
        let path = EncodedPath::from_path(&m.record.path);
        let o = &m.record.observation;
        csv.serialize((
            m.group,
            &path.encoding,
            &path.data,
            display_path(&m.record.path),
            o.size,
            o.allocated,
            o.identity.device,
            o.identity.inode,
            o.links,
            m.keeper,
            m.protected,
            &m.reason,
            m.alias_of
                .map(|p| EncodedPath::from_path(p).data)
                .unwrap_or_default(),
        ))?;
        Ok(())
    })?;
    csv.flush().map_err(Error::Output)
}

#[derive(Serialize)]
struct UnresolvedRecord<'a> {
    record: &'static str,
    #[serde(flatten)]
    item: &'a crate::model::Unresolved,
}

#[derive(Serialize)]
struct AliasRecord {
    record: &'static str,
    group: i64,
    path: EncodedPath,
    alias_of: EncodedPath,
    protected: bool,
}

#[derive(Serialize)]
struct EndRecord {
    record: &'static str,
    schema: &'static str,
    groups: u64,
    unresolved: u64,
}

fn write_jsonl(scan: &ScanResult, out: &mut impl Write) -> Result<()> {
    serde_json::to_writer(&mut *out, &scan.summary)?;
    out.write_all(b"\n")?;
    each_member(scan, |m| {
        if let Some(of) = m.alias_of {
            serde_json::to_writer(
                &mut *out,
                &AliasRecord {
                    record: "alias",
                    group: m.group,
                    path: EncodedPath::from_path(&m.record.path),
                    alias_of: EncodedPath::from_path(of),
                    protected: m.protected,
                },
            )?;
            out.write_all(b"\n")?;
            return Ok(());
        }
        let o = &m.record.observation;
        serde_json::to_writer(
            &mut *out,
            &GroupMember {
                record: "member".into(),
                group: m.group,
                path: EncodedPath::from_path(&m.record.path),
                identity: o.identity,
                size: o.size,
                allocated: o.allocated,
                links: o.links,
                is_keeper: m.keeper,
                protected: m.protected,
                reason: m.reason.clone(),
                metadata_differs_from_keeper: m
                    .metadata_differs
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
        )?;
        out.write_all(b"\n")?;
        Ok(())
    })?;
    scan.store.for_each_unresolved(|item| {
        serde_json::to_writer(
            &mut *out,
            &UnresolvedRecord {
                record: "unresolved",
                item,
            },
        )?;
        out.write_all(b"\n")?;
        Ok(())
    })?;
    serde_json::to_writer(
        &mut *out,
        &EndRecord {
            record: "end",
            schema: MACHINE_SCHEMA,
            groups: scan.summary.metrics.groups,
            unresolved: scan.summary.unresolved,
        },
    )?;
    out.write_all(b"\n")?;
    Ok(())
}

/// NUL-terminated native paths of every eligible copy (never keepers, members the policy
/// retains, or hard-link aliases), exactly the targets a plan would contain, suitable for
/// `xargs -0`.
/// Only meaningful for complete exact scans.
fn write_nul(scan: &ScanResult, out: &mut impl Write) -> Result<()> {
    each_member(scan, |m| {
        if m.alias_of.is_none() && !m.keeper && scan.policy.retained_because(m.record).is_none() {
            out.write_all(path_bytes(&m.record.path))?;
            out.write_all(&[0])?;
        }
        Ok(())
    })
}
