use crate::accounting;
use crate::cache;
use crate::candidates;
use crate::config::ScanConfig;
use crate::error::{Error, Result};
use crate::fingerprint::{Fingerprinter, SampleFingerprinter};
use crate::fs::{canonical_roots, discover, DiscoveryOptions};
use crate::model::{Completeness, Exactness, ScanSummary};
use crate::policy::Policy;
use crate::scheduler::{Cancellation, Progress};
use crate::store::Store;
use std::collections::HashSet;

pub const MACHINE_SCHEMA: &str = "rdedupe-machine-v1";

pub struct ScanResult {
    pub store: Store,
    pub summary: ScanSummary,
    pub config: ScanConfig,
    pub policy: Policy,
}

/// Resolves an owned artifact path the way discovery will see it: the parent directory is
/// canonicalized (symlinks resolved) and the final name kept, even if it does not exist yet.
pub fn resolve_owned(path: &std::path::Path) -> Result<std::path::PathBuf> {
    let absolute =
        std::path::absolute(path).map_err(|e| crate::error::io("resolve path", path, e))?;
    match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => match std::fs::canonicalize(parent) {
            Ok(parent) => Ok(parent.join(name)),
            Err(_) => Ok(absolute),
        },
        _ => Ok(absolute),
    }
}

pub fn scan(config: ScanConfig, policy: Policy, cancel: &Cancellation) -> Result<ScanResult> {
    scan_with(config, policy, cancel, &SampleFingerprinter)
}

/// Runs discovery → size elimination → fingerprints → exact comparison → accounting.
///
/// Cancellation is not an error here: the scan stops at the next check point and returns a
/// summary marked `cancelled`, so callers can still report what was established.
pub fn scan_with(
    mut config: ScanConfig,
    policy: Policy,
    cancel: &Cancellation,
    fingerprinter: &dyn Fingerprinter,
) -> Result<ScanResult> {
    config.budget.validate()?;
    config.roots = canonical_roots(&config.roots)?;
    let cache_slot = config.cache.as_deref().map(cache::claim).transpose()?;
    let store = Store::create(&config.budget)?;
    let mut excluded = HashSet::new();
    excluded.insert(resolve_owned(store.workspace())?);
    if let Some(cache_path) = &config.cache {
        excluded.insert(resolve_owned(cache_path)?);
    }
    let progress = Progress {
        enabled: config.progress,
    };
    let mut loaded = None;
    let mut metrics = crate::model::Metrics::default();
    // Allocated once: `readers` fingerprint buffers plus one, lent to every stage.
    let buffers =
        crate::scheduler::BufferPool::new(config.budget.readers + 1, config.budget.buffer_bytes);
    // Every stage checks `cancel`; an interrupt at any point yields a summary marked
    // cancelled with whatever was established so far.
    let outcome = (|| -> Result<()> {
        let counts = discover(
            &store,
            &DiscoveryOptions {
                roots: &config.roots,
                pattern: &config.pattern,
                one_file_system: config.one_file_system,
                excluded: &excluded,
                cancel,
            },
        )?;
        loaded = match (&config.cache, config.trust_cache && !config.fresh) {
            (Some(path), true) => Some(cache::load(path, &store, cancel)?),
            _ => None,
        };
        metrics = store.base_metrics(config.budget.page_cache_bytes())?;
        metrics.symlinks_skipped = counts.symlinks_skipped;
        metrics.special_files_skipped = counts.special_files_skipped;
        metrics.owned_dirs_skipped = counts.owned_dirs_skipped;
        metrics.abandoned_scratch_removed = store.abandoned_scratch_removed();
        progress.report(format_args!(
            "discovered {} paths, {} objects; {} candidates share a size",
            metrics.paths, metrics.objects, metrics.candidate_objects
        ));
        candidates::establish_groups(
            &store,
            &candidates::Options {
                budget: &config.budget,
                fingerprinter,
                trust_cache: matches!(loaded, Some(cache::Loaded::Entries(_))),
                cancel,
                progress,
                buffers: &buffers,
            },
            &mut metrics,
        )?;
        accounting::finish(&store, &policy, &mut metrics, cancel)
    })();
    match outcome {
        Ok(()) => {}
        Err(Error::Cancelled) => {
            store.commit()?;
            if metrics.paths == 0 {
                metrics = store.base_metrics(config.budget.page_cache_bytes())?;
            }
            store.set_completeness(Completeness::Cancelled)?;
        }
        Err(error) => return Err(error),
    }
    let exactness = if metrics.cached_fingerprints_reused > 0 {
        Exactness::Heuristic
    } else {
        Exactness::Exact
    };
    let mut cache_provenance = match (&config.cache, &loaded) {
        (None, _) => "no cache".to_string(),
        (Some(_), None) if config.trust_cache => {
            "fresh: existing cache ignored by --fresh".to_string()
        }
        (Some(_), None) => "exact: cache not consulted".to_string(),
        (Some(_), Some(cache::Loaded::Missing)) => "trusted: no cache file yet".to_string(),
        (Some(_), Some(cache::Loaded::Ignored(why))) => format!("trusted: cache ignored ({why})"),
        (Some(_), Some(cache::Loaded::Entries(n))) => format!(
            "heuristic: {n} cache entries loaded, {} fingerprints reused",
            metrics.cached_fingerprints_reused
        ),
    };
    let completeness = store.completeness()?;
    if let (Some(path), Some(slot), Completeness::Complete | Completeness::Partial) =
        (&config.cache, cache_slot, &completeness)
    {
        let written = cache::write(path, &store, slot)?;
        cache_provenance.push_str(&format!("; refreshed with {written} fingerprints"));
    }
    let unresolved = store.unresolved_count()?;
    progress.report(format_args!(
        "{:?}: {} groups, {} unresolved, {} bytes read",
        completeness,
        metrics.groups,
        unresolved,
        metrics.fingerprint_bytes_read
            + metrics.full_hash_bytes_read
            + metrics.comparison_bytes_read
    ));
    Ok(ScanResult {
        store,
        summary: ScanSummary {
            schema: MACHINE_SCHEMA.into(),
            record: "summary".into(),
            binary_version: env!("CARGO_PKG_VERSION").into(),
            completeness,
            exactness,
            metrics,
            unresolved,
            cache_provenance,
        },
        config,
        policy,
    })
}
