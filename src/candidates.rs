//! Candidate elimination and exact grouping.
//!
//! 1. Independent objects whose size is unique are never read.
//! 2. Remaining objects are fingerprinted (bounded parallel readers); fingerprints only
//!    separate candidates.
//! 3. Within every fingerprint bucket, each object is compared byte for byte against one
//!    representative of every equality class found so far, so several distinct contents
//!    sharing one fingerprint (a forced collision) still form separate, exact groups.

use crate::compare;
use crate::config::Budget;
use crate::error::{Error, Result};
use crate::fingerprint::Fingerprinter;
use crate::model::{FileRecord, Metrics, Unresolved};
use crate::paths::{display_path, path_bytes, EncodedPath};
use crate::scheduler::{bounded_map, Cancellation, Progress};
use crate::store::Store;

pub struct Options<'a> {
    pub budget: &'a Budget,
    pub fingerprinter: &'a dyn Fingerprinter,
    /// Reuse validated cached fingerprints (heuristic scan).
    pub trust_cache: bool,
    pub cancel: &'a Cancellation,
    pub progress: Progress,
}

fn unresolved(store: &Store, record: &FileRecord, operation: &str, message: String) -> Result<()> {
    store.insert_unresolved(&Unresolved {
        path: EncodedPath::from_path(&record.path),
        operation: operation.into(),
        message,
        retry_safe: true,
    })
}

pub fn establish_groups(store: &Store, options: &Options<'_>, metrics: &mut Metrics) -> Result<()> {
    store.begin()?;
    fingerprint_candidates(store, options, metrics)?;
    compare_buckets(store, options, metrics)?;
    store.commit()?;
    store.renumber_groups()
}

fn fingerprint_candidates(
    store: &Store,
    options: &Options<'_>,
    metrics: &mut Metrics,
) -> Result<()> {
    let budget = options.budget;
    let mut last_size = None;
    let mut done = 0u64;
    loop {
        let sizes = store.candidate_sizes_after(last_size)?;
        let Some(&tail) = sizes.last() else {
            return Ok(());
        };
        for size in sizes {
            let mut last_path: Option<Vec<u8>> = None;
            loop {
                options.cancel.check()?;
                let page = store.candidates_after(size, last_path.as_deref())?;
                let Some(last) = page.last() else {
                    break;
                };
                last_path = Some(path_bytes(&last.path).to_vec());
                let mut todo = Vec::with_capacity(page.len());
                for record in page {
                    if options.trust_cache {
                        if let Some(digest) = store.cached_digest(&record)? {
                            store.put_fingerprint(&record, &digest)?;
                            metrics.cached_fingerprints_reused += 1;
                            continue;
                        }
                    }
                    todo.push(record);
                }
                let results = bounded_map(
                    &todo,
                    budget.readers,
                    budget.buffer_bytes,
                    |record, buffer| {
                        options.fingerprinter.fingerprint(
                            &record.path,
                            &record.observation,
                            buffer,
                            options.cancel,
                        )
                    },
                );
                for (record, result) in todo.iter().zip(results) {
                    done += 1;
                    options.progress.tick("fingerprinted", done);
                    match result {
                        Ok((digest, bytes)) => {
                            metrics.fingerprint_bytes_read += bytes;
                            store.put_fingerprint(record, &digest)?;
                        }
                        Err(Error::Cancelled) => return Err(Error::Cancelled),
                        Err(error) => unresolved(store, record, "fingerprint", error.to_string())?,
                    }
                }
            }
        }
        last_size = Some(tail);
    }
}

/// Phase of a candidate in the fingerprints table.
const SAMPLED: i64 = 0;
const REFINED: i64 = 2;

/// A sampled-digest bucket that shows more than this many equality classes is split by
/// full-content digest instead of comparing every member with every class.
const CLASS_LIMIT: u64 = 2;

/// Comparison runs one representative against up to this many members at once. Fingerprint
/// readers are idle during comparison, so their descriptors and buffers are reused.
fn batch_size(options: &Options<'_>) -> usize {
    options.budget.readers + 1
}

fn compare_buckets(store: &Store, options: &Options<'_>, metrics: &mut Metrics) -> Result<()> {
    compare_phase(store, options, metrics, SAMPLED)?;
    refine_flagged(store, options, metrics)?;
    compare_phase(store, options, metrics, REFINED)?;
    metrics.eliminated_by_fingerprint = store.singleton_buckets()?;
    Ok(())
}

/// Hashes the complete content of every candidate flagged by [`compare_phase`], so N files
/// that agree on every sampled window but differ elsewhere cost N reads, not one
/// comparison per equality class.
fn refine_flagged(store: &Store, options: &Options<'_>, metrics: &mut Metrics) -> Result<()> {
    let budget = options.budget;
    let mut last_path: Option<Vec<u8>> = None;
    loop {
        options.cancel.check()?;
        let page = store.flagged_after(last_path.as_deref())?;
        let Some(last) = page.last() else {
            return Ok(());
        };
        last_path = Some(path_bytes(&last.path).to_vec());
        let results = bounded_map(
            &page,
            budget.readers,
            budget.buffer_bytes,
            |record, buffer| {
                options.fingerprinter.refine(
                    &record.path,
                    &record.observation,
                    buffer,
                    options.cancel,
                )
            },
        );
        for (record, result) in page.iter().zip(results) {
            match result {
                Ok((refined, bytes)) => {
                    metrics.full_hash_bytes_read += bytes;
                    store.set_refined(record, &refined)?;
                }
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(error) => {
                    store.remove_fingerprint(record)?;
                    unresolved(store, record, "fingerprint", error.to_string())?;
                }
            }
        }
    }
}

/// Outcome of classifying one bucket.
enum Bucket {
    Classified,
    /// More than [`CLASS_LIMIT`] classes in a sampled bucket whose samples leave bytes
    /// unread: refine it instead.
    TooManyClasses,
}

fn compare_phase(
    store: &Store,
    options: &Options<'_>,
    metrics: &mut Metrics,
    phase: i64,
) -> Result<()> {
    let mut last_digest: Option<Vec<u8>> = None;
    let mut buckets = 0u64;
    loop {
        let digests = store.colliding_digests_after(phase, last_digest.as_deref())?;
        let Some(tail) = digests.last().cloned() else {
            return Ok(());
        };
        for digest in digests {
            store.reset_comparison_classes()?;
            match classify_bucket(store, options, metrics, phase, &digest)? {
                Bucket::Classified => {
                    let (_, eliminated) = store.materialize_comparison_groups()?;
                    metrics.eliminated_by_comparison += eliminated;
                }
                Bucket::TooManyClasses => store.flag_for_refinement(&digest)?,
            }
            buckets += 1;
            options.progress.tick("compared buckets", buckets);
        }
        last_digest = Some(tail);
    }
}

fn classify_bucket(
    store: &Store,
    options: &Options<'_>,
    metrics: &mut Metrics,
    phase: i64,
    digest: &[u8],
) -> Result<Bucket> {
    let mut classes = 0u64;
    let mut last_path: Option<Vec<u8>> = None;
    loop {
        let page = store.bucket_after(phase, digest, last_path.as_deref())?;
        let Some(last) = page.last() else {
            return Ok(Bucket::Classified);
        };
        last_path = Some(path_bytes(&last.path).to_vec());
        // Samples cover every byte up to two windows; beyond that, many classes in one
        // sampled bucket mean the samples are blind to the differences.
        let limited = phase == SAMPLED && last.observation.size > 2 * crate::fingerprint::WINDOW;
        for batch in page.chunks(batch_size(options)) {
            let mut pending: Vec<&FileRecord> = batch.iter().collect();
            join_existing_classes(store, options, metrics, &mut pending)?;
            while let Some((&first, rest)) = pending.split_first() {
                let class_id = store.add_comparison_class(first)?;
                classes += 1;
                if limited && classes > CLASS_LIMIT {
                    return Ok(Bucket::TooManyClasses);
                }
                let mut rest: Vec<&FileRecord> = rest.to_vec();
                compare_against(store, options, metrics, first, class_id, &mut rest)?;
                pending = rest;
            }
        }
    }
}

/// Compares `pending` with every existing class representative; members that match join
/// that class and leave `pending`.
fn join_existing_classes(
    store: &Store,
    options: &Options<'_>,
    metrics: &mut Metrics,
    pending: &mut Vec<&FileRecord>,
) -> Result<()> {
    let mut last_class = 0;
    while !pending.is_empty() {
        let representatives = store.representatives_after(last_class)?;
        let Some(&(tail, _)) = representatives.last() else {
            return Ok(());
        };
        for (class_id, representative) in representatives {
            if pending.is_empty() {
                return Ok(());
            }
            compare_against(store, options, metrics, &representative, class_id, pending)?;
        }
        last_class = tail;
    }
    Ok(())
}

/// Compares `pending` with one representative in a single pass. Equal members join the
/// representative's class; members that cannot be compared become unresolved; different
/// members stay in `pending`.
fn compare_against(
    store: &Store,
    options: &Options<'_>,
    metrics: &mut Metrics,
    representative: &FileRecord,
    class_id: i64,
    pending: &mut Vec<&FileRecord>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    options.cancel.check()?;
    let members: Vec<_> = pending
        .iter()
        .map(|r| (r.path.as_path(), &r.observation))
        .collect();
    let outcome = compare::equal_many(
        (&representative.path, &representative.observation),
        &members,
        options.budget.buffer_bytes,
        options.cancel,
    );
    let verdicts = match outcome {
        Ok((verdicts, bytes)) => {
            metrics.comparison_bytes_read += bytes;
            verdicts
        }
        Err(Error::Cancelled) => return Err(Error::Cancelled),
        Err(error) => {
            let message = format!(
                "comparison with {} failed: {error}",
                display_path(&representative.path)
            );
            for record in pending.drain(..) {
                store.remove_fingerprint(record)?;
                unresolved(store, record, "compare", message.clone())?;
            }
            return Ok(());
        }
    };
    let mut different = Vec::with_capacity(pending.len());
    for (record, verdict) in pending.drain(..).zip(verdicts) {
        match verdict {
            compare::Verdict::Equal => store.add_comparison_member(class_id, record)?,
            compare::Verdict::Different => different.push(record),
            compare::Verdict::Failed(error) => {
                store.remove_fingerprint(record)?;
                let message = format!(
                    "comparison with {} failed: {error}",
                    display_path(&representative.path)
                );
                unresolved(store, record, "compare", message)?;
            }
        }
    }
    *pending = different;
    Ok(())
}
