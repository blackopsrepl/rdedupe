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
    refine_buckets(store, options, metrics)?;
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

/// Splits every sampled-digest bucket with more than two members (of files larger than two
/// sample windows, whose samples leave bytes unread) by full-content digest,
/// so N files that collide on their samples cost N extra reads rather than up to N²
/// comparisons. Buckets of two go straight to one byte comparison, which is cheaper.
fn refine_buckets(store: &Store, options: &Options<'_>, metrics: &mut Metrics) -> Result<()> {
    let budget = options.budget;
    let mut last_digest: Option<Vec<u8>> = None;
    loop {
        // Up to two windows' size the samples cover every byte, so the sampled digest
        // already separates contents and refining would only re-read the same bytes.
        let digests = store.sampled_buckets_after(
            2,
            2 * crate::fingerprint::WINDOW,
            last_digest.as_deref(),
        )?;
        let Some(tail) = digests.last().cloned() else {
            return Ok(());
        };
        for digest in digests {
            let mut last_path: Option<Vec<u8>> = None;
            loop {
                options.cancel.check()?;
                let page = store.sampled_bucket_after(&digest, last_path.as_deref())?;
                let Some(last) = page.last() else {
                    break;
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
        last_digest = Some(tail);
    }
}

fn compare_buckets(store: &Store, options: &Options<'_>, metrics: &mut Metrics) -> Result<()> {
    let mut last_digest: Option<Vec<u8>> = None;
    let mut buckets = 0u64;
    loop {
        let digests = store.colliding_digests_after(last_digest.as_deref())?;
        let Some(tail) = digests.last().cloned() else {
            return Ok(());
        };
        for digest in digests {
            store.reset_comparison_classes()?;
            let mut last_path: Option<Vec<u8>> = None;
            loop {
                let page = store.bucket_after(&digest, last_path.as_deref())?;
                let Some(last) = page.last() else {
                    break;
                };
                last_path = Some(path_bytes(&last.path).to_vec());
                for record in &page {
                    classify(store, options, metrics, record)?;
                }
            }
            let (_, eliminated) = store.materialize_comparison_groups()?;
            metrics.eliminated_by_fingerprint += eliminated;
            buckets += 1;
            options.progress.tick("compared buckets", buckets);
        }
        last_digest = Some(tail);
    }
}

/// Adds `record` to the first equality class whose representative is byte-identical, or
/// starts a new class. A failed comparison makes the record unresolved; it never joins a
/// class on partial evidence.
fn classify(
    store: &Store,
    options: &Options<'_>,
    metrics: &mut Metrics,
    record: &FileRecord,
) -> Result<()> {
    let mut last_class = 0;
    loop {
        let representatives = store.representatives_after(last_class)?;
        let Some(&(tail, _)) = representatives.last() else {
            break;
        };
        for (class_id, representative) in representatives {
            options.cancel.check()?;
            match compare::equal_expected(
                (&representative.path, &representative.observation),
                (&record.path, &record.observation),
                options.budget.buffer_bytes,
                options.cancel,
            ) {
                Ok((same, bytes)) => {
                    metrics.comparison_bytes_read += bytes;
                    if same {
                        return store.add_comparison_member(class_id, record);
                    }
                }
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(error) => {
                    let message = format!(
                        "comparison with {} failed: {error}",
                        display_path(&representative.path)
                    );
                    return unresolved(store, record, "compare", message);
                }
            }
        }
        last_class = tail;
    }
    store.add_comparison_class(record).map(|_| ())
}
