use crate::config::WALK_OPEN_DIRECTORIES;
use crate::error::{io, Result};
use crate::fileinfo::observe_path;
use crate::model::Unresolved;
use crate::paths::{os_contains, EncodedPath};
use crate::store::Store;
use std::collections::HashSet;
use std::path::PathBuf;
use walkdir::WalkDir;

pub struct DiscoveryOptions<'a> {
    /// Canonical roots.
    pub roots: &'a [PathBuf],
    /// Byte substring every inventoried full path must contain (empty matches everything).
    pub pattern: &'a str,
    pub one_file_system: bool,
    /// Exact owned artifact paths (scratch workspace, cache file) never inventoried.
    pub excluded: &'a HashSet<PathBuf>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DiscoveryCounts {
    pub symlinks_skipped: u64,
    pub special_files_skipped: u64,
    /// Marker-owned quarantine directories (and the owned artifacts above) not descended.
    pub owned_dirs_skipped: u64,
}

/// Walks every root without following symlinks, in sorted native order, and records each
/// matching regular file. Traversal and metadata failures become unresolved entries. Paths
/// reached through several roots are inventoried once; hard-link aliases are resolved
/// afterwards by [`Store::finalize_aliases`].
pub fn discover(store: &Store, options: &DiscoveryOptions<'_>) -> Result<DiscoveryCounts> {
    let mut counts = DiscoveryCounts::default();
    let owned_skipped = std::cell::Cell::new(0u64);
    store.begin()?;
    for root in options.roots {
        let root_device = observe_path(root).ok().map(|o| o.identity.device);
        let walker = WalkDir::new(root)
            .follow_links(false)
            .same_file_system(options.one_file_system)
            .max_open(WALK_OPEN_DIRECTORIES)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|entry| {
                let owned = options.excluded.iter().any(|owned| entry.path() == owned)
                    || (entry.file_type().is_dir()
                        && crate::quarantine::is_owned_quarantine_dir(entry.path()));
                if owned && entry.file_type().is_dir() {
                    owned_skipped.set(owned_skipped.get() + 1);
                }
                !owned
            });
        for entry in walker {
            let entry = match entry {
                Ok(value) => value,
                Err(error) => {
                    let path = error.path().unwrap_or(root);
                    store.insert_unresolved(&Unresolved {
                        path: EncodedPath::from_path(path),
                        operation: "traverse".into(),
                        message: error.to_string(),
                        retry_safe: true,
                    })?;
                    continue;
                }
            };
            let file_type = entry.file_type();
            if file_type.is_dir() {
                continue;
            }
            let path = entry.path();
            if !os_contains(path, options.pattern) {
                continue;
            }
            if file_type.is_symlink() {
                counts.symlinks_skipped += 1;
                continue;
            }
            if !file_type.is_file() {
                counts.special_files_skipped += 1;
                continue;
            }
            match observe_path(path) {
                Ok(obs) => {
                    if options.one_file_system && root_device != Some(obs.identity.device) {
                        continue;
                    }
                    store.insert_file(path, &obs)?;
                }
                Err(error) => store.insert_unresolved(&Unresolved {
                    path: EncodedPath::from_path(path),
                    operation: "metadata".into(),
                    message: error.to_string(),
                    retry_safe: true,
                })?,
            }
        }
    }
    store.finalize_aliases()?;
    counts.owned_dirs_skipped = owned_skipped.get();
    Ok(counts)
}

pub fn canonical_roots(roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::with_capacity(roots.len());
    for root in roots {
        let canonical =
            std::fs::canonicalize(root).map_err(|e| io("canonicalize root", root, e))?;
        if !out.contains(&canonical) {
            out.push(canonical);
        }
    }
    out.sort_by(|a, b| crate::paths::stable_cmp(a, b));
    Ok(out)
}
