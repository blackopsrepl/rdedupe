//! Duplicate file deletion operations.

use log::{error, info, warn};
use polars::prelude::*;
use std::error::Error;
use std::fs;
use std::io::{self, Write};

#[derive(Debug)]
pub struct DeleteResult {
    pub deleted_count: usize,
    pub bytes_freed: u64,
    pub dry_run: bool,
    pub kept_files: Vec<String>,
    pub deleted_files: Vec<String>,
}

pub fn confirm_deletion(
    files_to_delete: &[String],
    bytes_to_free: u64,
) -> Result<bool, Box<dyn Error>> {
    warn!(
        "\n⚠️  WARNING: About to DELETE {} files",
        files_to_delete.len()
    );
    warn!(
        "   This will free {} bytes ({:.2} MB)",
        bytes_to_free,
        bytes_to_free as f64 / 1_048_576.0
    );
    warn!("\nFiles to be deleted:");
    for (i, file) in files_to_delete.iter().take(10).enumerate() {
        warn!("  {}. {}", i + 1, file);
    }
    if files_to_delete.len() > 10 {
        warn!("  ... and {} more files", files_to_delete.len() - 10);
    }

    print!("\nType 'yes' to confirm deletion: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().to_lowercase() == "yes")
}

/* Delete duplicate files from a DataFrame, keeping the first file in each group.
 * # Arguments
 * * `df` - DataFrame with duplicate information
 * * `dry_run` - If true, only report what would be deleted
 * * `force` - If true, skip confirmation prompt */
pub fn delete_duplicates(
    df: &DataFrame,
    dry_run: bool,
    force: bool,
) -> Result<DeleteResult, Box<dyn Error>> {
    let duplicates = df
        .clone()
        .lazy()
        .filter(col("is_duplicate").eq(lit(true)))
        .collect()?;

    if duplicates.height() == 0 {
        info!("No duplicate files found to delete.");
        return Ok(DeleteResult {
            deleted_count: 0,
            bytes_freed: 0,
            dry_run,
            kept_files: vec![],
            deleted_files: vec![],
        });
    }

    // Use Polars to determine which file to keep in each group (first alphabetically)
    let with_keep_path = duplicates
        .lazy()
        .with_column(
            col("file_path")
                .sort(false)
                .first()
                .over([col("md5_hash")])
                .alias("keep_path"),
        )
        .collect()?;

    // Extract kept files (unique per group)
    let kept_df = with_keep_path
        .clone()
        .lazy()
        .select([col("keep_path")])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?;

    let kept_files: Vec<String> = kept_df
        .column("keep_path")?
        .utf8()?
        .into_iter()
        .filter_map(|s| s.map(|s| s.to_string()))
        .collect();

    // Extract files to delete (duplicates where file_path != keep_path)
    let to_delete_df = with_keep_path
        .lazy()
        .filter(col("file_path").neq(col("keep_path")))
        .select([col("file_path"), col("size_bytes")])
        .collect()?;

    let mut files_to_delete: Vec<String> = Vec::new();
    let mut bytes_to_free: u64 = 0;

    let paths = to_delete_df.column("file_path")?.utf8()?;
    let sizes = to_delete_df.column("size_bytes")?.u64()?;

    for i in 0..to_delete_df.height() {
        if let (Some(path), Some(size)) = (paths.get(i), sizes.get(i)) {
            files_to_delete.push(path.to_string());
            bytes_to_free += size;
        }
    }

    if files_to_delete.is_empty() {
        info!("No files to delete after analysis.");
        return Ok(DeleteResult {
            deleted_count: 0,
            bytes_freed: 0,
            dry_run,
            kept_files,
            deleted_files: vec![],
        });
    }

    if dry_run {
        info!("\n🔍 DRY RUN - No files will be deleted");
        info!(
            "Would delete {} files, freeing {} bytes ({:.2} MB)",
            files_to_delete.len(),
            bytes_to_free,
            bytes_to_free as f64 / 1_048_576.0
        );
        info!("\nFiles that would be KEPT (one per duplicate group):");
        for file in &kept_files {
            info!("  ✓ {}", file);
        }
        info!("\nFiles that would be DELETED:");
        for file in &files_to_delete {
            info!("  ✗ {}", file);
        }

        return Ok(DeleteResult {
            deleted_count: files_to_delete.len(),
            bytes_freed: bytes_to_free,
            dry_run: true,
            kept_files,
            deleted_files: files_to_delete,
        });
    }

    if !force && !confirm_deletion(&files_to_delete, bytes_to_free)? {
        info!("Deletion cancelled by user.");
        return Ok(DeleteResult {
            deleted_count: 0,
            bytes_freed: 0,
            dry_run: false,
            kept_files: vec![],
            deleted_files: vec![],
        });
    }

    info!("\n🗑️  Deleting {} files...", files_to_delete.len());
    let mut actually_deleted = Vec::new();
    let mut actual_bytes_freed: u64 = 0;
    let mut errors = 0;

    for file_path in &files_to_delete {
        match fs::metadata(file_path) {
            Ok(metadata) => {
                let size = metadata.len();
                match fs::remove_file(file_path) {
                    Ok(()) => {
                        actually_deleted.push(file_path.clone());
                        actual_bytes_freed += size;
                        info!("  ✓ Deleted: {}", file_path);
                    }
                    Err(e) => {
                        errors += 1;
                        error!("  ✗ Failed to delete {}: {}", file_path, e);
                    }
                }
            }
            Err(e) => {
                errors += 1;
                error!("  ✗ Cannot access {}: {}", file_path, e);
            }
        }
    }

    info!("\n=== Deletion Summary ===");
    info!("Files deleted: {}", actually_deleted.len());
    info!(
        "Bytes freed: {} ({:.2} MB)",
        actual_bytes_freed,
        actual_bytes_freed as f64 / 1_048_576.0
    );
    if errors > 0 {
        warn!("Errors: {}", errors);
    }

    Ok(DeleteResult {
        deleted_count: actually_deleted.len(),
        bytes_freed: actual_bytes_freed,
        dry_run: false,
        kept_files,
        deleted_files: actually_deleted,
    })
}
