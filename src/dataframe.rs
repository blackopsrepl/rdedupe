//! DataFrame creation, statistics, validation, and CSV export.

use crate::fileinfo::FileInfo;
use polars::prelude::*;
use std::error::Error;

pub fn create_dataframe(file_infos: Vec<FileInfo>) -> Result<DataFrame, Box<dyn Error>> {
    let paths: Vec<String> = file_infos.iter().map(|f| f.path.clone()).collect();
    let names: Vec<String> = file_infos.iter().map(|f| f.name.clone()).collect();
    let extensions: Vec<String> = file_infos.iter().map(|f| f.extension.clone()).collect();
    let sizes_bytes: Vec<u64> = file_infos.iter().map(|f| f.size_bytes).collect();
    let sizes_mb: Vec<f64> = file_infos.iter().map(|f| f.size_mb).collect();
    let hashes: Vec<String> = file_infos.iter().map(|f| f.md5_hash.clone()).collect();

    let base_df = df! [
        "file_path" => paths,
        "file_name" => names,
        "extension" => extensions,
        "size_bytes" => sizes_bytes,
        "size_mb" => sizes_mb,
        "md5_hash" => hashes,
    ]?;

    // Use Polars window function to identify duplicates:
    // Count files per hash group, mark as duplicate if count > 1
    let df = base_df
        .lazy()
        .with_column(
            col("md5_hash")
                .count()
                .over([col("md5_hash")])
                .alias("group_count"),
        )
        .with_column(col("group_count").gt(lit(1)).alias("is_duplicate"))
        .collect()?;

    // Print duplicate summary
    let duplicate_count = df
        .column("is_duplicate")?
        .bool()?
        .into_iter()
        .filter(|x| x.unwrap_or(false))
        .count();

    let group_count = df
        .clone()
        .lazy()
        .filter(col("is_duplicate"))
        .select([col("md5_hash")])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?
        .height();

    println!("Found {} files in {} duplicate groups", duplicate_count, group_count);

    Ok(df)
}

pub fn generate_statistics(df: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    let total_files = df.height();
    let total_size_bytes: u64 = df.column("size_bytes")?.sum().unwrap_or(0);
    let total_size_mb = total_size_bytes as f64 / 1_048_576.0;

    let duplicate_count = df
        .column("is_duplicate")?
        .bool()?
        .into_iter()
        .filter(|x| x.unwrap_or(false))
        .count();

    let unique_extensions = df.column("extension")?.unique()?.len();

    let avg_file_size_mb = total_size_mb / total_files as f64;

    let stats_df = df! [
        "metric" => vec!["total_files", "duplicate_files", "total_size_mb", "avg_file_size_mb", "unique_extensions"],
        "value" => vec![total_files as f64, duplicate_count as f64, total_size_mb, avg_file_size_mb, unique_extensions as f64],
    ]?;

    Ok(stats_df)
}

pub fn validate_duplicates(df: &DataFrame) -> Result<(), Box<dyn Error>> {
    println!("\n=== Duplicate Detection Validation ===");

    let duplicates = df
        .clone()
        .lazy()
        .filter(col("is_duplicate").eq(lit(true)))
        .collect()?;

    if duplicates.height() == 0 {
        println!("✓ No duplicates found - validation passed");
        return Ok(());
    }

    let grouped = duplicates
        .lazy()
        .group_by([col("md5_hash")])
        .agg([col("file_path").count().alias("file_count")])
        .collect()?;

    println!("Duplicate groups found:");
    for row in 0..grouped.height() {
        let hash = grouped.column("md5_hash")?.get(row)?;
        let count = grouped.column("file_count")?.get(row)?;
        println!("  Hash: {} -> {} files", hash, count);
    }

    println!("✓ Duplicate detection validation completed");
    Ok(())
}

pub fn generate_csv_report(df: &mut DataFrame, output_path: &str) -> Result<(), Box<dyn Error>> {
    let duplicates_only = df
        .clone()
        .lazy()
        .filter(col("is_duplicate").eq(lit(true)))
        .collect()?;

    if duplicates_only.height() == 0 {
        println!("No duplicates found - CSV report not generated");
        return Ok(());
    }

    let mut file = std::fs::File::create(output_path)?;
    let mut duplicates_df = duplicates_only;

    CsvWriter::new(&mut file)
        .include_header(true)
        .finish(&mut duplicates_df)?;

    println!(
        "CSV report generated: {} ({} duplicate files)",
        output_path,
        duplicates_df.height()
    );

    Ok(())
}

pub fn empty_dataframe() -> Result<DataFrame, Box<dyn Error>> {
    Ok(df! [
        "file_path" => Vec::<String>::new(),
        "file_name" => Vec::<String>::new(),
        "extension" => Vec::<String>::new(),
        "size_bytes" => Vec::<u64>::new(),
        "size_mb" => Vec::<f64>::new(),
        "md5_hash" => Vec::<String>::new(),
        "group_count" => Vec::<u32>::new(),
        "is_duplicate" => Vec::<bool>::new(),
    ]?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_create_dataframe_marks_duplicates() {
        let temp_dir = TempDir::new().unwrap();
        let file1 = temp_dir.path().join("dup1.txt");
        let file2 = temp_dir.path().join("dup2.txt");
        let unique = temp_dir.path().join("unique.txt");

        fs::write(&file1, "duplicate").unwrap();
        fs::write(&file2, "duplicate").unwrap();
        fs::write(&unique, "unique content").unwrap();

        let file_infos = vec![
            FileInfo::new(file1.to_str().unwrap()).unwrap(),
            FileInfo::new(file2.to_str().unwrap()).unwrap(),
            FileInfo::new(unique.to_str().unwrap()).unwrap(),
        ];

        let df = create_dataframe(file_infos).unwrap();

        assert_eq!(df.height(), 3);

        let is_dup = df.column("is_duplicate").unwrap().bool().unwrap();
        let dup_count = is_dup.into_iter().filter(|x| x.unwrap_or(false)).count();
        assert_eq!(dup_count, 2);
    }
}
