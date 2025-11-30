//! rdedupe - A tool to find and remove duplicate files.

mod dataframe;
mod delete;
mod fileinfo;
mod fs;

pub use dataframe::{
    create_dataframe, empty_dataframe, generate_csv_report, generate_statistics,
    validate_duplicates,
};
pub use delete::{delete_duplicates, DeleteResult};
pub use fileinfo::{collect_file_info, FileInfo};
pub use fs::{find, walk};

use log::info;
use polars::prelude::*;
use std::error::Error;

pub fn display_thread_info() {
    let num_cpus = num_cpus::get();
    let rayon_threads = rayon::current_num_threads();

    info!("💻 CPU cores: {}", num_cpus);
    info!("🧵 Rayon thread pool size: {}", rayon_threads);
}

pub fn run_with_dataframe(
    path: &str,
    pattern: &str,
    output_csv: Option<&str>,
) -> Result<DataFrame, Box<dyn Error>> {
    info!("Scanning directory: {}", path);

    let files = walk(path)?;
    let files = find(files, pattern);

    info!("Found {} files matching pattern '{}'", files.len(), pattern);

    if files.is_empty() {
        info!("No files found to analyze.");
        return empty_dataframe();
    }

    let file_infos = collect_file_info(files)?;
    let df = create_dataframe(file_infos)?;

    let stats = generate_statistics(&df)?;

    info!("\n=== File Analysis Summary ===");
    info!("{}", stats);

    validate_duplicates(&df)?;

    let duplicates = df
        .clone()
        .lazy()
        .filter(col("is_duplicate").eq(lit(true)))
        .collect()?;

    if duplicates.height() > 0 {
        info!("\n=== Duplicate Files Found ===");
        info!("{}", duplicates);
    } else {
        info!("\nNo duplicate files found.");
    }

    if let Some(csv_path) = output_csv {
        let mut df_copy = df.clone();
        generate_csv_report(&mut df_copy, csv_path)?;
    }

    Ok(df)
}
