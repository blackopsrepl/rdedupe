//! File information collection and parallel processing.

use indicatif::{ParallelProgressIterator, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::error::Error;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub name: String,
    pub extension: String,
    pub size_bytes: u64,
    pub size_mb: f64,
    pub md5_hash: String,
    pub created: Option<String>,
    pub modified: Option<String>,
}

impl FileInfo {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let path_obj = Path::new(path);
        let metadata = fs::metadata(path)?;

        let name = path_obj
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let extension = path_obj
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let size_bytes = metadata.len();
        let size_mb = size_bytes as f64 / 1_048_576.0;

        let file_content = fs::read(path)?;
        let md5_hash = format!("{:x}", md5::compute(&file_content));

        let created = metadata
            .created()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| format!("{}", duration.as_secs()));

        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| format!("{}", duration.as_secs()));

        Ok(FileInfo {
            path: path.to_string(),
            name,
            extension,
            size_bytes,
            size_mb,
            md5_hash,
            created,
            modified,
        })
    }
}

pub fn collect_file_info(files: Vec<String>) -> Result<Vec<FileInfo>, Box<dyn Error>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }

    println!(
        "\nAnalyzing {} files with {} threads...",
        files.len(),
        rayon::current_num_threads()
    );

    let pb = indicatif::ProgressBar::new(files.len() as u64);
    let sty = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("Invalid progress bar template")
        .progress_chars("##-");

    pb.set_style(sty);
    pb.set_message("Computing MD5 hashes...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let file_infos: Vec<Option<FileInfo>> = files
        .par_iter()
        .progress_with(pb.clone())
        .map(|file_path| FileInfo::new(file_path).ok())
        .collect();

    pb.finish_with_message("✓ File analysis complete!");
    println!();

    let valid_infos: Vec<FileInfo> = file_infos.into_iter().flatten().collect();

    Ok(valid_infos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_info_creates_correctly() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        fs::write(&file_path, "test content").unwrap();

        let info = FileInfo::new(file_path.to_str().unwrap()).unwrap();
        assert_eq!(info.name, "test.txt");
        assert_eq!(info.extension, "txt");
        assert_eq!(info.size_bytes, 12);
        assert!(!info.md5_hash.is_empty());
    }
}
