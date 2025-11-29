//! File system operations for walking directories and finding files.

use std::error::Error;
use walkdir::WalkDir;

pub fn walk(path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut files = Vec::new();

    for entry in WalkDir::new(path) {
        let entry = entry?;

        if entry.file_type().is_file() {
            files.push(entry.path().to_string_lossy().to_string());
        }
    }

    Ok(files)
}

pub fn find(files: Vec<String>, pattern: &str) -> Vec<String> {
    let mut matches = Vec::new();

    for file in files {
        if file.contains(pattern) {
            matches.push(file);
        }
    }

    matches
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_walk_finds_files() {
        let files = walk("tests/inputs").unwrap();
        assert!(!files.is_empty());
        assert!(files.iter().any(|f| f.contains("one.txt")));
    }

    #[test]
    fn test_walk_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let files = walk(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_filters_by_pattern() {
        let files = vec![
            "file.txt".to_string(),
            "file.jpg".to_string(),
            "another.txt".to_string(),
        ];
        let matches = find(files, ".txt");
        assert_eq!(matches.len(), 2);
        assert!(matches.iter().all(|f| f.contains(".txt")));
    }

    #[test]
    fn test_find_no_matches() {
        let files = vec!["file.txt".to_string(), "file.jpg".to_string()];
        let matches = find(files, ".pdf");
        assert!(matches.is_empty());
    }
}
