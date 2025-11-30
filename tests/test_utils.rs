// Integration testing utilities for rdedupe
use rand::Rng;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Once;
use tempfile::TempDir;

static INIT: Once = Once::new();

/// Initialize logger for tests
pub fn init_test_logger() {
    INIT.call_once(|| {
        env_logger::builder()
            .is_test(true)
            .try_init()
            .ok();
    });
}

/// Configuration for test file generation
#[derive(Debug, Clone)]
pub struct TestFileConfig {
    /// Total number of files to generate
    pub num_files: usize,
    /// Ratio of files that should be duplicates (0.0 to 1.0)
    /// E.g., 0.3 means 30% of files will be duplicates
    pub duplicate_coefficient: f64,
    /// File extension for generated files
    pub extension: String,
    /// Minimum file size in bytes
    pub min_size_bytes: usize,
    /// Maximum file size in bytes
    pub max_size_bytes: usize,
}

impl Default for TestFileConfig {
    fn default() -> Self {
        TestFileConfig {
            num_files: 100,
            duplicate_coefficient: 0.3,
            extension: "txt".to_string(),
            min_size_bytes: 100,
            max_size_bytes: 1024,
        }
    }
}

/// Result of test file generation
#[derive(Debug)]
pub struct GeneratedFiles {
    /// Directory containing the generated files
    pub temp_dir: TempDir,
    /// All file paths generated
    pub all_files: Vec<PathBuf>,
    /// Map of content hash to file paths (groups of duplicates)
    pub expected_duplicates: HashMap<String, Vec<PathBuf>>,
    /// Number of unique file contents
    pub unique_count: usize,
}

impl GeneratedFiles {
    /// Get the path to the temp directory
    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    /// Get pattern for file searching
    pub fn pattern(&self) -> String {
        format!(".{}", self.extension())
    }

    /// Get file extension
    pub fn extension(&self) -> String {
        self.all_files
            .first()
            .and_then(|p| p.extension())
            .and_then(|e| e.to_str())
            .unwrap_or("txt")
            .to_string()
    }

    /// Count how many duplicate groups exist (groups with > 1 file)
    pub fn duplicate_groups_count(&self) -> usize {
        self.expected_duplicates
            .values()
            .filter(|files| files.len() > 1)
            .count()
    }

    /// Get total files that are part of duplicate groups
    pub fn total_duplicate_files(&self) -> usize {
        self.expected_duplicates
            .values()
            .filter(|files| files.len() > 1)
            .map(|files| files.len())
            .sum()
    }
}

/// Generate random file content
fn generate_random_content(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

/// Generate test files with controlled duplicate distribution
///
/// # Arguments
/// * `config` - Configuration for file generation
///
/// # Returns
/// * `GeneratedFiles` - Information about generated files and expected duplicates
pub fn generate_test_files(
    config: TestFileConfig,
) -> Result<GeneratedFiles, Box<dyn std::error::Error>> {
    if config.duplicate_coefficient < 0.0 || config.duplicate_coefficient > 1.0 {
        return Err("duplicate_coefficient must be between 0.0 and 1.0".into());
    }

    let temp_dir = TempDir::new()?;
    let mut all_files = Vec::new();
    let mut expected_duplicates: HashMap<String, Vec<PathBuf>> = HashMap::new();
    let mut rng = rand::thread_rng();

    // Calculate how many unique files vs duplicates
    // If coefficient is 0.3 and we have 100 files:
    // - We want 30 duplicate files total
    // - We could have various distributions, e.g., 15 pairs, or 10 triplets, etc.
    // For simplicity, we'll create duplicate pairs

    let target_duplicate_count =
        (config.num_files as f64 * config.duplicate_coefficient).round() as usize;

    // Ensure we have an even number of duplicates (for pairing)
    let duplicate_count = if target_duplicate_count % 2 == 0 {
        target_duplicate_count
    } else {
        target_duplicate_count.saturating_sub(1)
    };

    let unique_count = config.num_files - duplicate_count;
    let duplicate_pairs = duplicate_count / 2;

    println!("\n=== Test File Generation ===");
    println!("Total files: {}", config.num_files);
    println!(
        "Target duplicate coefficient: {:.2}",
        config.duplicate_coefficient
    );
    println!("Unique files: {}", unique_count);
    println!(
        "Duplicate pairs: {} ({} files)",
        duplicate_pairs, duplicate_count
    );

    // Generate unique files
    for i in 0..unique_count {
        let size = rng.gen_range(config.min_size_bytes..=config.max_size_bytes);
        let content = generate_random_content(size);
        let hash = format!("{:x}", md5::compute(&content));

        let file_path = temp_dir
            .path()
            .join(format!("unique_{:04}.{}", i, config.extension));
        fs::write(&file_path, &content)?;

        all_files.push(file_path.clone());
        expected_duplicates.entry(hash).or_default().push(file_path);
    }

    // Generate duplicate pairs
    for i in 0..duplicate_pairs {
        let size = rng.gen_range(config.min_size_bytes..=config.max_size_bytes);
        let content = generate_random_content(size);
        let hash = format!("{:x}", md5::compute(&content));

        // Create first file
        let file_path1 = temp_dir
            .path()
            .join(format!("dup_pair_{:04}_a.{}", i, config.extension));
        fs::write(&file_path1, &content)?;
        all_files.push(file_path1.clone());
        expected_duplicates
            .entry(hash.clone())
            .or_default()
            .push(file_path1);

        // Create duplicate
        let file_path2 = temp_dir
            .path()
            .join(format!("dup_pair_{:04}_b.{}", i, config.extension));
        fs::write(&file_path2, &content)?;
        all_files.push(file_path2.clone());
        expected_duplicates
            .entry(hash)
            .or_default()
            .push(file_path2);
    }

    println!("Generated {} total files", all_files.len());

    Ok(GeneratedFiles {
        temp_dir,
        all_files,
        expected_duplicates,
        unique_count,
    })
}

/// Verification result for duplicate detection
#[derive(Debug)]
pub struct DuplicateVerification {
    pub expected_duplicate_files: usize,
    pub detected_duplicate_files: usize,
    pub expected_duplicate_groups: usize,
    pub detected_duplicate_groups: usize,
    pub accuracy_percent: f64,
    pub passed: bool,
    pub tolerance_percent: f64,
}

impl DuplicateVerification {
    pub fn print_report(&self) {
        println!("\n=== Duplicate Detection Verification ===");
        println!(
            "Expected duplicate files: {}",
            self.expected_duplicate_files
        );
        println!(
            "Detected duplicate files: {}",
            self.detected_duplicate_files
        );
        println!(
            "Expected duplicate groups: {}",
            self.expected_duplicate_groups
        );
        println!(
            "Detected duplicate groups: {}",
            self.detected_duplicate_groups
        );
        println!("Accuracy: {:.2}%", self.accuracy_percent);
        println!("Tolerance: {:.2}%", self.tolerance_percent);
        println!(
            "Status: {}",
            if self.passed {
                "✓ PASSED"
            } else {
                "✗ FAILED"
            }
        );
    }
}

/// Verify that duplicate detection matches expected results
///
/// # Arguments
/// * `generated` - The generated test files
/// * `df` - DataFrame from duplicate detection
/// * `tolerance_percent` - Acceptable deviation percentage (e.g., 5.0 for 5%)
pub fn verify_duplicate_detection(
    generated: &GeneratedFiles,
    df: &polars::frame::DataFrame,
    tolerance_percent: f64,
) -> Result<DuplicateVerification, Box<dyn std::error::Error>> {
    use polars::prelude::*;

    // Get detected duplicates from DataFrame
    let duplicates = df
        .clone()
        .lazy()
        .filter(col("is_duplicate").eq(lit(true)))
        .collect()?;

    let detected_duplicate_files = duplicates.height();

    // Count detected duplicate groups
    let groups = duplicates
        .clone()
        .lazy()
        .group_by([col("md5_hash")])
        .agg([col("file_path").count().alias("count")])
        .collect()?;

    let detected_duplicate_groups = groups.height();

    // Expected values
    let expected_duplicate_files = generated.total_duplicate_files();
    let expected_duplicate_groups = generated.duplicate_groups_count();

    // Calculate accuracy
    let accuracy_percent = if expected_duplicate_files > 0 {
        (detected_duplicate_files as f64 / expected_duplicate_files as f64) * 100.0
    } else {
        if detected_duplicate_files == 0 {
            100.0
        } else {
            0.0
        }
    };

    // Check if within tolerance
    let deviation = (100.0 - accuracy_percent).abs();
    let passed = deviation <= tolerance_percent;

    Ok(DuplicateVerification {
        expected_duplicate_files,
        detected_duplicate_files,
        expected_duplicate_groups,
        detected_duplicate_groups,
        accuracy_percent,
        passed,
        tolerance_percent,
    })
}

/// Verification result for deduplication
#[derive(Debug)]
pub struct DeduplicationVerification {
    pub files_before: usize,
    pub files_after: usize,
    pub files_deleted: usize,
    pub expected_files_remaining: usize,
    pub actual_files_remaining: usize,
    pub bytes_freed: u64,
    pub all_duplicates_removed: bool,
    pub passed: bool,
}

impl DeduplicationVerification {
    pub fn print_report(&self) {
        println!("\n=== Deduplication Verification ===");
        println!("Files before deduplication: {}", self.files_before);
        println!("Files after deduplication: {}", self.files_after);
        println!("Files deleted: {}", self.files_deleted);
        println!(
            "Expected files remaining: {}",
            self.expected_files_remaining
        );
        println!("Actual files remaining: {}", self.actual_files_remaining);
        println!(
            "Bytes freed: {} ({:.2} MB)",
            self.bytes_freed,
            self.bytes_freed as f64 / 1_048_576.0
        );
        println!(
            "All duplicates removed: {}",
            if self.all_duplicates_removed {
                "Yes"
            } else {
                "No"
            }
        );
        println!(
            "Status: {}",
            if self.passed {
                "✓ PASSED"
            } else {
                "✗ FAILED"
            }
        );
    }
}

/// Verify that deduplication was effective
///
/// # Arguments
/// * `generated` - The generated test files
/// * `delete_result` - Result from delete_duplicates operation
pub fn verify_deduplication(
    generated: &GeneratedFiles,
    delete_result: &rdedupe::DeleteResult,
) -> Result<DeduplicationVerification, Box<dyn std::error::Error>> {
    let files_before = generated.all_files.len();
    let expected_files_remaining = generated.unique_count + generated.duplicate_groups_count();

    // Count actual files remaining in directory
    let actual_files_remaining = fs::read_dir(generated.path())?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext == generated.extension().as_str())
        })
        .count();

    let files_deleted = delete_result.deleted_count;
    let files_after = actual_files_remaining;
    let bytes_freed = delete_result.bytes_freed;

    // Verify no duplicates remain by checking actual files
    let mut remaining_hashes: HashMap<String, usize> = HashMap::new();

    for entry in fs::read_dir(generated.path())? {
        let entry = entry?;
        let path = entry.path();

        if path
            .extension()
            .map_or(false, |ext| ext == generated.extension().as_str())
        {
            let content = fs::read(&path)?;
            let hash = format!("{:x}", md5::compute(&content));
            *remaining_hashes.entry(hash).or_insert(0) += 1;
        }
    }

    // Check if any hash appears more than once (which would mean duplicates remain)
    let all_duplicates_removed = remaining_hashes.values().all(|&count| count == 1);

    // Verification passes if:
    // 1. Actual files match expected (one per unique content)
    // 2. No duplicate hashes remain
    let passed = actual_files_remaining == expected_files_remaining && all_duplicates_removed;

    Ok(DeduplicationVerification {
        files_before,
        files_after,
        files_deleted,
        expected_files_remaining,
        actual_files_remaining,
        bytes_freed,
        all_duplicates_removed,
        passed,
    })
}
