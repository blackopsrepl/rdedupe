# Integration Testing Utility

This directory contains a comprehensive integration testing utility for `rdedupe` that validates duplicate detection and deduplication functionality.

## Overview

The testing utility (`test_utils.rs`) provides tools to:

1. **Generate test files** with controlled duplicate distribution
2. **Verify duplicate detection** accuracy against expected results
3. **Verify deduplication** effectiveness
4. **Validate the complete pipeline** from generation to deduplication

## Usage

### Basic Example

```rust
use test_utils::*;
use rdedupe::{walk, find, collect_file_info, create_dataframe, delete_duplicates};

// Configure test file generation
let config = TestFileConfig {
    num_files: 100,              // Generate 100 files
    duplicate_coefficient: 0.3,   // 30% will be duplicates
    extension: "txt".to_string(),
    min_size_bytes: 100,
    max_size_bytes: 1024,
};

// Generate test files
let generated = generate_test_files(config).expect("Failed to generate test files");

// Run duplicate detection
let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk");
let files = find(files, &generated.pattern());
let file_infos = collect_file_info(files).expect("Failed to collect info");
let df = create_dataframe(file_infos).expect("Failed to create dataframe");

// Verify detection accuracy (within 5% tolerance)
let verification = verify_duplicate_detection(&generated, &df, 5.0)
    .expect("Verification failed");
verification.print_report();

assert!(verification.passed);

// Perform deduplication
let delete_result = delete_duplicates(&df, false, true)
    .expect("Deduplication failed");

// Verify deduplication was effective
let dedup_verification = verify_deduplication(&generated, &delete_result)
    .expect("Deduplication verification failed");
dedup_verification.print_report();

assert!(dedup_verification.passed);
```

## TestFileConfig

Controls how test files are generated:

```rust
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
```

### Default Configuration

```rust
TestFileConfig::default()  // 100 files, 30% duplicates, .txt extension, 100-1024 bytes
```

## GeneratedFiles

Contains information about generated test files:

```rust
pub struct GeneratedFiles {
    /// Directory containing the generated files
    pub temp_dir: TempDir,

    /// All file paths generated
    pub all_files: Vec<PathBuf>,

    /// Map of content hash to file paths (groups of duplicates)
    pub expected_duplicates: HashMap<String, Vec<PathBuf>>,

    /// Number of unique file contents
    pub unique_count: usize,

    /// Total number of duplicate files
    pub duplicate_count: usize,
}
```

### Helper Methods

- `path()` - Get the path to the temp directory
- `pattern()` - Get file search pattern (e.g., ".txt")
- `extension()` - Get file extension
- `duplicate_groups_count()` - Count duplicate groups (groups with > 1 file)
- `total_duplicate_files()` - Get total files that are duplicates

## Verification Functions

### verify_duplicate_detection()

Verifies that duplicate detection matches expected results within a tolerance.

```rust
pub fn verify_duplicate_detection(
    generated: &GeneratedFiles,
    df: &DataFrame,
    tolerance_percent: f64,
) -> Result<DuplicateVerification, Box<dyn std::error::Error>>
```

**Returns:**
```rust
pub struct DuplicateVerification {
    pub expected_duplicate_files: usize,
    pub detected_duplicate_files: usize,
    pub expected_duplicate_groups: usize,
    pub detected_duplicate_groups: usize,
    pub accuracy_percent: f64,
    pub passed: bool,
    pub tolerance_percent: f64,
}
```

### verify_deduplication()

Verifies that deduplication was effective.

```rust
pub fn verify_deduplication(
    generated: &GeneratedFiles,
    delete_result: &DeleteResult,
) -> Result<DeduplicationVerification, Box<dyn std::error::Error>>
```

**Returns:**
```rust
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
```

## Test Scenarios

The `integration.rs` file includes comprehensive test scenarios:

### 1. Duplicate Detection Tests

- **30% duplicates** - Standard duplicate ratio
- **50% duplicates** - Higher duplicate ratio
- **0% duplicates** - All unique files (negative test)

### 2. Full Pipeline Tests

- **Complete deduplication pipeline** - End-to-end validation
- **Dry run** - Verify no files are deleted in dry-run mode

### 3. Scale Tests

- **Large dataset (200 files, 70% duplicates)** - High volume, high duplication
- **Small dataset (20 files, 10% duplicates)** - Low volume, low duplication

### 4. Correctness Tests

- **One file kept per group** - Verify exactly one file remains per duplicate group

## Running Tests

```bash
# Run all integration tests
cargo test --test integration

# Run specific test
cargo test test_full_deduplication_pipeline

# Run with output
cargo test test_duplicate_detection_30_percent -- --nocapture

# Run tests sequentially (recommended for integration tests)
cargo test --test integration -- --test-threads=1
```

## Test Output Example

```
=== Test File Generation ===
Total files: 100
Target duplicate coefficient: 0.30
Unique files: 70
Duplicate pairs: 15 (30 files)
Generated 100 total files

=== Duplicate Detection Verification ===
Expected duplicate files: 30
Detected duplicate files: 30
Expected duplicate groups: 15
Detected duplicate groups: 15
Accuracy: 100.00%
Tolerance: 5.00%
Status: ✓ PASSED

=== Deduplication Verification ===
Files before deduplication: 100
Files after deduplication: 85
Files deleted: 15
Expected files remaining: 85
Actual files remaining: 85
Bytes freed: 8186 (0.01 MB)
All duplicates removed: Yes
Status: ✓ PASSED
```

## How It Works

### File Generation

1. Calculates how many unique vs duplicate files to create based on coefficient
2. Generates random binary content for each unique file
3. Creates duplicate pairs by copying content
4. Tracks expected duplicate groups by MD5 hash

### Duplicate Detection Verification

1. Runs duplicate detection on generated files
2. Compares detected duplicates with expected duplicates
3. Calculates accuracy percentage
4. Verifies accuracy is within specified tolerance

### Deduplication Verification

1. Runs deduplication operation
2. Counts remaining files in directory
3. Verifies each remaining file hash is unique (no duplicates remain)
4. Confirms file count matches expected (unique files + one per duplicate group)

## Implementation Details

- **Random content generation** - Uses `rand` crate for cryptographically random bytes
- **MD5 hashing** - Consistent with main rdedupe implementation
- **Temporary directories** - Tests are isolated using `tempfile::TempDir`
- **Duplicate pairing** - Creates duplicate pairs for predictable verification
- **Parallel processing** - Tests run with rayon parallelization like production code

## Best Practices

1. **Use appropriate tolerance** - Small datasets may need higher tolerance (10%) vs large datasets (5%)
2. **Test various coefficients** - Cover edge cases (0%, 50%, 90%)
3. **Verify both detection and deletion** - Test the complete pipeline
4. **Use meaningful file sizes** - Vary size to test real-world scenarios
5. **Run tests sequentially** - Use `--test-threads=1` to avoid resource contention

## Future Enhancements

Possible improvements to the testing utility:

- Support for duplicate triplets/quadruplets (not just pairs)
- Configurable duplicate group sizes
- Performance benchmarking integration
- Statistical analysis of detection accuracy
- CSV report verification
- Multi-directory test scenarios
