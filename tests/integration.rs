// Integration tests for duplicate detection and deduplication
mod test_utils;

use rdedupe::{collect_file_info, create_dataframe, delete_duplicates, find, walk};
use test_utils::*;

/// Test: Generate files with 30% duplicates and verify detection accuracy
#[test]
fn test_duplicate_detection_30_percent() {
    let config = TestFileConfig {
        num_files: 100,
        duplicate_coefficient: 0.3,
        extension: "txt".to_string(),
        min_size_bytes: 100,
        max_size_bytes: 1024,
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Verify detection with 5% tolerance
    let verification =
        verify_duplicate_detection(&generated, &df, 5.0).expect("Verification failed");
    verification.print_report();

    assert!(
        verification.passed,
        "Duplicate detection accuracy outside tolerance: {:.2}% (expected within 5%)",
        verification.accuracy_percent
    );
}

/// Test: Generate files with 50% duplicates and verify detection accuracy
#[test]
fn test_duplicate_detection_50_percent() {
    let config = TestFileConfig {
        num_files: 100,
        duplicate_coefficient: 0.5,
        extension: "dat".to_string(),
        min_size_bytes: 50,
        max_size_bytes: 500,
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Verify detection with 5% tolerance
    let verification =
        verify_duplicate_detection(&generated, &df, 5.0).expect("Verification failed");
    verification.print_report();

    assert!(
        verification.passed,
        "Duplicate detection accuracy outside tolerance: {:.2}% (expected within 5%)",
        verification.accuracy_percent
    );
}

/// Test: Generate files with 0% duplicates (all unique)
#[test]
fn test_no_duplicates() {
    let config = TestFileConfig {
        num_files: 50,
        duplicate_coefficient: 0.0,
        extension: "bin".to_string(),
        min_size_bytes: 100,
        max_size_bytes: 500,
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Verify no duplicates detected
    let verification =
        verify_duplicate_detection(&generated, &df, 5.0).expect("Verification failed");
    verification.print_report();

    assert_eq!(
        verification.detected_duplicate_files, 0,
        "Should detect 0 duplicates when coefficient is 0.0"
    );
    assert!(
        verification.passed,
        "Verification should pass with 0 duplicates"
    );
}

/// Test: Full pipeline - generate, detect, deduplicate, verify
#[test]
fn test_full_deduplication_pipeline() {
    let config = TestFileConfig {
        num_files: 80,
        duplicate_coefficient: 0.4,
        extension: "test".to_string(),
        min_size_bytes: 200,
        max_size_bytes: 800,
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    println!("\n=== Step 1: Duplicate Detection ===");
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Verify detection
    let detection_verification =
        verify_duplicate_detection(&generated, &df, 5.0).expect("Detection verification failed");
    detection_verification.print_report();

    assert!(
        detection_verification.passed,
        "Duplicate detection should pass within tolerance"
    );

    println!("\n=== Step 2: Deduplication ===");
    // Perform deduplication with force flag
    let delete_result = delete_duplicates(&df, false, true).expect("Deduplication failed");

    println!("\n=== Step 3: Deduplication Verification ===");
    // Verify deduplication
    let dedup_verification = verify_deduplication(&generated, &delete_result)
        .expect("Deduplication verification failed");
    dedup_verification.print_report();

    assert!(
        dedup_verification.all_duplicates_removed,
        "All duplicates should be removed"
    );
    assert!(
        dedup_verification.passed,
        "Deduplication verification should pass"
    );
    assert_eq!(
        dedup_verification.actual_files_remaining, dedup_verification.expected_files_remaining,
        "Remaining files should match expected count"
    );
}

/// Test: Dry run doesn't delete files
#[test]
fn test_dry_run_no_deletion() {
    let config = TestFileConfig {
        num_files: 50,
        duplicate_coefficient: 0.3,
        extension: "txt".to_string(),
        ..Default::default()
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    let files_before = generated.all_files.len();

    // Perform dry run
    let delete_result = delete_duplicates(&df, true, true).expect("Dry run failed");

    // Count files after dry run
    let files_after = std::fs::read_dir(generated.path())
        .expect("Failed to read directory")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext == generated.extension().as_str())
        })
        .count();

    assert!(delete_result.dry_run, "Should be marked as dry run");
    assert_eq!(
        files_after, files_before,
        "Dry run should not delete any files"
    );
}

/// Test: Large dataset with high duplicate ratio
#[test]
fn test_large_dataset_high_duplicates() {
    let config = TestFileConfig {
        num_files: 200,
        duplicate_coefficient: 0.7,
        extension: "data".to_string(),
        min_size_bytes: 100,
        max_size_bytes: 2048,
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Verify detection with 5% tolerance
    let verification =
        verify_duplicate_detection(&generated, &df, 5.0).expect("Verification failed");
    verification.print_report();

    assert!(
        verification.passed,
        "Large dataset detection accuracy outside tolerance"
    );

    // Perform deduplication
    let delete_result = delete_duplicates(&df, false, true).expect("Deduplication failed");

    // Verify deduplication
    let dedup_verification = verify_deduplication(&generated, &delete_result)
        .expect("Deduplication verification failed");
    dedup_verification.print_report();

    assert!(
        dedup_verification.passed,
        "Large dataset deduplication should pass"
    );
}

/// Test: Small dataset with low duplicate ratio
#[test]
fn test_small_dataset_low_duplicates() {
    let config = TestFileConfig {
        num_files: 20,
        duplicate_coefficient: 0.1,
        extension: "small".to_string(),
        min_size_bytes: 50,
        max_size_bytes: 200,
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Verify detection
    let verification =
        verify_duplicate_detection(&generated, &df, 10.0).expect("Verification failed");
    verification.print_report();

    // With small datasets and low duplicates, allow higher tolerance
    assert!(
        verification.passed,
        "Small dataset detection should pass with appropriate tolerance"
    );
}

/// Test: Verify one file kept per duplicate group
#[test]
fn test_one_file_kept_per_group() {
    let config = TestFileConfig {
        num_files: 60,
        duplicate_coefficient: 0.4,
        extension: "grp".to_string(),
        ..Default::default()
    };

    let generated = generate_test_files(config).expect("Failed to generate test files");

    // Run duplicate detection
    let files = walk(generated.path().to_str().unwrap()).expect("Failed to walk directory");
    let files = find(files, &generated.pattern());
    let file_infos = collect_file_info(files).expect("Failed to collect file info");
    let df = create_dataframe(file_infos).expect("Failed to create dataframe");

    // Perform deduplication
    let delete_result = delete_duplicates(&df, false, true).expect("Deduplication failed");

    // Verify exactly one file is kept per duplicate group
    assert_eq!(
        delete_result.kept_files.len(),
        generated.duplicate_groups_count(),
        "Should keep exactly one file per duplicate group"
    );
}
