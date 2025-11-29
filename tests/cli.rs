//test duplicates
use assert_cmd::cargo::*;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn usage() {
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Finds and removes duplicate files",
        ));
}

#[test]
fn dedupe_report_default() {
    // Default action should be report (no deletion)
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg("tests/inputs")
        .arg("--pattern")
        .arg(".txt")
        .assert()
        .success()
        .stdout(predicate::str::contains("Duplicate Files Found"));
}

#[test]
fn dedupe_dry_run_shows_preview() {
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg("tests/inputs")
        .arg("--pattern")
        .arg(".txt")
        .arg("--delete")
        .arg("--dry-run")
        .assert()
        .success()
        .stdout(predicate::str::contains("DRY RUN"))
        .stdout(predicate::str::contains("would be DELETED"))
        .stdout(predicate::str::contains("would be KEPT"));
}

#[test]
fn dedupe_dry_run_no_files_deleted() {
    // Create a temp directory with duplicate files
    let temp_dir = TempDir::new().unwrap();
    let file1 = temp_dir.path().join("file1.txt");
    let file2 = temp_dir.path().join("file2.txt");

    fs::write(&file1, "duplicate content").unwrap();
    fs::write(&file2, "duplicate content").unwrap();

    // Run dry-run
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg(temp_dir.path())
        .arg("--pattern")
        .arg(".txt")
        .arg("--delete")
        .arg("--dry-run")
        .assert()
        .success();

    // Both files should still exist
    assert!(file1.exists(), "file1 should still exist after dry run");
    assert!(file2.exists(), "file2 should still exist after dry run");
}

#[test]
fn dedupe_delete_force_removes_duplicates() {
    // Create a temp directory with duplicate files
    let temp_dir = TempDir::new().unwrap();
    let file1 = temp_dir.path().join("aaa_first.txt");
    let file2 = temp_dir.path().join("bbb_second.txt");
    let unique = temp_dir.path().join("unique.txt");

    fs::write(&file1, "duplicate content").unwrap();
    fs::write(&file2, "duplicate content").unwrap();
    fs::write(&unique, "unique content").unwrap();

    // Run with --force to skip confirmation
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg(temp_dir.path())
        .arg("--pattern")
        .arg(".txt")
        .arg("--delete")
        .arg("--force")
        .assert()
        .success()
        .stdout(predicate::str::contains("Deleted"));

    // First file (alphabetically) should be kept, second deleted
    assert!(file1.exists(), "first file (alphabetically) should be kept");
    assert!(
        !file2.exists(),
        "second file (alphabetically) should be deleted"
    );
    assert!(unique.exists(), "unique file should not be touched");
}

#[test]
fn dedupe_keeps_one_copy_per_group() {
    // Create temp directory with multiple duplicate groups
    let temp_dir = TempDir::new().unwrap();

    // Group 1: 3 identical files
    let g1_a = temp_dir.path().join("group1_a.txt");
    let g1_b = temp_dir.path().join("group1_b.txt");
    let g1_c = temp_dir.path().join("group1_c.txt");
    fs::write(&g1_a, "group one content").unwrap();
    fs::write(&g1_b, "group one content").unwrap();
    fs::write(&g1_c, "group one content").unwrap();

    // Group 2: 2 identical files
    let g2_a = temp_dir.path().join("group2_a.txt");
    let g2_b = temp_dir.path().join("group2_b.txt");
    fs::write(&g2_a, "group two content").unwrap();
    fs::write(&g2_b, "group two content").unwrap();

    // Run deletion
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg(temp_dir.path())
        .arg("--pattern")
        .arg(".txt")
        .arg("--delete")
        .arg("--force")
        .assert()
        .success();

    // Count remaining files
    let remaining: Vec<_> = fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "txt"))
        .collect();

    // Should have exactly 2 files (one from each group)
    assert_eq!(
        remaining.len(),
        2,
        "should keep exactly one file per duplicate group"
    );

    // Verify content - both groups should still have one representative
    let contents: Vec<String> = remaining
        .iter()
        .map(|e| fs::read_to_string(e.path()).unwrap())
        .collect();

    assert!(contents.contains(&"group one content".to_string()));
    assert!(contents.contains(&"group two content".to_string()));
}

#[test]
fn handles_nonexistent_path_gracefully() {
    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg("/nonexistent/path/that/does/not/exist")
        .arg("--pattern")
        .arg(".txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn dedupe_no_duplicates_found() {
    // Create temp directory with only unique files
    let temp_dir = TempDir::new().unwrap();
    let file1 = temp_dir.path().join("unique1.txt");
    let file2 = temp_dir.path().join("unique2.txt");

    fs::write(&file1, "content one").unwrap();
    fs::write(&file2, "content two").unwrap();

    let mut cmd = cargo_bin_cmd!("rdedupe");
    cmd.arg(temp_dir.path())
        .arg("--pattern")
        .arg(".txt")
        .arg("--delete")
        .arg("--dry-run")
        .assert()
        .success()
        .stdout(predicate::str::contains("No duplicate files found"));
}
