//! End-to-end pipeline: generate → scan → plan → apply → rescan → restore, with exact
//! survivor assertions against the independent oracle.

mod support;

use std::collections::BTreeSet;
use std::os::unix::ffi::OsStrExt;
use support::*;

fn all_files(fixture: &Fixture) -> BTreeSet<(Vec<u8>, Vec<u8>)> {
    let mut out = BTreeSet::new();
    let mut stack = vec![fixture.root().to_path_buf()];
    while let Some(dir) = stack.pop() {
        if dir.join("RDEDUPE-QUARANTINE").exists() {
            continue;
        }
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file() {
                out.insert((bytes(&path), std::fs::read(&path).unwrap()));
            }
        }
    }
    out
}

#[test]
fn full_pipeline_removes_exactly_the_eligible_copies_and_restores_them() {
    for seed in [3u64, 11, 29] {
        let fixture = Fixture::new();
        seeded_tree(&fixture, seed, 150);
        let before = all_files(&fixture);
        let expected_groups = oracle(&[fixture.root()]);
        // Expected survivors: every non-group file, the smallest path of each group, and
        // every member of an empty-file group (retained by default).
        let removed: BTreeSet<Vec<u8>> = expected_groups
            .iter()
            .filter(|g| {
                let first = g.iter().next().unwrap();
                let path = std::path::Path::new(std::ffi::OsStr::from_bytes(first));
                std::fs::metadata(path).unwrap().len() > 0
            })
            .flat_map(|g| g.iter().skip(1).cloned())
            .collect();

        let work = Fixture::new();
        let plan = work.path("plan");
        let journal = work.path("journal");
        rdedupe()
            .args(["plan", fixture.str(), "--output"])
            .arg(&plan)
            .assert()
            .success();
        let apply = rdedupe()
            .args(["apply", "--force", "--format", "jsonl", "--plan"])
            .arg(&plan)
            .arg("--journal")
            .arg(&journal)
            .output()
            .unwrap();
        assert!(apply.status.success(), "seed {seed}");
        let records = jsonl(&apply.stdout);
        let summary = records.last().unwrap();
        assert_eq!(summary["record"], "apply_summary");
        assert_eq!(
            summary["quarantined"].as_u64().unwrap() as usize,
            removed.len()
        );

        let after = all_files(&fixture);
        let survivors: BTreeSet<Vec<u8>> = after.iter().map(|(p, _)| p.clone()).collect();
        let expected_survivors: BTreeSet<Vec<u8>> = before
            .iter()
            .map(|(p, _)| p.clone())
            .filter(|p| !removed.contains(p))
            .collect();
        assert_eq!(survivors, expected_survivors, "seed {seed}");
        // Every distinct content still exists exactly once more than zero times.
        let contents_before: BTreeSet<&Vec<u8>> = before.iter().map(|(_, c)| c).collect();
        let contents_after: BTreeSet<&Vec<u8>> = after.iter().map(|(_, c)| c).collect();
        assert_eq!(contents_before, contents_after, "no content lost");
        // Only empty-file groups remain (retained by default), and the quarantine is not
        // rescanned, so a rescan reports exactly those.
        let remaining = oracle(&[fixture.root()]);
        assert!(
            remaining.iter().all(|g| g.iter().all(|p| {
                let path = std::path::Path::new(std::ffi::OsStr::from_bytes(p));
                std::fs::metadata(path).unwrap().len() == 0
            })),
            "no non-empty duplicates remain"
        );
        let rescan = scan_jsonl([fixture.root()]);
        assert_eq!(
            machine_groups(&rescan),
            remaining,
            "quarantine is not rescanned"
        );

        rdedupe()
            .args(["restore", "--journal"])
            .arg(&journal)
            .assert()
            .success();
        assert_eq!(all_files(&fixture), before, "seed {seed}: restore is exact");
    }
}

#[test]
fn dry_run_and_scan_never_mutate() {
    let fixture = Fixture::new();
    seeded_tree(&fixture, 1, 60);
    let before = all_files(&fixture);
    rdedupe()
        .arg(fixture.root())
        .args(["--delete", "--dry-run"])
        .assert()
        .success();
    rdedupe()
        .args(["scan", fixture.str(), "--format", "nul"])
        .assert()
        .success();
    assert_eq!(all_files(&fixture), before);
}
