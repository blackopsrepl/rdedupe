mod support;

use rdedupe::fileinfo::observe_path;
use rdedupe::fingerprint::{Blake3Fingerprinter, CollisionFingerprinter, Fingerprinter};
use rdedupe::model::Observation;
use rdedupe::scheduler::Cancellation;
use rdedupe::{Budget, Completeness, Exactness, Policy};
use std::path::Path;
use support::*;

#[test]
fn seeded_trees_match_the_independent_oracle_exactly() {
    for seed in 1..=6 {
        let fixture = Fixture::new();
        seeded_tree(&fixture, seed, 120);
        let records = scan_jsonl([fixture.root()]);
        assert_eq!(
            machine_groups(&records),
            oracle(&[fixture.root()]),
            "seed {seed}"
        );
        assert_eq!(summary(&records)["completeness"], "complete");
    }
}

#[test]
fn groups_are_identical_across_readers_buffers_and_budgets() {
    let fixture = Fixture::new();
    seeded_tree(&fixture, 42, 150);
    let expected = oracle(&[fixture.root()]);
    for (readers, buffer) in [(1, 4096), (2, 65_536), (4, 1024 * 1024), (8, 4096)] {
        let budget = Budget {
            readers,
            buffer_bytes: buffer,
            ..Budget::default()
        };
        let result = library_scan_with(
            &[fixture.root()],
            Policy::default(),
            budget,
            &Blake3Fingerprinter,
        );
        assert_eq!(
            store_groups(&result),
            expected,
            "readers {readers} buffer {buffer}"
        );
    }
    let result = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        minimum_budget(),
        &Blake3Fingerprinter,
    );
    assert_eq!(store_groups(&result), expected, "minimum budget");
}

#[test]
fn forced_collisions_keep_every_equality_class_a_b_b_c_a_c_d() {
    let fixture = Fixture::new();
    let sequence = [
        b"aaaa", b"bbbb", b"bbbb", b"cccc", b"aaaa", b"cccc", b"dddd",
    ];
    let paths: Vec<_> = sequence
        .iter()
        .enumerate()
        .map(|(i, content)| fixture.write(format!("f{}", i + 1), *content))
        .collect();
    let result = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        Budget::default(),
        &CollisionFingerprinter,
    );
    let expected = Groups::from([
        group([&paths[0], &paths[4]]),
        group([&paths[1], &paths[2]]),
        group([&paths[3], &paths[5]]),
    ]);
    assert_eq!(store_groups(&result), expected);
    assert_eq!(expected, oracle(&[fixture.root()]));
    assert_eq!(result.summary.metrics.groups, 3);
    assert_eq!(
        result.summary.metrics.eliminated_by_fingerprint, 1,
        "D stays unique"
    );
}

#[test]
fn identical_samples_with_unsampled_difference_are_not_duplicates() {
    let fixture = Fixture::new();
    let a = vec![7u8; 1024 * 1024];
    let mut b = a.clone();
    b[300_000] = 8;
    let pa = fixture.write("a", &a);
    let pb = fixture.write("b", &b);
    // Prove the production fingerprints really collide, so only full comparison separates.
    let mut buffer = vec![0; 65_536];
    let cancel = Cancellation::default();
    let (fa, _) = Blake3Fingerprinter
        .fingerprint(&pa, &observe_path(&pa).unwrap(), &mut buffer, &cancel)
        .unwrap();
    let (fb, _) = Blake3Fingerprinter
        .fingerprint(&pb, &observe_path(&pb).unwrap(), &mut buffer, &cancel)
        .unwrap();
    assert_eq!(fa, fb, "fixture must defeat the sampled fingerprint");
    let result = library_scan(&[fixture.root()], Policy::default());
    assert!(store_groups(&result).is_empty());
    assert!(result.summary.metrics.comparison_bytes_read > 0);
}

#[test]
fn fingerprints_do_not_depend_on_the_buffer_size() {
    let fixture = Fixture::new();
    let content: Vec<u8> = (0..1_000_003u32).map(|i| (i % 253) as u8).collect();
    let path = fixture.write("f", &content);
    let cancel = Cancellation::default();
    let mut digests = std::collections::BTreeSet::new();
    for size in [4096usize, 65_536, 1 << 20] {
        let mut buffer = vec![0; size];
        digests.insert(
            Blake3Fingerprinter
                .fingerprint(&path, &observe_path(&path).unwrap(), &mut buffer, &cancel)
                .unwrap()
                .0,
        );
    }
    assert_eq!(digests.len(), 1);
}

#[test]
fn lengths_around_buffer_boundaries_compare_exactly() {
    let fixture = Fixture::new();
    for size in [4095usize, 4096, 4097, 8191, 8192, 8193, 65_536 * 3 + 1] {
        let content: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        fixture.write(format!("{size}/same-1"), &content);
        fixture.write(format!("{size}/same-2"), &content);
        let mut last = content.clone();
        *last.last_mut().unwrap() ^= 0xff;
        fixture.write(format!("{size}/last-byte-differs"), &last);
        let mut first = content.clone();
        first[0] ^= 0xff;
        fixture.write(format!("{size}/first-byte-differs"), &first);
    }
    let budget = Budget {
        buffer_bytes: 4096,
        ..Budget::default()
    };
    let result = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        budget,
        &CollisionFingerprinter,
    );
    assert_eq!(store_groups(&result), oracle(&[fixture.root()]));
    assert_eq!(result.summary.metrics.groups, 7);
}

#[test]
fn unique_sizes_cause_no_content_reads() {
    let fixture = Fixture::new();
    for size in 1..40 {
        fixture.write(format!("f{size}"), &vec![0u8; size * 1000]);
    }
    let result = library_scan(&[fixture.root()], Policy::default());
    let metrics = &result.summary.metrics;
    assert_eq!(metrics.fingerprint_bytes_read, 0);
    assert_eq!(metrics.comparison_bytes_read, 0);
    assert_eq!(metrics.eliminated_by_size, 39);
    assert!(store_groups(&result).is_empty());
}

#[test]
fn empty_files_group_and_hard_links_collapse() {
    let fixture = Fixture::new();
    let a = fixture.write("a", b"");
    let b = fixture.write("b", b"");
    fixture.hard_link("b", "c-link-to-b");
    let d = fixture.write("sub/d", b"");
    let result = library_scan(&[fixture.root()], Policy::default());
    assert_eq!(store_groups(&result), Groups::from([group([&a, &b, &d])]));
    assert_eq!(store_groups(&result), oracle(&[fixture.root()]));
    assert_eq!(result.summary.metrics.aliases, 1);
    // Empty files are grouped but retained unless the policy opts in.
    assert_eq!(result.summary.metrics.eligible_copies, 0);
    let included = library_scan(
        &[fixture.root()],
        Policy {
            include_empty: true,
            ..Policy::default()
        },
    );
    assert_eq!(included.summary.metrics.eligible_copies, 2);
    assert_eq!(included.summary.metrics.logical_redundant_bytes, 0);
}

struct FailOn(&'static str);

impl Fingerprinter for FailOn {
    fn fingerprint(
        &self,
        path: &Path,
        expected: &Observation,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> rdedupe::Result<([u8; 32], u64)> {
        if path.file_name().is_some_and(|n| n == self.0) {
            Err(rdedupe::Error::Unstable("injected read failure".into()))
        } else {
            Blake3Fingerprinter.fingerprint(path, expected, buffer, cancel)
        }
    }
}

#[test]
fn read_failure_is_unresolved_partial_and_blocks_plans() {
    let fixture = Fixture::new();
    let good_a = fixture.write("good-a", b"same");
    let good_b = fixture.write("good-b", b"same");
    let bad = fixture.write("bad", b"same");
    let result = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        Budget::default(),
        &FailOn("bad"),
    );
    assert_eq!(result.summary.completeness, Completeness::Partial);
    assert_eq!(result.summary.unresolved, 1);
    assert_eq!(
        store_groups(&result),
        Groups::from([group([&good_a, &good_b])])
    );
    let mut unresolved = Vec::new();
    result
        .store
        .for_each_unresolved(|item| {
            unresolved.push((item.path.to_path()?, item.operation.clone()));
            Ok(())
        })
        .unwrap();
    assert_eq!(unresolved, vec![(bad, "fingerprint".to_string())]);
    let plan = fixture.path("plan");
    assert!(matches!(
        rdedupe::plan::write_plan(&result, &plan, rdedupe::ActionKind::Quarantine),
        Err(rdedupe::Error::Partial)
    ));
    assert!(!plan.exists());
}

/// Rewrites one file just before it is fingerprinted, after it was inventoried.
struct RewriteBefore {
    name: &'static str,
    content: Vec<u8>,
}

impl Fingerprinter for RewriteBefore {
    fn fingerprint(
        &self,
        path: &Path,
        expected: &Observation,
        buffer: &mut [u8],
        cancel: &Cancellation,
    ) -> rdedupe::Result<([u8; 32], u64)> {
        if path.file_name().is_some_and(|n| n == self.name) {
            std::fs::write(path, &self.content).unwrap();
        }
        Blake3Fingerprinter.fingerprint(path, expected, buffer, cancel)
    }
}

#[test]
fn a_file_changed_after_inventory_is_unresolved_never_unique() {
    let fixture = Fixture::new();
    fixture.write("a", &[1; 10]);
    fixture.write("b", &[1; 10]);
    fixture.write("c", &[2; 20]);
    let result = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        Budget {
            readers: 1,
            ..Budget::default()
        },
        &RewriteBefore {
            name: "b",
            content: vec![2; 20],
        },
    );
    assert_eq!(result.summary.completeness, Completeness::Partial);
    assert_eq!(result.summary.exactness, Exactness::Exact);
    assert!(result.summary.unresolved >= 1);
}

#[test]
fn sample_collisions_are_refined_with_linear_reads() {
    // N files identical in every sampled window, each different in unsampled bytes: the
    // engine must not compare every file against every equality class.
    let fixture = Fixture::new();
    let size = 300 * 1024;
    let n = 40u32;
    for i in 0..n {
        let mut content = vec![9u8; size];
        content[100_000..100_004].copy_from_slice(&i.to_le_bytes());
        fixture.write(format!("f{i:02}"), &content);
    }
    let result = library_scan(&[fixture.root()], Policy::default());
    assert!(store_groups(&result).is_empty());
    let metrics = &result.summary.metrics;
    assert_eq!(metrics.full_hash_bytes_read, u64::from(n) * size as u64);
    assert_eq!(metrics.comparison_bytes_read, 0);
}
