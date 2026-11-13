mod support;

use rdedupe::scheduler::Cancellation;
use rdedupe::{Budget, Exactness, Policy};
use std::path::Path;
use support::*;

fn scan(root: &Path, cache: &Path, trust: bool, fresh: bool) -> rdedupe::ScanResult {
    let mut config = scan_config(&[root], Budget::default());
    config.cache = Some(cache.to_path_buf());
    config.trust_cache = trust;
    config.fresh = fresh;
    rdedupe::scan(config, Policy::default(), &Cancellation::default()).expect("scan")
}

/// Rewrites the digest of every cache entry whose path ends with `suffix`.
fn poison(cache: &Path, suffix: &str) {
    let text = std::fs::read_to_string(cache).unwrap();
    let mut out = String::new();
    for (i, line) in text.lines().enumerate() {
        let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
        if i > 0 {
            let path = decode_path(&value["path"]);
            if path.to_string_lossy().ends_with(suffix) {
                value["digest"] = "ab".repeat(32).into();
            }
        }
        out.push_str(&serde_json::to_string(&value).unwrap());
        out.push('\n');
    }
    std::fs::write(cache, out).unwrap();
}

#[test]
fn exact_mode_never_lets_a_stale_negative_hide_a_duplicate() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let cache = work.path("fingerprints.cache");
    let a = fixture.write("a", b"same");
    let b = fixture.write("b", b"same");
    scan(fixture.root(), &cache, false, false);
    // A stale entry whose validators match but whose fingerprint claims b differs.
    poison(&cache, "/b");
    let exact = scan(fixture.root(), &cache, false, false);
    assert_eq!(exact.summary.exactness, Exactness::Exact);
    assert_eq!(store_groups(&exact), Groups::from([group([&a, &b])]));
    assert_eq!(exact.summary.metrics.cached_fingerprints_reused, 0);

    // Trusting the cache is explicitly heuristic, and heuristic scans cannot authorize plans.
    poison(&cache, "/b");
    let trusted = scan(fixture.root(), &cache, true, false);
    assert_eq!(trusted.summary.exactness, Exactness::Heuristic);
    assert!(trusted.summary.cache_provenance.starts_with("heuristic"));
    assert!(
        store_groups(&trusted).is_empty(),
        "the stale entry hid the duplicate"
    );
    assert!(matches!(
        rdedupe::plan::write_plan(
            &trusted,
            &work.path("plan"),
            rdedupe::ActionKind::Quarantine
        ),
        Err(rdedupe::Error::Heuristic)
    ));
    assert!(!work.path("plan").exists());
}

#[test]
fn trusted_reuse_requires_every_validator_to_match() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let cache = work.path("c");
    fixture.write("a", b"aaaa");
    let b = fixture.write("b", b"bbbb");
    fixture.write("keep-size-group", b"zzzz");
    scan(fixture.root(), &cache, false, false);
    let warm = scan(fixture.root(), &cache, true, false);
    assert_eq!(warm.summary.metrics.cached_fingerprints_reused, 3);
    assert_eq!(warm.summary.metrics.fingerprint_bytes_read, 0);

    // b becomes a copy of a; its mtime is restored, but replacement changes the inode.
    let modified = std::fs::metadata(&b).unwrap().modified().unwrap();
    let replacement = fixture.write("replacement", b"aaaa");
    std::fs::File::options()
        .write(true)
        .open(&replacement)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    std::fs::rename(&replacement, &b).unwrap();
    let after = scan(fixture.root(), &cache, true, false);
    assert_eq!(after.summary.metrics.cached_fingerprints_reused, 2);
    assert_eq!(
        store_groups(&after),
        Groups::from([group([fixture.path("a"), b.clone()])])
    );

    // In-place rewrite with restored mtime still changes ctime, so the entry is not reused.
    scan(fixture.root(), &cache, false, false);
    let file = std::fs::File::options().write(true).open(&b).unwrap();
    std::os::unix::fs::FileExt::write_at(&file, b"z", 0).unwrap();
    file.set_modified(modified).unwrap();
    drop(file);
    let rewritten = scan(fixture.root(), &cache, true, false);
    assert_eq!(rewritten.summary.metrics.cached_fingerprints_reused, 2);
}

#[test]
fn corrupt_or_foreign_caches_are_ignored_and_replaced() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let cache = work.path("c");
    let a = fixture.write("a", b"same");
    let b = fixture.write("b", b"same");
    for garbage in [
        &b"not json at all"[..],
        b"{\"schema\":\"rdedupe-cache-v1\",\"layout\":\"x\",\"authority\":\"\"}\n",
    ] {
        std::fs::write(&cache, garbage).unwrap();
        let result = scan(fixture.root(), &cache, true, false);
        assert!(
            result.summary.cache_provenance.contains("cache ignored"),
            "{}",
            result.summary.cache_provenance
        );
        assert_eq!(result.summary.exactness, Exactness::Exact);
        assert_eq!(store_groups(&result), Groups::from([group([&a, &b])]));
        assert!(std::fs::read_to_string(&cache)
            .unwrap()
            .starts_with("{\"schema\":\"rdedupe-cache-v2\""));
    }
    // A corrupt record after a valid header discards every entry, not just the bad one.
    let mut text = std::fs::read_to_string(&cache).unwrap();
    text.push_str("{broken\n");
    std::fs::write(&cache, text).unwrap();
    let result = scan(fixture.root(), &cache, true, false);
    assert!(result.summary.cache_provenance.contains("corrupt record"));
    assert_eq!(result.summary.metrics.cached_fingerprints_reused, 0);
}

#[test]
fn fresh_mode_is_equivalent_to_no_cache() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let cache = work.path("c");
    seeded_tree(&fixture, 5, 60);
    scan(fixture.root(), &cache, false, false);
    poison(&cache, "0");
    let fresh = scan(fixture.root(), &cache, true, true);
    assert_eq!(fresh.summary.exactness, Exactness::Exact);
    assert_eq!(fresh.summary.metrics.cached_fingerprints_reused, 0);
    assert_eq!(store_groups(&fresh), oracle(&[fixture.root()]));
}

#[test]
fn cache_inside_the_scanned_tree_is_never_inventoried_and_writes_are_atomic() {
    let fixture = Fixture::new();
    let a = fixture.write("a", b"same");
    let b = fixture.write("b", b"same");
    let cache = fixture.path("fingerprints.cache");
    scan(fixture.root(), &cache, false, false);
    let second = scan(fixture.root(), &cache, false, false);
    assert_eq!(second.summary.metrics.paths, 2);
    assert_eq!(store_groups(&second), Groups::from([group([&a, &b])]));
    let leftovers: Vec<_> = std::fs::read_dir(fixture.root())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().contains(".tmp-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}
