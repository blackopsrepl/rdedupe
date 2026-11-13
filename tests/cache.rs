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
        &b"{\"schema\":\"rdedupe-cache-v1\",\"layout\":\"x\",\"authority\":\"\"}\n"[..],
        b"{\"schema\":\"rdedupe-cache-v2\",\"layout\":\"other-layout\",\"authority\":\"\"}\n",
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

/// `--cache` must never destroy a file that is not an rdedupe cache: a source file, a
/// symlink, a directory or a hard-linked alias is refused before scanning and left intact.
#[test]
fn cache_path_that_is_not_a_cache_is_refused_and_untouched() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    let precious = fixture.write("important.txt", b"precious only copy");
    let mut config = scan_config(&[fixture.root()], Budget::default());
    config.cache = Some(precious.clone());
    let Err(error) = rdedupe::scan(config, Policy::default(), &Cancellation::default()) else {
        panic!("a user file is not a cache");
    };
    assert!(error.to_string().contains("refusing --cache"), "{error}");
    assert_eq!(std::fs::read(&precious).unwrap(), b"precious only copy");

    // A real cache that gained a second name is an alias of data now: refused.
    let cache = work.path("c");
    scan(fixture.root(), &cache, false, false);
    let alias = work.path("alias");
    std::fs::hard_link(&cache, &alias).unwrap();
    let mut config = scan_config(&[fixture.root()], Budget::default());
    config.cache = Some(cache.clone());
    assert!(rdedupe::scan(config, Policy::default(), &Cancellation::default()).is_err());
    std::fs::remove_file(&alias).unwrap();

    #[cfg(unix)]
    {
        let link = work.path("link");
        std::os::unix::fs::symlink(&precious, &link).unwrap();
        let mut config = scan_config(&[fixture.root()], Budget::default());
        config.cache = Some(link);
        assert!(rdedupe::scan(config, Policy::default(), &Cancellation::default()).is_err());
        assert_eq!(std::fs::read(&precious).unwrap(), b"precious only copy");
    }
    let directory = work.path("dir");
    std::fs::create_dir(&directory).unwrap();
    let mut config = scan_config(&[fixture.root()], Budget::default());
    config.cache = Some(directory.clone());
    assert!(rdedupe::scan(config, Policy::default(), &Cancellation::default()).is_err());
    assert!(directory.is_dir());
}

#[test]
fn cli_refuses_a_source_file_as_cache() {
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    let precious = fixture.write("important.txt", b"precious only copy");
    rdedupe()
        .args(["scan", fixture.str(), "--cache"])
        .arg(&precious)
        .assert()
        .code(4);
    assert_eq!(std::fs::read(&precious).unwrap(), b"precious only copy");
}

/// Starts `rdedupe scan ROOT --cache CACHE` over two huge sparse duplicates and signals it
/// mid-comparison.
fn interrupt_scan(root: &std::path::Path, cache: &std::path::Path, signal: i32) -> Option<i32> {
    let child = std::process::Command::new(assert_cmd::cargo::cargo_bin("rdedupe"))
        .args([
            "scan",
            root.to_str().unwrap(),
            "--format",
            "jsonl",
            "--cache",
        ])
        .arg(cache)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(unsafe { libc::kill(child.id() as libc::pid_t, signal) }, 0);
    child.wait_with_output().unwrap().status.code()
}

fn temporaries(dir: &std::path::Path) -> Vec<std::ffi::OsString> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().contains(".tmp-"))
        .collect()
}

/// A cancelled or killed scan leaves the existing cache byte-for-byte intact and no
/// temporary file behind; a temporary left by a dead writer is removed by the next run,
/// one belonging to a live process is not touched.
#[test]
fn interrupted_scans_leave_the_existing_cache_intact() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let cache = work.path("fingerprints.cache");
    fixture.write("small-a", b"same");
    fixture.write("small-b", b"same");
    scan(fixture.root(), &cache, false, false);
    let before = std::fs::read(&cache).unwrap();
    for name in ["huge-a", "huge-b"] {
        std::fs::File::create(fixture.path(name))
            .unwrap()
            .set_len(8 << 30)
            .unwrap();
    }
    assert_eq!(
        interrupt_scan(fixture.root(), &cache, libc::SIGINT),
        Some(130)
    );
    assert_eq!(
        std::fs::read(&cache).unwrap(),
        before,
        "SIGINT kept the cache"
    );
    assert!(temporaries(work.root()).is_empty());
    assert_eq!(interrupt_scan(fixture.root(), &cache, libc::SIGKILL), None);
    assert_eq!(
        std::fs::read(&cache).unwrap(),
        before,
        "SIGKILL kept the cache"
    );

    // Leftovers: one from an exited process, one from a live one (this test).
    let dead = std::process::Command::new("true").spawn().unwrap();
    let dead_pid = dead.id();
    let mut dead = dead;
    dead.wait().unwrap();
    let stale = work.path(format!("fingerprints.cache.tmp-{dead_pid}-1"));
    let live = work.path(format!("fingerprints.cache.tmp-{}-1", std::process::id()));
    std::fs::write(&stale, b"half-written").unwrap();
    std::fs::write(&live, b"another writer").unwrap();
    std::fs::remove_file(fixture.path("huge-a")).unwrap();
    std::fs::remove_file(fixture.path("huge-b")).unwrap();
    scan(fixture.root(), &cache, false, false);
    assert!(!stale.exists(), "a dead writer's temporary is removed");
    assert_eq!(std::fs::read(&live).unwrap(), b"another writer");
    std::fs::remove_file(&live).unwrap();
    assert!(temporaries(work.root()).is_empty());
}
