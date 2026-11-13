//! Reproducible end-to-end workloads for the scan pipeline (`cargo bench --bench pipeline`).
//!
//! Every workload is generated from a fixed seed into a fresh temporary directory, scanned
//! `REPEATS` times through the public library API, and reported as one JSON line with the
//! median and spread of wall time plus the engine's own byte counters. Process-level peak
//! RSS and before/after comparisons with another binary live in `benches/compare.py`.

use rand::{rngs::StdRng, RngCore, SeedableRng};
use rdedupe::scheduler::Cancellation;
use rdedupe::{scan, Budget, Policy, ScanConfig};
use std::fs::{self, File};
use std::path::Path;
use std::time::Instant;

const REPEATS: usize = 5;

fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill_bytes(&mut out);
    out
}

type Builder = fn(&Path);

fn unique_sizes(root: &Path) {
    for i in 0..1200u64 {
        fs::write(root.join(format!("{i:05}")), bytes(i, 32_768 + i as usize)).unwrap();
    }
}

fn same_size_distinct(root: &Path) {
    for i in 0..1000u64 {
        fs::write(root.join(format!("{i:05}")), bytes(10_000 + i, 65_536)).unwrap();
    }
}

fn shared_prefix(root: &Path) {
    let prefix = bytes(20_000, 32_768);
    for i in 0..1000u64 {
        fs::write(
            root.join(format!("{i:05}")),
            [prefix.clone(), bytes(20_001 + i, 32_768)].concat(),
        )
        .unwrap();
    }
}

fn tiny_duplicates(root: &Path) {
    for i in 0..6000u64 {
        fs::write(root.join(format!("{i:05}")), bytes(i / 3, 80)).unwrap();
    }
}

fn large_duplicates(root: &Path) {
    let large = bytes(30_000, 16 * 1024 * 1024);
    for i in 0..12 {
        fs::write(root.join(format!("{i:05}")), &large).unwrap();
    }
}

fn large_group(root: &Path) {
    for i in 0..5000 {
        fs::write(root.join(format!("{i:05}")), b"one large duplicate group").unwrap();
    }
}

fn hard_links(root: &Path) {
    for i in 0..300u64 {
        let original = root.join(format!("{i:05}"));
        fs::write(&original, bytes(40_000 + i, 65_536)).unwrap();
        fs::hard_link(&original, root.join(format!("alias-{i:05}"))).unwrap();
    }
}

/// 200 files of 300 KiB, identical in every sampled window, each different at offset
/// 100,000: the worst case for sampled fingerprints.
fn sample_collisions(root: &Path) {
    let base = bytes(50_000, 300 * 1024);
    for i in 0..200u32 {
        let mut content = base.clone();
        content[100_000..100_004].copy_from_slice(&i.to_le_bytes());
        fs::write(root.join(format!("{i:05}")), content).unwrap();
    }
}

fn sparse(root: &Path) {
    for (name, len) in [("a", 64 << 20), ("b", 64 << 20), ("c", (64 << 20) + 1)] {
        File::create(root.join(name)).unwrap().set_len(len).unwrap();
    }
}

/// 60,000 small files with only 64 distinct sizes, so most share a size with many others:
/// at a 7 MiB budget the inventory is many times the page cache and every stage pages.
fn large_inventory(root: &Path) {
    for d in 0..60u64 {
        let dir = root.join(format!("d{d:02}"));
        fs::create_dir(&dir).unwrap();
        for i in 0..1000u64 {
            let n = d * 1000 + i;
            // One file in three duplicates its predecessor.
            let seed = if n % 3 == 2 { n - 1 } else { n };
            fs::write(
                dir.join(format!("{i:04}")),
                bytes(seed, 64 + (seed % 64) as usize),
            )
            .unwrap();
        }
    }
}

fn config(root: &Path, budget: Budget) -> ScanConfig {
    ScanConfig {
        roots: vec![root.to_path_buf()],
        pattern: String::new(),
        one_file_system: false,
        budget,
        cache: None,
        fresh: false,
        trust_cache: false,
        progress: false,
    }
}

fn median(mut samples: Vec<f64>) -> (f64, f64, f64) {
    samples.sort_by(f64::total_cmp);
    (
        samples[samples.len() / 2],
        samples[0],
        samples[samples.len() - 1],
    )
}

/// Repeat-scan speed with a fingerprint cache, published separately from fresh
/// verification: `fresh` has no cache, `exact-with-cache` reads a valid cache but (being
/// exact) still reads current content, `trusted-cache` reuses cached fingerprints
/// (heuristic; plans refuse it). Groups are byte-verified in every mode.
fn cache_modes(filter: Option<&str>) {
    let name = "repeat-scan-cache";
    if filter.is_some_and(|f| !name.contains(f)) {
        return;
    }
    let directory = tempfile::tempdir().expect("benchmark fixture");
    let root = directory.path().join("tree");
    fs::create_dir(&root).unwrap();
    same_size_distinct(&root);
    for i in 0..200u64 {
        fs::write(root.join(format!("dup-{i:05}")), bytes(10_000 + i, 65_536)).unwrap();
    }
    let cache = directory.path().join("fingerprints.cache");
    let mut warm = config(&root, Budget::default());
    warm.cache = Some(cache.clone());
    scan(warm, Policy::default(), &Cancellation::default()).expect("cache warm-up");
    for (mode, cache_path, trust) in [
        ("fresh", None, false),
        ("exact-with-cache", Some(cache.clone()), false),
        ("trusted-cache", Some(cache.clone()), true),
    ] {
        let mut samples = Vec::with_capacity(REPEATS);
        let mut last = None;
        for _ in 0..REPEATS {
            let mut c = config(&root, Budget::default());
            c.cache = cache_path.clone();
            c.trust_cache = trust;
            let started = Instant::now();
            let result = scan(c, Policy::default(), &Cancellation::default()).expect("scan");
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
            last = Some(result.summary);
        }
        let (mid, min, max) = median(samples);
        let summary = last.unwrap();
        let m = summary.metrics;
        println!(
            "{{\"workload\":\"{name}\",\"mode\":\"{mode}\",\"exactness\":\"{:?}\",\"repeats\":{REPEATS},\"median_ms\":{mid:.2},\"min_ms\":{min:.2},\"max_ms\":{max:.2},\"paths\":{},\"groups\":{},\"fingerprint_bytes\":{},\"cached_fingerprints_reused\":{},\"comparison_bytes\":{}}}",
            summary.exactness,
            m.paths,
            m.groups,
            m.fingerprint_bytes_read,
            m.cached_fingerprints_reused,
            m.comparison_bytes_read
        );
    }
}

/// Throughput of the digest used for fingerprints (BLAKE3) against xxh3 (a fast
/// non-cryptographic hash, benchmark-only dependency) and the standard library's
/// SipHash-1-3, over the same 64 MiB in 64 KiB windows (the sampling window size).
fn hash_throughput(filter: Option<&str>) {
    use std::hash::Hasher;
    let name = "hash-throughput";
    if filter.is_some_and(|f| !name.contains(f)) {
        return;
    }
    let data = bytes(60_000, 64 << 20);
    let run = |label: &str, digest: &dyn Fn(&[u8]) -> u64| {
        let mut samples = Vec::with_capacity(REPEATS);
        let mut sink = 0u64;
        for _ in 0..REPEATS {
            let started = Instant::now();
            for window in data.chunks(64 << 10) {
                sink ^= digest(window);
            }
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        let (mid, min, max) = median(samples);
        println!(
            "{{\"workload\":\"{name}\",\"hash\":\"{label}\",\"repeats\":{REPEATS},\"median_ms\":{mid:.2},\"min_ms\":{min:.2},\"max_ms\":{max:.2},\"mib_per_s\":{:.0},\"sink\":{sink}}}",
            64.0 / (mid / 1000.0)
        );
    };
    run("blake3", &|w| {
        u64::from_le_bytes(blake3::hash(w).as_bytes()[..8].try_into().unwrap())
    });
    run("xxh3-64", &|w| xxhash_rust::xxh3::xxh3_64(w));
    run("xxh3-128", &|w| xxhash_rust::xxh3::xxh3_128(w) as u64);
    run("siphash-1-3 (std DefaultHasher)", &|w| {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        h.write(w);
        h.finish()
    });
}

fn main() {
    // `cargo test --all-targets` also runs bench targets; only `cargo bench` passes --bench.
    if !std::env::args().any(|a| a == "--bench") {
        println!("pipeline: run with `cargo bench --bench pipeline` (skipped under cargo test)");
        return;
    }
    let workloads: [(&str, Builder, Budget); 11] = [
        ("unique-sizes", unique_sizes, Budget::default()),
        ("same-size-distinct", same_size_distinct, Budget::default()),
        ("shared-prefix", shared_prefix, Budget::default()),
        ("tiny-duplicates", tiny_duplicates, Budget::default()),
        ("large-duplicates", large_duplicates, Budget::default()),
        ("large-group", large_group, Budget::default()),
        ("hard-links", hard_links, Budget::default()),
        ("sparse", sparse, Budget::default()),
        ("sample-collisions", sample_collisions, Budget::default()),
        (
            "tiny-duplicates-8mib-budget",
            tiny_duplicates,
            Budget {
                memory_bytes: 8 << 20,
                buffer_bytes: 64 << 10,
                readers: 1,
                ..Budget::default()
            },
        ),
        (
            "large-inventory-7mib-budget",
            large_inventory,
            Budget {
                memory_bytes: 7 << 20,
                buffer_bytes: 256 << 10,
                readers: 1,
                queue_rows: 64,
                ..Budget::default()
            },
        ),
    ];
    let filter = std::env::args().skip(1).find(|a| !a.starts_with('-'));
    for (name, build, budget) in workloads {
        if filter.as_deref().is_some_and(|f| !name.contains(f)) {
            continue;
        }
        let directory = tempfile::tempdir().expect("benchmark fixture");
        build(directory.path());
        let mut samples = Vec::with_capacity(REPEATS);
        let mut last = None;
        for _ in 0..REPEATS {
            let started = Instant::now();
            let result = scan(
                config(directory.path(), budget.clone()),
                Policy::default(),
                &Cancellation::default(),
            )
            .expect("benchmark scan");
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
            last = Some(result.summary);
        }
        samples.sort_by(f64::total_cmp);
        let m = last.unwrap().metrics;
        println!(
            "{{\"workload\":\"{name}\",\"repeats\":{REPEATS},\"median_ms\":{:.2},\"min_ms\":{:.2},\"max_ms\":{:.2},\"paths\":{},\"objects\":{},\"groups\":{},\"fingerprint_bytes\":{},\"full_hash_bytes\":{},\"comparison_bytes\":{},\"eliminated_by_size\":{},\"page_cache_bytes\":{},\"inventory_bytes\":{}}}",
            samples[REPEATS / 2],
            samples[0],
            samples[REPEATS - 1],
            m.paths,
            m.objects,
            m.groups,
            m.fingerprint_bytes_read,
            m.full_hash_bytes_read,
            m.comparison_bytes_read,
            m.eliminated_by_size,
            m.sqlite_page_cache_bytes,
            m.inventory_bytes
        );
    }
    cache_modes(filter.as_deref());
    hash_throughput(filter.as_deref());
}
