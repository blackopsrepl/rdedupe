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

fn main() {
    // `cargo test --all-targets` also runs bench targets; only `cargo bench` passes --bench.
    if !std::env::args().any(|a| a == "--bench") {
        println!("pipeline: run with `cargo bench --bench pipeline` (skipped under cargo test)");
        return;
    }
    let workloads: [(&str, Builder, Budget); 10] = [
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
                ScanConfig {
                    roots: vec![directory.path().to_path_buf()],
                    pattern: String::new(),
                    one_file_system: false,
                    budget: budget.clone(),
                    cache: None,
                    fresh: false,
                    trust_cache: false,
                    progress: false,
                },
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
            "{{\"workload\":\"{name}\",\"repeats\":{REPEATS},\"median_ms\":{:.2},\"min_ms\":{:.2},\"max_ms\":{:.2},\"paths\":{},\"objects\":{},\"groups\":{},\"fingerprint_bytes\":{},\"full_hash_bytes\":{},\"comparison_bytes\":{},\"eliminated_by_size\":{},\"page_cache_bytes\":{}}}",
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
            m.sqlite_page_cache_bytes
        );
    }
}
