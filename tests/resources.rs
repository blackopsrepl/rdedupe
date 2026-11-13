mod support;

use rdedupe::scheduler::Cancellation;
use rdedupe::{Budget, Completeness, Policy};
use std::os::unix::process::CommandExt;
use support::*;

/// Runs the binary with RLIMIT_NOFILE and returns (exit code, stdout, peak RSS in KiB).
fn run_limited(args: &[&std::ffi::OsStr], nofile: u64) -> (i32, Vec<u8>, i64) {
    let (code, stdout, rss, _) = run_measured(args, nofile);
    (code, stdout, rss)
}

/// Like [`run_limited`], also returning the peak number of open descriptors sampled from
/// `/proc/<pid>/fd` every millisecond (Linux; `None` elsewhere).
fn run_measured(args: &[&std::ffi::OsStr], nofile: u64) -> (i32, Vec<u8>, i64, Option<usize>) {
    let mut command = std::process::Command::new(assert_cmd::cargo::cargo_bin("rdedupe"));
    command
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    unsafe {
        command.pre_exec(move || {
            let limit = libc::rlimit {
                rlim_cur: nofile,
                rlim_max: nofile,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let done = done.clone();
        let fd_dir = std::path::PathBuf::from(format!("/proc/{}/fd", child.id()));
        std::thread::spawn(move || {
            let mut peak = None;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                if let Ok(entries) = std::fs::read_dir(&fd_dir) {
                    let n = entries.count();
                    peak = Some(peak.map_or(n, |p: usize| p.max(n)));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            peak
        })
    };
    let mut stdout = Vec::new();
    std::io::Read::read_to_end(child.stdout.as_mut().unwrap(), &mut stdout).unwrap();
    let mut status = 0;
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let pid = child.id() as libc::pid_t;
    assert_eq!(unsafe { libc::wait4(pid, &mut status, 0, &mut usage) }, pid);
    std::mem::forget(child);
    // Linux reports ru_maxrss in KiB, macOS in bytes.
    let max_rss_kib = if cfg!(target_os = "macos") {
        usage.ru_maxrss / 1024
    } else {
        usage.ru_maxrss
    };
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let peak_fds = sampler.join().unwrap();
    (
        libc::WEXITSTATUS(status),
        stdout,
        max_rss_kib as i64,
        peak_fds,
    )
}

#[test]
fn an_inventory_far_larger_than_the_page_cache_stays_exact_and_bounded() {
    // 60,000 paths at the smallest memory budget: the inventory database grows to several
    // times the page cache, so every stage works from paged, on-disk state.
    let fixture = Fixture::new();
    let mut expected = Groups::new();
    for d in 0..60 {
        for i in 0..1000 {
            fixture.write(
                format!("d{d:02}/f{i:04}"),
                format!("unique {d} {i}").as_bytes(),
            );
        }
    }
    for g in 0..200 {
        let members: Vec<_> = (0..3)
            .map(|k| fixture.write(format!("dup/{k}/g{g:03}"), format!("dup {g}").as_bytes()))
            .collect();
        expected.insert(group(&members));
    }
    let args: Vec<&std::ffi::OsStr> = vec![
        "scan".as_ref(),
        fixture.root().as_os_str(),
        "--format".as_ref(),
        "jsonl".as_ref(),
        "--memory-mib".as_ref(),
        "7".as_ref(),
        "--readers".as_ref(),
        "1".as_ref(),
        "--queue".as_ref(),
        "64".as_ref(),
        "--descriptors".as_ref(),
        "16".as_ref(),
    ];
    let (code, stdout, max_rss_kib, peak_fds) = run_measured(&args, 16);
    assert_eq!(code, 0);
    let records = jsonl(&stdout);
    assert_eq!(machine_groups(&records), expected);
    let metrics = &summary(&records)["metrics"];
    assert_eq!(metrics["paths"].as_u64(), Some(60_600));
    let page_cache = metrics["sqlite_page_cache_bytes"].as_u64().unwrap();
    let inventory = metrics["inventory_bytes"].as_u64().unwrap();
    eprintln!(
        "inventory {inventory} bytes, page cache {page_cache} bytes, peak RSS {max_rss_kib} KiB, peak descriptors {peak_fds:?}"
    );
    assert!(
        inventory > 3 * page_cache,
        "inventory {inventory} must exceed the {page_cache} byte page cache several times"
    );
    assert!(
        max_rss_kib < 32 * 1024,
        "peak RSS {max_rss_kib} KiB for a 7 MiB budget"
    );
    if let Some(peak) = peak_fds {
        assert!(peak <= 16, "peak descriptors {peak} above the budget of 16");
    }
}

#[test]
fn queue_depth_is_validated() {
    for queue in ["8", "70000"] {
        let output = rdedupe()
            .args(["scan", ".", "--queue", queue])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(4), "--queue {queue}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("queue depth"));
    }
}

#[test]
fn huge_group_and_large_files_stay_within_descriptor_and_memory_limits() {
    let fixture = Fixture::new();
    for i in 0..4000 {
        fixture.write(format!("tiny/{:02}/f{i:05}", i % 50), b"duplicate payload");
    }
    // Two 256 MiB sparse files: comparing them streams through small buffers.
    for name in ["big-a", "big-b"] {
        let file = std::fs::File::create(fixture.path(name)).unwrap();
        file.set_len(256 * 1024 * 1024).unwrap();
    }
    let args: Vec<&std::ffi::OsStr> = vec![
        "scan".as_ref(),
        fixture.root().as_os_str(),
        "--format".as_ref(),
        "jsonl".as_ref(),
        "--memory-mib".as_ref(),
        "16".as_ref(),
        "--buffer-kib".as_ref(),
        "64".as_ref(),
        "--readers".as_ref(),
        "2".as_ref(),
        "--descriptors".as_ref(),
        "16".as_ref(),
    ];
    let (code, stdout, max_rss_kib) = run_limited(&args, 16);
    assert_eq!(code, 0);
    let records = jsonl(&stdout);
    let groups = machine_groups(&records);
    assert_eq!(groups.len(), 2);
    assert!(groups.iter().any(|g| g.len() == 4000));
    let metrics = &summary(&records)["metrics"];
    assert!(metrics["sqlite_page_cache_bytes"].as_u64().unwrap() <= 16 * 1024 * 1024);
    eprintln!("peak RSS {max_rss_kib} KiB for a 16 MiB working-set budget");
    assert!(
        max_rss_kib < 64 * 1024,
        "peak RSS {max_rss_kib} KiB exceeds 64 MiB"
    );
}

#[test]
fn memory_and_minimum_budgets_produce_identical_reports() {
    let fixture = Fixture::new();
    seeded_tree(&fixture, 77, 200);
    let big = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        Budget::default(),
        &rdedupe::fingerprint::Blake3Fingerprinter,
    );
    let small = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        minimum_budget(),
        &rdedupe::fingerprint::Blake3Fingerprinter,
    );
    let mut a = Vec::new();
    let mut b = Vec::new();
    rdedupe::report::write(&big, rdedupe::report::Format::Csv, &mut a).unwrap();
    rdedupe::report::write(&small, rdedupe::report::Format::Csv, &mut b).unwrap();
    assert_eq!(a, b);
    assert_eq!(store_groups(&small), oracle(&[fixture.root()]));
}

#[test]
fn long_paths_are_handled() {
    let fixture = Fixture::new();
    // Fill most of the platform's PATH_MAX (4096 on Linux, 1024 on macOS).
    let budget = libc::PATH_MAX as usize - fixture.root().as_os_str().len() - 16;
    let components = budget / 104;
    let deep: std::path::PathBuf = (0..components)
        .map(|i| format!("{i:02}-{}", "d".repeat(100)))
        .collect();
    let a = fixture.write(deep.join("a"), b"same");
    let b = fixture.write(deep.join("b"), b"same");
    assert!(a.as_os_str().len() > libc::PATH_MAX as usize * 3 / 4);
    let result = library_scan(&[fixture.root()], Policy::default());
    assert_eq!(store_groups(&result), Groups::from([group([&a, &b])]));
}

#[test]
fn cancellation_yields_a_cancelled_summary_that_cannot_plan() {
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    let cancel = Cancellation::default();
    cancel.cancel();
    let result = rdedupe::scan(
        scan_config(&[fixture.root()], Budget::default()),
        Policy::default(),
        &cancel,
    )
    .unwrap();
    assert_eq!(result.summary.completeness, Completeness::Cancelled);
    let work = Fixture::new();
    assert!(rdedupe::plan::write_plan(
        &result,
        &work.path("plan"),
        rdedupe::ActionKind::Quarantine
    )
    .is_err());
}

#[test]
fn exhausting_temporary_storage_fails_explicitly_and_cleans_up() {
    let fixture = Fixture::new();
    let name = "n".repeat(200);
    for i in 0..30_000 {
        fixture.write(format!("{:03}/{name}{i:05}", i % 100), b"");
    }
    let scratch = Fixture::new();
    let budget = Budget {
        temp_bytes: 16 * 1024 * 1024,
        scratch_dir: Some(scratch.root().to_path_buf()),
        ..Budget::default()
    };
    let error = rdedupe::scan(
        scan_config(&[fixture.root()], budget),
        Policy::default(),
        &Cancellation::default(),
    )
    .err()
    .expect("16 MiB cannot hold this inventory");
    assert!(error.to_string().to_lowercase().contains("full"), "{error}");
    assert_eq!(
        std::fs::read_dir(scratch.root()).unwrap().count(),
        0,
        "scratch workspace removed"
    );
}

#[test]
fn scratch_workspace_is_confined_and_removed() {
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    // The scratch directory lies inside the scanned tree and is still excluded.
    let scratch = fixture.path("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let budget = Budget {
        scratch_dir: Some(scratch.clone()),
        ..Budget::default()
    };
    let result = library_scan_with(
        &[fixture.root()],
        Policy::default(),
        budget,
        &rdedupe::fingerprint::Blake3Fingerprinter,
    );
    assert_eq!(result.summary.metrics.paths, 2);
    assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 1);
    drop(result);
    assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 0);
}
