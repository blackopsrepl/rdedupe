mod support;

use predicates::prelude::*;
use std::io::Read;
use std::process::Stdio;
use support::*;

fn duplicates() -> Fixture {
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    fixture.write("c", b"other");
    fixture
}

#[test]
fn version_and_help_come_from_package_metadata() {
    rdedupe()
        .arg("--version")
        .assert()
        .success()
        .stdout(format!("rdedupe {}\n", env!("CARGO_PKG_VERSION")));
    rdedupe()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Exit codes"));
}

#[test]
fn invalid_invocations_exit_64() {
    rdedupe().arg("--no-such-flag").assert().code(64);
    rdedupe().args(["apply", "--plan", "x"]).assert().code(64);
    rdedupe().arg("scan").assert().code(64);
}

#[test]
fn legacy_default_route_reports_groups_on_stdout_only() {
    let fixture = duplicates();
    rdedupe()
        .arg(fixture.root())
        .assert()
        .success()
        .stdout(predicate::str::contains("group 1 (4 bytes each)"))
        .stderr(predicate::str::is_empty());
}

#[test]
fn legacy_pattern_and_csv_routes_still_work() {
    let fixture = duplicates();
    let report = fixture.path("report.csv");
    rdedupe()
        .arg(fixture.root())
        .args(["--pattern", "/a"])
        .arg("--csv")
        .arg(&report)
        .assert()
        .success();
    let text = std::fs::read_to_string(&report).unwrap();
    assert!(text.starts_with("group,path_encoding,path_hex,path_display,"));
    assert_eq!(
        text.lines().count(),
        1,
        "pattern /a leaves no duplicate group"
    );
}

#[test]
fn csv_rows_are_lossless_and_quoted() {
    let fixture = Fixture::new();
    let a = fixture.write_raw_name("", b"comma,\"quote\"\n", b"same");
    let b = fixture.write("b", b"same");
    let report = fixture.path("../report-csv-lossless.csv");
    let _ = std::fs::remove_file(&report);
    rdedupe()
        .args(["scan", fixture.str(), "--format", "csv", "--output"])
        .arg(&report)
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
    let mut reader = csv::Reader::from_path(&report).unwrap();
    let mut paths = std::collections::BTreeSet::new();
    for row in reader.records() {
        let row = row.unwrap();
        assert_eq!(&row[1], "unix-bytes");
        paths.insert(hex::decode(&row[2]).unwrap());
    }
    std::fs::remove_file(&report).unwrap();
    assert_eq!(paths, group([&a, &b]));
}

#[test]
fn reports_never_overwrite_sources_or_follow_links_into_them() {
    let fixture = duplicates();
    fixture.symlink(fixture.path("a"), "link-to-a");
    fixture.hard_link("b", "hardlink-to-b");
    for (target, protected) in [("a", "a"), ("link-to-a", "a"), ("hardlink-to-b", "b")] {
        rdedupe()
            .args(["scan", fixture.str(), "--format", "csv", "--output"])
            .arg(fixture.path(target))
            .assert()
            .code(4)
            .stderr(predicate::str::contains("never overwritten"));
        assert_eq!(
            fixture.read(protected),
            b"same",
            "{target} clobbered {protected}"
        );
        rdedupe()
            .arg(fixture.root())
            .arg("--csv")
            .arg(fixture.path(target))
            .assert()
            .code(4);
        assert_eq!(fixture.read(protected), b"same");
    }
}

#[test]
fn legacy_delete_refuses_and_mutates_nothing() {
    let fixture = duplicates();
    rdedupe()
        .arg(fixture.root())
        .args(["--delete", "--force"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("rdedupe plan"));
    assert!(fixture.exists("a") && fixture.exists("b"));
    rdedupe()
        .arg(fixture.root())
        .args(["--delete", "--dry-run"])
        .assert()
        .success()
        .stderr(predicate::str::contains("nothing was changed"));
    assert!(fixture.exists("a") && fixture.exists("b"));
}

#[test]
fn budget_validation_is_an_execution_failure() {
    let fixture = duplicates();
    rdedupe()
        .args(["scan", fixture.str(), "--descriptors", "1"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("descriptor budget"));
    rdedupe()
        .args(["scan", fixture.str(), "--memory-mib", "1"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("memory budget"));
    rdedupe()
        .args(["scan", fixture.str(), "--buffer-kib", "1"])
        .assert()
        .code(4);
}

#[test]
fn missing_root_is_an_execution_failure() {
    rdedupe()
        .args(["scan", "/nonexistent/rdedupe-root"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("canonicalize root"));
}

#[test]
fn closed_stdout_exits_141_without_an_error_message() {
    let fixture = Fixture::new();
    for i in 0..3000 {
        fixture.write(format!("f{i:05}"), format!("{}", i / 2).as_bytes());
    }
    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("rdedupe"))
        .args(["scan", fixture.str(), "--format", "jsonl"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0u8; 1];
    stdout.read_exact(&mut first).unwrap();
    drop(stdout);
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(141));
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn apply_without_a_terminal_requires_force() {
    let fixture = duplicates();
    let plan = fixture.path("../cli-confirm.plan");
    let _ = std::fs::remove_file(&plan);
    rdedupe()
        .args(["plan", fixture.str(), "--output"])
        .arg(&plan)
        .assert()
        .success();
    rdedupe()
        .args(["apply", "--plan"])
        .arg(&plan)
        .arg("--journal")
        .arg(fixture.path("../cli-confirm.journal"))
        .write_stdin("yes\n")
        .assert()
        .code(3)
        .stderr(predicate::str::contains("pass --force"));
    std::fs::remove_file(&plan).unwrap();
    assert!(fixture.exists("a") && fixture.exists("b"));
    assert!(!fixture.path("../cli-confirm.journal").exists());
}

#[test]
fn progress_goes_to_stderr_and_machine_output_stays_clean() {
    let fixture = duplicates();
    let output = rdedupe()
        .args(["scan", fixture.str(), "--format", "jsonl", "--progress"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("rdedupe: discovered 3 paths"), "{stderr}");
    assert!(stderr.contains("Complete: 1 groups"), "{stderr}");
    let records = jsonl(&output.stdout);
    assert_eq!(records.first().unwrap()["record"], "summary");
    assert_eq!(records.last().unwrap()["record"], "end");
}

#[test]
fn capabilities_lists_formats() {
    rdedupe()
        .arg("capabilities")
        .assert()
        .success()
        .stdout(predicate::str::contains("plan-schema=rdedupe-plan-v1"))
        .stdout(predicate::str::contains(
            "journal-schema=rdedupe-journal-v1",
        ));
}
