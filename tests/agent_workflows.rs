//! Scripted automation contracts: the decisions an agent following the skill must be able
//! to make from rdedupe's machine interface alone, each verified on disk. These prove the
//! interface supports autonomous operation; they are not themselves evidence of an agent
//! operating autonomously (see docs/agent-operation.md for the real-agent evaluation).

mod support;

use std::collections::BTreeMap;
use support::*;

fn tree(fixture: &Fixture) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![fixture.root().to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                stack.push(path);
            } else {
                out.insert(bytes(&path), std::fs::read(&path).unwrap_or_default());
            }
        }
    }
    out
}

fn photos() -> Fixture {
    let fixture = Fixture::new();
    fixture.write("Photos/2024/beach.jpg", b"beach pixels");
    fixture.write("Photos/2024/dog.jpg", b"dog pixels");
    fixture.write("Downloads/beach (1).jpg", b"beach pixels");
    fixture.write("Downloads/beach (2).jpg", b"beach pixels");
    fixture.write("Downloads/dog.jpg", b"dog pixels");
    fixture.write("Downloads/notes.txt", b"unique notes");
    fixture
}

fn outcomes(stdout: &[u8]) -> (Vec<serde_json::Value>, serde_json::Value) {
    let records = jsonl(stdout);
    let summary = records.last().unwrap().clone();
    (
        records
            .into_iter()
            .filter(|r| r["record"] == "outcome")
            .collect(),
        summary,
    )
}

#[test]
fn report_only_request_changes_nothing() {
    let fixture = photos();
    let before = tree(&fixture);
    let work = Fixture::new();
    let records = scan_jsonl([fixture.root()]);
    assert_eq!(summary(&records)["metrics"]["eligible_copies"], 3);
    // An agent may save a plan to show what *would* happen; that is still report-only.
    rdedupe()
        .args(["plan", fixture.str(), "--output"])
        .arg(work.path("plan"))
        .assert()
        .success();
    rdedupe()
        .args(["recover", "--journal"])
        .arg(work.path("absent"))
        .assert()
        .code(4);
    assert_eq!(
        tree(&fixture),
        before,
        "report-level commands never modify the tree"
    );
}

#[test]
fn authorized_cleanup_preserves_the_protected_library() {
    let fixture = photos();
    let work = Fixture::new();
    let plan = work.path("plan");
    let journal = work.path("journal");
    rdedupe()
        .args(["plan", fixture.str(), "--protect"])
        .arg(fixture.path("Photos"))
        .arg("--output")
        .arg(&plan)
        .assert()
        .success();
    let output = rdedupe()
        .args(["apply", "--force", "--format", "jsonl", "--plan"])
        .arg(&plan)
        .arg("--journal")
        .arg(&journal)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (items, summary) = outcomes(&output.stdout);
    assert_eq!(summary["quarantined"], 3);
    assert!(items.iter().all(|o| o["status"] == "quarantined"));
    for kept in [
        "Photos/2024/beach.jpg",
        "Photos/2024/dog.jpg",
        "Downloads/notes.txt",
    ] {
        assert!(fixture.exists(kept), "{kept}");
    }
    for gone in [
        "Downloads/beach (1).jpg",
        "Downloads/beach (2).jpg",
        "Downloads/dog.jpg",
    ] {
        assert!(!fixture.exists(gone), "{gone}");
    }
    assert!(machine_groups(&scan_jsonl([fixture.root()])).is_empty());
}

#[test]
fn partial_scan_is_detectable_and_blocks_planning() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("SKIP: running as root, permission bits do not deny reads");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let fixture = photos();
    let locked = fixture.write("Downloads/locked.jpg", b"dog pixels");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let scan = rdedupe()
        .args(["scan", fixture.str(), "--format", "jsonl"])
        .output()
        .unwrap();
    let work = Fixture::new();
    let plan = rdedupe()
        .args(["plan", fixture.str(), "--output"])
        .arg(work.path("plan"))
        .output()
        .unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(scan.status.code(), Some(2));
    let records = jsonl(&scan.stdout);
    assert_eq!(summary(&records)["completeness"], "partial");
    assert!(records
        .iter()
        .any(|r| r["record"] == "unresolved" && decode_path(&r["path"]) == locked));
    assert_eq!(plan.status.code(), Some(4));
    assert!(!work.path("plan").exists());
}

#[test]
fn stale_plan_is_reported_per_action_and_leaves_files() {
    let fixture = photos();
    let work = Fixture::new();
    rdedupe()
        .args(["plan", fixture.str(), "--protect"])
        .arg(fixture.path("Photos"))
        .arg("--output")
        .arg(work.path("plan"))
        .assert()
        .success();
    fixture.write("Downloads/dog.jpg", b"new dog edit"); // user edited after planning
    let output = rdedupe()
        .args(["apply", "--force", "--format", "jsonl", "--plan"])
        .arg(work.path("plan"))
        .arg("--journal")
        .arg(work.path("journal"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "partial apply exits 2");
    let (items, summary) = outcomes(&output.stdout);
    assert_eq!(summary["skipped"], 1);
    let stale: Vec<_> = items
        .iter()
        .filter(|o| o["status"] == "skipped_stale")
        .collect();
    assert_eq!(
        decode_path(&stale[0]["target"]),
        fixture.path("Downloads/dog.jpg")
    );
    assert_eq!(fixture.read("Downloads/dog.jpg"), b"new dog edit");
}

#[test]
fn interrupted_run_is_recovered_from_the_journal_alone() {
    let fixture = photos();
    let before = tree(&fixture);
    let work = Fixture::new();
    let journal = work.path("journal");
    rdedupe()
        .args(["plan", fixture.str(), "--output"])
        .arg(work.path("plan"))
        .assert()
        .success();
    rdedupe()
        .args(["apply", "--force", "--plan"])
        .arg(work.path("plan"))
        .arg("--journal")
        .arg(&journal)
        .assert()
        .success();
    // Simulate a crash while a further record was being written.
    let mut torn = std::fs::read(&journal).unwrap();
    torn.extend_from_slice(b"{\"schema\":\"rdedupe-journal-v1\",\"seq");
    std::fs::write(&journal, torn).unwrap();
    let recover = rdedupe()
        .args(["recover", "--format", "jsonl", "--journal"])
        .arg(&journal)
        .output()
        .unwrap();
    assert!(recover.status.success());
    let restore = rdedupe()
        .args(["restore", "--format", "jsonl", "--journal"])
        .arg(&journal)
        .output()
        .unwrap();
    assert!(restore.status.success());
    let records = jsonl(&restore.stdout);
    let summary = records.last().unwrap();
    assert_eq!(summary["restored"], 3);
    assert_eq!(summary["truncated_tail_dropped"], true);
    // Restoring everything leaves exactly the original tree: emptied quarantines are removed.
    assert_eq!(tree(&fixture), before);
}

#[test]
fn purge_needs_its_own_confirmation_and_keeps_last_copies() {
    let fixture = photos();
    let work = Fixture::new();
    let journal = work.path("journal");
    rdedupe()
        .args(["plan", fixture.str(), "--protect"])
        .arg(fixture.path("Photos"))
        .arg("--output")
        .arg(work.path("plan"))
        .assert()
        .success();
    rdedupe()
        .args(["apply", "--force", "--plan"])
        .arg(work.path("plan"))
        .arg("--journal")
        .arg(&journal)
        .assert()
        .success();
    rdedupe()
        .args(["purge", "--journal"])
        .arg(&journal)
        .write_stdin("yes\n")
        .assert()
        .code(3);
    // The dog keeper changes after quarantine: its quarantined copy may be the last one.
    std::fs::write(fixture.path("Photos/2024/dog.jpg"), b"keeper replaced").unwrap();
    let output = rdedupe()
        .args(["purge", "--force", "--format", "jsonl", "--journal"])
        .arg(&journal)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "a refused purge exits 2");
    let summary = jsonl(&output.stdout).last().unwrap().clone();
    assert_eq!(summary["purged"], 2);
    assert_eq!(summary["conflicts"], 1);
}
