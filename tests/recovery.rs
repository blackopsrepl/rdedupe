mod support;

use rdedupe::journal::{JournalEntry, JournalHooks};
use rdedupe::recovery::RecoverySummary;
use rdedupe::scheduler::Cancellation;
use rdedupe::{ActionKind, Policy};
use std::cell::Cell;
use std::path::{Path, PathBuf};
use support::*;

const CONTENT: &[u8] = b"precious content";

/// a is the keeper; b and c are quarantine targets (two actions, three records each).
struct Case {
    fixture: Fixture,
    work: Fixture,
}

impl Case {
    fn new() -> Self {
        let fixture = Fixture::new();
        for name in ["a", "b", "c"] {
            fixture.write(name, CONTENT);
        }
        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Quarantine).unwrap();
        Self { fixture, work }
    }
    fn plan(&self) -> PathBuf {
        self.work.path("plan")
    }
    fn journal(&self) -> PathBuf {
        self.work.path("journal")
    }
    fn present(&self) -> Vec<&'static str> {
        ["a", "b", "c"]
            .into_iter()
            .filter(|n| self.fixture.exists(n))
            .collect()
    }
    fn assert_all_restored(&self) {
        for name in ["a", "b", "c"] {
            assert_eq!(
                self.fixture.read(name),
                CONTENT,
                "{name} content after recovery"
            );
        }
    }
    fn restore(&self) -> RecoverySummary {
        rdedupe::recovery::restore(&self.journal(), |_| {}).expect("restore")
    }
    fn purge(&self) -> RecoverySummary {
        rdedupe::recovery::purge(&self.journal(), 65_536, &Cancellation::default(), |_| {})
            .expect("purge")
    }
    fn quarantine_paths(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        rdedupe::journal::for_each(&self.journal(), |entry| {
            if let Some(q) = &entry.quarantine {
                let q = q.to_path()?;
                if !out.contains(&q) {
                    out.push(q);
                }
            }
            Ok(())
        })
        .unwrap();
        out
    }
}

#[test]
fn quarantine_restore_and_purge_reconcile_actual_files() {
    let case = Case::new();
    let (summary, _) = apply(&case.plan(), &case.journal());
    assert_eq!(summary.quarantined, 2);
    assert_eq!(case.present(), vec!["a"]);
    for q in case.quarantine_paths() {
        assert_eq!(std::fs::read(q).unwrap(), CONTENT);
    }
    let restored = case.restore();
    assert_eq!(
        (restored.restored, restored.conflicts, restored.missing),
        (2, 0, 0)
    );
    case.assert_all_restored();
    let again = case.restore();
    assert_eq!(
        (again.restored, again.already_in_place),
        (0, 2),
        "restore is idempotent"
    );
    assert!(
        !case.fixture.exists(".rdedupe-quarantine"),
        "an emptied quarantine is removed"
    );

    // A second cycle: apply a fresh plan, then purge.
    let result = library_scan(&[case.fixture.root()], Policy::default());
    let plan2 = case.work.path("plan2");
    let journal2 = case.work.path("journal2");
    plan_and_apply(&result, &plan2, &journal2, ActionKind::Quarantine);
    let purged =
        rdedupe::recovery::purge(&journal2, 65_536, &Cancellation::default(), |_| {}).unwrap();
    assert_eq!((purged.purged, purged.conflicts), (2, 0));
    assert_eq!(case.present(), vec!["a"]);
    assert_eq!(case.fixture.read("a"), CONTENT);
    let again =
        rdedupe::recovery::purge(&journal2, 65_536, &Cancellation::default(), |_| {}).unwrap();
    assert_eq!((again.purged, again.missing), (0, 2), "purge is idempotent");
}

#[test]
fn restore_never_overwrites_a_reoccupied_original_path() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    case.fixture.write("b", b"new user data");
    let summary = case.restore();
    assert_eq!((summary.restored, summary.conflicts), (1, 1));
    assert_eq!(case.fixture.read("b"), b"new user data");
    assert_eq!(case.fixture.read("c"), CONTENT);
    let held: Vec<_> = case
        .quarantine_paths()
        .into_iter()
        .filter(|q| q.exists())
        .collect();
    assert_eq!(held.len(), 1, "the displaced copy stays in quarantine");
    assert_eq!(std::fs::read(&held[0]).unwrap(), CONTENT);
    assert!(
        rdedupe::quarantine::is_owned_quarantine_dir(&case.fixture.path(".rdedupe-quarantine")),
        "a quarantine still holding objects keeps its marker"
    );
    // The quarantined copy is not rescanned; only the restored pair is a group.
    assert_eq!(
        machine_groups(&scan_jsonl([case.fixture.root()])),
        Groups::from([group([case.fixture.path("a"), case.fixture.path("c")])])
    );
}

#[test]
fn altered_or_missing_quarantine_objects_are_reported_not_restored() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let q = case.quarantine_paths();
    std::fs::remove_file(&q[0]).unwrap();
    std::fs::write(&q[0], b"impostor").unwrap();
    std::fs::remove_file(&q[1]).unwrap();
    let summary = case.restore();
    assert_eq!(
        (summary.restored, summary.conflicts, summary.missing),
        (0, 1, 1)
    );
    assert!(!case.fixture.exists("b") && !case.fixture.exists("c"));
    let purge = case.purge();
    assert_eq!(purge.purged, 0, "purge never removes an altered object");
    assert_eq!(std::fs::read(&q[0]).unwrap(), b"impostor");
}

#[test]
fn purge_refuses_when_the_keeper_changed_or_vanished() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    std::fs::write(case.fixture.path("a"), b"keeper rewritten").unwrap();
    let summary = case.purge();
    assert_eq!((summary.purged, summary.conflicts), (0, 2));
    std::fs::remove_file(case.fixture.path("a")).unwrap();
    let summary = case.purge();
    assert_eq!((summary.purged, summary.conflicts), (0, 2));
    for q in case.quarantine_paths() {
        assert_eq!(std::fs::read(q).unwrap(), CONTENT, "last copies survive");
    }
    assert_eq!(case.restore().restored, 2);
}

#[test]
fn a_locked_journal_rejects_concurrent_recovery() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let held = rdedupe::journal::Journal::open_existing(&case.journal()).unwrap();
    rdedupe()
        .args(["restore", "--journal"])
        .arg(case.journal())
        .assert()
        .code(4);
    assert!(rdedupe::journal::Journal::create(&case.journal()).is_err());
    drop(held);
    assert_eq!(case.restore().restored, 2);
}

#[test]
fn torn_final_record_is_dropped_but_interior_corruption_stops_everything() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let clean = std::fs::read(case.journal()).unwrap();
    let mut torn = clean.clone();
    torn.extend_from_slice(b"{\"schema\":\"rdedupe-jour");
    std::fs::write(case.journal(), &torn).unwrap();
    let summary = case.restore();
    assert!(summary.truncated_tail_dropped);
    assert_eq!(summary.restored, 2);
    case.assert_all_restored();
    assert!(!std::fs::read(case.journal())
        .unwrap()
        .ends_with(b"rdedupe-jour"));

    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let text = String::from_utf8(std::fs::read(case.journal()).unwrap()).unwrap();
    let mut lines: Vec<&str> = text.lines().collect();
    lines.insert(2, "{not json");
    std::fs::write(case.journal(), lines.join("\n") + "\n").unwrap();
    assert!(rdedupe::recovery::restore(&case.journal(), |_| {}).is_err());
    assert_eq!(
        case.present(),
        vec!["a"],
        "corruption must not trigger moves"
    );
}

struct FailAfter(Cell<u32>);

impl JournalHooks for FailAfter {
    fn after_sync(&self, _: &JournalEntry) -> rdedupe::Result<()> {
        let left = self.0.get();
        if left == 1 {
            return Err(rdedupe::Error::Journal(
                "injected journal write failure".into(),
            ));
        }
        self.0.set(left - 1);
        Ok(())
    }
}

#[test]
fn journal_failure_stops_execution_before_the_mutation() {
    let case = Case::new();
    let result = rdedupe::delete::execute_with_hooks(
        &case.plan(),
        &case.journal(),
        65_536,
        &Cancellation::default(),
        &FailAfter(Cell::new(1)),
        |_| Ok(()),
    );
    assert!(matches!(result, Err(rdedupe::Error::Journal(_))));
    assert_eq!(case.present(), vec!["a", "b", "c"]);
    let mut records = 0;
    rdedupe::journal::for_each(&case.journal(), |_| {
        records += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(records, 1, "only the first intent was recorded");
}

/// Child half of the crash tests: when RDEDUPE_TEST_CRASH_AT is set, apply the plan and
/// abort the process right after the given durable journal record.
#[test]
fn crash_child_process() {
    let Ok(at) = std::env::var("RDEDUPE_TEST_CRASH_AT") else {
        return;
    };
    struct Abort(Cell<u32>);
    impl JournalHooks for Abort {
        fn after_sync(&self, _: &JournalEntry) -> rdedupe::Result<()> {
            let left = self.0.get() - 1;
            if left == 0 {
                std::process::abort();
            }
            self.0.set(left);
            Ok(())
        }
    }
    let plan = PathBuf::from(std::env::var("RDEDUPE_TEST_PLAN").unwrap());
    let journal = PathBuf::from(std::env::var("RDEDUPE_TEST_JOURNAL").unwrap());
    let _ = rdedupe::delete::execute_with_hooks(
        &plan,
        &journal,
        65_536,
        &Cancellation::default(),
        &Abort(Cell::new(at.parse().unwrap())),
        |_| Ok(()),
    );
    std::process::exit(0);
}

fn crash_at(case: &Case, record: u32) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_child_process",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("RDEDUPE_TEST_CRASH_AT", record.to_string())
        .env("RDEDUPE_TEST_PLAN", case.plan())
        .env("RDEDUPE_TEST_JOURNAL", case.journal())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "child must die at record {record}");
}

fn inspections(journal: &Path) -> Vec<String> {
    let mut out = Vec::new();
    rdedupe::recovery::inspect(journal, |item| {
        out.push(item.assessment.to_string());
        Ok(())
    })
    .unwrap();
    out
}

#[test]
fn process_crash_at_every_durable_boundary_is_recoverable() {
    // Records: 1 prepared(b) 2 applied(b) 3 committed(b) 4 prepared(c) 5 applied(c) 6 committed(c)
    let expected_present = [
        vec!["a", "b", "c"],
        vec!["a", "c"],
        vec!["a", "c"],
        vec!["a", "c"],
        vec!["a"],
        vec!["a"],
    ];
    for record in 1..=6u32 {
        let case = Case::new();
        crash_at(&case, record);
        assert_eq!(
            case.present(),
            expected_present[record as usize - 1],
            "after crash at record {record}"
        );
        assert!(inspections(&case.journal())
            .iter()
            .all(|a| !a.starts_with("indeterminate")));
        let first = case.restore();
        assert!(first.clean(), "record {record}: {first:?}");
        case.assert_all_restored();
        let second = case.restore();
        assert_eq!(
            second.restored, 0,
            "record {record}: recovery is idempotent"
        );
        case.assert_all_restored();
    }
}

#[test]
fn crash_between_rename_and_applied_record_is_restorable() {
    let case = Case::new();
    crash_at(&case, 1);
    // Reproduce the state where the rename completed but its record never became durable.
    let destination = case.quarantine_paths()[0].clone();
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    std::fs::rename(case.fixture.path("b"), &destination).unwrap();
    assert_eq!(
        inspections(&case.journal()),
        vec!["moved but not recorded as applied (interrupted); restorable"]
    );
    assert_eq!(case.restore().restored, 1);
    case.assert_all_restored();
}

#[test]
fn interrupted_link_then_unlink_move_is_restored_by_dropping_the_extra_name() {
    let case = Case::new();
    crash_at(&case, 1);
    // The portable move links the destination before unlinking the original; reproduce a
    // crash between the two steps.
    let destination = case.quarantine_paths()[0].clone();
    std::fs::hard_link(case.fixture.path("b"), &destination).unwrap();
    assert_eq!(
        inspections(&case.journal()),
        vec!["interrupted move left the object under both names; restore removes the extra name"]
    );
    assert_eq!(
        case.purge().purged,
        0,
        "purge never unlinks a still-linked original"
    );
    assert!(destination.exists());
    assert_eq!(case.restore().restored, 1);
    assert!(!destination.exists());
    case.assert_all_restored();
}

#[test]
fn recover_command_reports_states_and_cli_restore_reports_conflicts() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let output = rdedupe()
        .args(["recover", "--format", "jsonl", "--journal"])
        .arg(case.journal())
        .output()
        .unwrap();
    assert!(output.status.success());
    let records = jsonl(&output.stdout);
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|r| r["last_state"] == "committed" && r["location"] == "in_quarantine"));
    case.fixture.write("c", b"reoccupied");
    rdedupe()
        .args(["restore", "--journal"])
        .arg(case.journal())
        .assert()
        .code(2);
    assert_eq!(case.fixture.read("b"), CONTENT);
    assert_eq!(case.fixture.read("c"), b"reoccupied");
}

#[test]
fn unwritable_parent_refuses_the_move_without_side_effects() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("SKIP: running as root, directory permissions do not deny writes");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let case = Case::new();
    std::fs::set_permissions(case.fixture.root(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let (summary, _) = apply(&case.plan(), &case.journal());
    std::fs::set_permissions(case.fixture.root(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(summary.quarantined, 0);
    assert_eq!(case.present(), vec!["a", "b", "c"]);
}
