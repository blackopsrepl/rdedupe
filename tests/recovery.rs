mod support;

use rdedupe::journal::{JournalEntry, JournalHooks};
use rdedupe::model::ActionStatus;
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
    let hooks = Abort(Cell::new(at.parse().unwrap()));
    if let Ok(limit) = std::env::var("RDEDUPE_TEST_FILE_SIZE_LIMIT") {
        // A file-size limit makes the journal write fail part-way, exactly like a full
        // disk: the write returns an error (EFBIG instead of ENOSPC) after a prefix of the
        // record may have reached the file. The limit is installed once the first record
        // is durable, i.e. after the plan was verified and its index built, so it is the
        // journal that runs into it.
        let limit: libc::rlim_t = limit.parse().unwrap();
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        }
        struct LimitAfterFirstRecord(libc::rlim_t, Cell<bool>);
        impl JournalHooks for LimitAfterFirstRecord {
            fn after_sync(&self, _: &JournalEntry) -> rdedupe::Result<()> {
                if !self.1.replace(true) {
                    let rl = libc::rlimit {
                        rlim_cur: self.0,
                        rlim_max: self.0,
                    };
                    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &rl) }, 0);
                }
                Ok(())
            }
        }
        let result = match std::env::var("RDEDUPE_TEST_OPERATION").as_deref() {
            // restore and purge only append to the journal: limit it from the start.
            Ok(operation @ ("restore" | "purge")) => {
                let rl = libc::rlimit {
                    rlim_cur: limit,
                    rlim_max: limit,
                };
                assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &rl) }, 0);
                if operation == "restore" {
                    rdedupe::recovery::restore(&journal, |_| {}).map(|_| ())
                } else {
                    rdedupe::recovery::purge(&journal, 65_536, &Cancellation::default(), |_| {})
                        .map(|_| ())
                }
            }
            _ => rdedupe::delete::execute_with_hooks(
                &plan,
                &journal,
                65_536,
                &Cancellation::default(),
                &LimitAfterFirstRecord(limit, Cell::new(false)),
                |_| Ok(()),
            )
            .map(|_| ()),
        };
        let code = match result {
            Err(error) if !error.retry_safe() => 3,
            Err(error) => {
                eprintln!("retry-safe failure: {error}");
                4
            }
            Ok(_) => 0,
        };
        std::process::exit(code);
    }
    let _ = match std::env::var("RDEDUPE_TEST_OPERATION").as_deref() {
        Ok("restore") => {
            rdedupe::recovery::restore_with_hooks(&journal, &hooks, |_| {}).map(|_| ())
        }
        Ok("purge") => rdedupe::recovery::purge_with_hooks(
            &journal,
            65_536,
            &Cancellation::default(),
            &hooks,
            |_| {},
        )
        .map(|_| ()),
        Ok("resume") => {
            let mut options = rdedupe::delete::ApplyOptions::new(65_536);
            options.resume = true;
            rdedupe::delete::execute_in(
                &plan,
                &journal,
                &options,
                &Cancellation::default(),
                &hooks,
                |_| Ok(()),
            )
            .map(|_| ())
        }
        _ => rdedupe::delete::execute_with_hooks(
            &plan,
            &journal,
            65_536,
            &Cancellation::default(),
            &hooks,
            |_| Ok(()),
        )
        .map(|_| ()),
    };
    std::process::exit(0);
}

fn crash_at(case: &Case, record: u32) {
    crash_during(case, "apply", record);
}

fn crash_during(case: &Case, operation: &str, record: u32) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_child_process",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("RDEDUPE_TEST_CRASH_AT", record.to_string())
        .env("RDEDUPE_TEST_OPERATION", operation)
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

/// Renames the keeper away right after the first intent is durable: the move then happens,
/// the post-move keeper check fails, and the target must be put back.
struct MoveKeeperAway(PathBuf);

impl JournalHooks for MoveKeeperAway {
    fn after_sync(&self, entry: &JournalEntry) -> rdedupe::Result<()> {
        if entry.sequence == 1 {
            std::fs::rename(&self.0, self.0.with_extension("moved")).unwrap();
        }
        Ok(())
    }
}

#[test]
fn keeper_vanishing_during_the_move_puts_the_target_back() {
    let case = Case::new();
    let mut outcomes = Vec::new();
    let summary = rdedupe::delete::execute_with_hooks(
        &case.plan(),
        &case.journal(),
        65_536,
        &Cancellation::default(),
        &MoveKeeperAway(case.fixture.path("a")),
        |outcome| {
            outcomes.push(outcome.clone());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(summary.quarantined, 0);
    assert_eq!(
        outcomes[0].status,
        rdedupe::model::ActionStatus::SkippedStale
    );
    assert!(outcomes[0]
        .message
        .as_deref()
        .unwrap()
        .contains("moved back"));
    assert_eq!(case.fixture.read("b"), CONTENT);
    assert_eq!(case.fixture.read("c"), CONTENT);
    assert!(!case.fixture.exists(".rdedupe-quarantine"));
    let mut states = Vec::new();
    rdedupe::journal::for_each(&case.journal(), |entry| {
        states.push(entry.state);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        states,
        vec![
            rdedupe::journal::State::Prepared,
            rdedupe::journal::State::Restored
        ]
    );
}

/// Rewrites one journal line (as JSON) in place, keeping its seal.
fn edit_journal_line(journal: &Path, index: usize, edit: impl FnOnce(&mut serde_json::Value)) {
    let text = std::fs::read_to_string(journal).unwrap();
    let mut lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    edit(&mut lines[index]);
    let mut out = String::new();
    for line in lines {
        out.push_str(&serde_json::to_string(&line).unwrap());
        out.push('\n');
    }
    std::fs::write(journal, out).unwrap();
}

#[test]
fn edited_journals_are_refused_and_never_purge_other_files() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let elsewhere = Fixture::new();
    let victim = elsewhere.write("victim", CONTENT);
    let identity = rdedupe::fileinfo::observe_path(&victim).unwrap().identity;
    // The review's attack: point a quarantine path at an unrelated identical file.
    edit_journal_line(&case.journal(), 0, |line| {
        line["quarantine"] = encoded(&victim);
        line["target_observation"]["identity"] = serde_json::to_value(identity).unwrap();
    });
    let error = rdedupe::recovery::purge(&case.journal(), 65_536, &Cancellation::default(), |_| {})
        .expect_err("an edited journal is refused");
    assert!(error.to_string().contains("seal"), "{error}");
    assert!(rdedupe::recovery::restore(&case.journal(), |_| {}).is_err());
    assert_eq!(std::fs::read(&victim).unwrap(), CONTENT);
}

#[test]
fn sealed_records_naming_a_foreign_quarantine_path_are_refused() {
    // Even a record sealed with the user's key is validated: its quarantine path must be
    // exactly the slot apply uses for that action.
    let case = Case::new();
    let elsewhere = Fixture::new();
    let victim = elsewhere.write("victim", CONTENT);
    let observation = rdedupe::fileinfo::observe_path(&victim).unwrap();
    let mut journal = rdedupe::journal::Journal::create(&case.journal()).unwrap();
    journal
        .append(
            JournalEntry {
                schema: String::new(),
                sequence: 0,
                action: 1,
                group: 1,
                state: rdedupe::journal::State::Committed,
                kind: ActionKind::Quarantine,
                target: rdedupe::paths::EncodedPath::from_path(&case.fixture.path("missing")),
                keeper: rdedupe::paths::EncodedPath::from_path(&case.fixture.path("a")),
                quarantine: Some(rdedupe::paths::EncodedPath::from_path(&victim)),
                target_observation: observation,
                bytes_completed: CONTENT.len() as u64,
                message: None,
                seal: String::new(),
            },
            &rdedupe::journal::NoHooks,
        )
        .unwrap();
    drop(journal);
    let error = rdedupe::recovery::purge(&case.journal(), 65_536, &Cancellation::default(), |_| {})
        .expect_err("a foreign quarantine path is refused");
    assert!(error.to_string().contains("quarantine"), "{error}");
    assert_eq!(std::fs::read(&victim).unwrap(), CONTENT);
}

#[test]
fn a_user_directory_named_like_the_quarantine_is_never_taken_over() {
    let fixture = Fixture::new();
    let user_dir = fixture.path(".rdedupe-quarantine");
    std::fs::create_dir(&user_dir).unwrap();
    std::fs::set_permissions(
        &user_dir,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    fixture.write(".rdedupe-quarantine/userfile", b"user data");
    fixture.write("dup-a", CONTENT);
    fixture.write("dup-b", CONTENT);
    let work = Fixture::new();
    let result = library_scan(&[fixture.root()], Policy::default());
    let (summary, outcomes) = plan_and_apply(
        &result,
        &work.path("plan"),
        &work.path("journal"),
        ActionKind::Quarantine,
    );
    assert_eq!(summary.quarantined, 0);
    assert_eq!(
        outcomes[0].status,
        rdedupe::model::ActionStatus::SkippedStale
    );
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&user_dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o755, "permissions untouched");
    assert!(
        !user_dir.join("RDEDUPE-QUARANTINE").exists(),
        "no marker written"
    );
    assert_eq!(fixture.read("dup-b"), CONTENT);
    // The user's directory is still scanned like any other directory.
    let rescan = library_scan(&[fixture.root()], Policy::default());
    assert_eq!(rescan.summary.metrics.objects, 3);
    assert_eq!(rescan.summary.metrics.owned_dirs_skipped, 0);
}

#[test]
fn crash_during_restore_or_purge_is_finished_by_running_it_again() {
    for record in 1..=2u32 {
        let case = Case::new();
        apply(&case.plan(), &case.journal());
        crash_during(&case, "restore", record);
        let again = case.restore();
        assert!(
            again.clean(),
            "restore after a crash at record {record}: {again:?}"
        );
        case.assert_all_restored();

        let case = Case::new();
        apply(&case.plan(), &case.journal());
        crash_during(&case, "purge", record);
        let again = case.purge();
        assert_eq!(again.conflicts, 0, "purge after a crash at record {record}");
        assert_eq!(case.present(), vec!["a"]);
        assert_eq!(case.fixture.read("a"), CONTENT);
        assert!(case.quarantine_paths().iter().all(|q| !q.exists()));
    }
}

#[test]
fn resume_after_a_crash_at_every_durable_boundary_finishes_the_plan() {
    for record in 1..=6u32 {
        let case = Case::new();
        crash_at(&case, record);
        let mut options = rdedupe::delete::ApplyOptions::new(65_536);
        options.resume = true;
        let summary = rdedupe::delete::execute_in(
            &case.plan(),
            &case.journal(),
            &options,
            &Cancellation::default(),
            &rdedupe::journal::NoHooks,
            |_| Ok(()),
        )
        .unwrap_or_else(|e| panic!("resume after record {record}: {e}"));
        assert_eq!(summary.quarantined, 2, "record {record}: {summary:?}");
        assert_eq!(case.present(), vec!["a"], "record {record}");
        for q in case.quarantine_paths() {
            assert_eq!(std::fs::read(q).unwrap(), CONTENT);
        }
        // Resume is idempotent, and everything stays restorable.
        let again = rdedupe::delete::execute_in(
            &case.plan(),
            &case.journal(),
            &options,
            &Cancellation::default(),
            &rdedupe::journal::NoHooks,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(again.quarantined, 2);
        assert!(case.restore().clean());
        case.assert_all_restored();
    }
}

#[test]
fn a_crashed_resume_can_itself_be_resumed() {
    let case = Case::new();
    crash_at(&case, 2);
    crash_during(&case, "resume", 2);
    let mut options = rdedupe::delete::ApplyOptions::new(65_536);
    options.resume = true;
    let summary = rdedupe::delete::execute_in(
        &case.plan(),
        &case.journal(),
        &options,
        &Cancellation::default(),
        &rdedupe::journal::NoHooks,
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(summary.quarantined, 2);
    assert_eq!(case.present(), vec!["a"]);
}

#[test]
fn resume_refuses_a_journal_from_another_plan() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    assert!(case.restore().clean());
    // A different plan over a different tree, resumed with the first plan's journal.
    let other = Case::new();
    let mut options = rdedupe::delete::ApplyOptions::new(65_536);
    options.resume = true;
    let error = rdedupe::delete::execute_in(
        &other.plan(),
        &case.journal(),
        &options,
        &Cancellation::default(),
        &rdedupe::journal::NoHooks,
        |_| Ok(()),
    )
    .expect_err("foreign journal");
    assert!(
        error.to_string().contains("does not belong to this plan"),
        "{error}"
    );
    assert_eq!(other.present(), vec!["a", "b", "c"]);
}

#[test]
fn a_failing_journal_write_stops_before_the_next_mutation_and_restores_cleanly() {
    // Sixty identical files: 59 actions, about 180 journal records (over 100 KiB). A 48 KiB
    // file-size limit makes a journal write fail part-way through the run.
    let fixture = Fixture::new();
    for i in 0..60 {
        fixture.write(format!("f{i:02}"), CONTENT);
    }
    let work = Fixture::new();
    let result = library_scan(&[fixture.root()], Policy::default());
    rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Quarantine).unwrap();
    let index = Fixture::new();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_child_process",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("RDEDUPE_TEST_CRASH_AT", "1000000")
        .env("RDEDUPE_TEST_FILE_SIZE_LIMIT", (48 * 1024).to_string())
        .env("RDEDUPE_TEST_PLAN", work.path("plan"))
        .env("RDEDUPE_TEST_JOURNAL", work.path("journal"))
        .env("TMPDIR", index.root())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&status.stderr).into_owned();
    let status = status.status;
    assert_eq!(
        status.code(),
        Some(3),
        "the run must stop with a not-retry-safe error; child stderr:\n{stderr}"
    );
    let journal_len = std::fs::metadata(work.path("journal")).unwrap().len();
    assert!(journal_len <= 48 * 1024);
    // Some files moved, the rest never did; restore returns every one.
    let summary = rdedupe::recovery::restore(&work.path("journal"), |_| {}).unwrap();
    assert_eq!(summary.conflicts + summary.missing, 0, "{summary:?}");
    assert!(
        summary.restored > 0,
        "the run stopped part-way: {summary:?}"
    );
    for i in 0..60 {
        assert_eq!(fixture.read(format!("f{i:02}")), CONTENT, "f{i:02}");
    }
}

/// Kill at every durable record of an extent action (prepared, per-chunk applied,
/// committed): file contents never change, and `apply --resume` finishes the sharing.
#[cfg(target_os = "linux")]
#[test]
fn crash_at_every_extent_record_keeps_contents_and_resume_finishes() {
    let probe_dir = Fixture::new();
    let extent = rdedupe::probe::filesystem(probe_dir.root()).unwrap().extent;
    if extent != "supported" {
        eprintln!("SKIP: filesystem does not support FIDEDUPERANGE: {extent}");
        return;
    }
    let content: Vec<u8> = (0..40 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    for record in 1..=4u32 {
        let fixture = Fixture::new();
        let a = fixture.write("a", &content);
        let b = fixture.write("b", &content);
        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Extent).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "crash_child_process",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("RDEDUPE_TEST_CRASH_AT", record.to_string())
            .env("RDEDUPE_TEST_OPERATION", "apply")
            .env("RDEDUPE_TEST_PLAN", work.path("plan"))
            .env("RDEDUPE_TEST_JOURNAL", work.path("journal"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        let mut records = 0;
        rdedupe::journal::for_each(&work.path("journal"), |_| {
            records += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            records, record,
            "killed right after record {record}: {status:?}"
        );
        assert_eq!(
            std::fs::read(&a).unwrap(),
            content,
            "record {record}: keeper"
        );
        assert_eq!(
            std::fs::read(&b).unwrap(),
            content,
            "record {record}: target"
        );
        let mut options = rdedupe::delete::ApplyOptions::new(65_536);
        options.resume = true;
        let summary = rdedupe::delete::execute_in(
            &work.path("plan"),
            &work.path("journal"),
            &options,
            &Cancellation::default(),
            &rdedupe::journal::NoHooks,
            |_| Ok(()),
        )
        .unwrap_or_else(|e| panic!("resume after record {record}: {e}"));
        assert_eq!(summary.shared, 1, "record {record}: {summary:?}");
        assert_eq!(std::fs::read(&b).unwrap(), content);
        assert_eq!(std::fs::read(&a).unwrap(), content);
    }
}

/// Runs restore or purge in a child whose journal cannot grow: the first append fails with
/// a real write error (EFBIG) after the filesystem step it records already happened.
fn with_journal_full(case: &Case, operation: &str) -> (Option<i32>, String) {
    let limit = std::fs::metadata(case.journal()).unwrap().len();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_child_process",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("RDEDUPE_TEST_CRASH_AT", "1000000")
        .env("RDEDUPE_TEST_FILE_SIZE_LIMIT", limit.to_string())
        .env("RDEDUPE_TEST_OPERATION", operation)
        .env("RDEDUPE_TEST_PLAN", case.plan())
        .env("RDEDUPE_TEST_JOURNAL", case.journal())
        .output()
        .unwrap();
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn restore_whose_journal_write_fails_is_finished_by_running_it_again() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let (code, stderr) = with_journal_full(&case, "restore");
    assert_eq!(code, Some(3), "not retry-safe: {stderr}");
    // The first object may already be back; nothing is lost or duplicated.
    let again = case.restore();
    assert!(again.clean(), "{again:?}");
    case.assert_all_restored();
}

#[test]
fn purge_whose_journal_write_fails_is_finished_by_running_it_again() {
    let case = Case::new();
    apply(&case.plan(), &case.journal());
    let (code, stderr) = with_journal_full(&case, "purge");
    assert_eq!(code, Some(3), "not retry-safe: {stderr}");
    let again = case.purge();
    assert_eq!(again.conflicts, 0, "{again:?}");
    assert_eq!(case.present(), vec!["a"]);
    assert_eq!(case.fixture.read("a"), CONTENT);
    assert!(case.quarantine_paths().iter().all(|q| !q.exists()));
}

/// Quarantine-directory creation fails part-way through a run (the second directory turns
/// unwritable after the first group was handled): the failing action changes nothing, the
/// earlier ones stay restorable.
#[test]
fn quarantine_directory_creation_failing_mid_run_leaves_everything_restorable() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("SKIP: running as root, directory permissions do not restrict writes");
        return;
    }
    let fixture = Fixture::new();
    fixture.write("one/a", b"first group");
    fixture.write("one/b", b"first group");
    fixture.write("two/a", b"second group");
    fixture.write("two/b", b"second group");
    let work = Fixture::new();
    let result = library_scan(&[fixture.root()], Policy::default());
    rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Quarantine).unwrap();
    struct LockSecondAfterFirst(PathBuf);
    impl JournalHooks for LockSecondAfterFirst {
        fn after_sync(&self, entry: &JournalEntry) -> rdedupe::Result<()> {
            if entry.action == 1 && entry.state == rdedupe::journal::State::Committed {
                std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o555)).unwrap();
            }
            Ok(())
        }
    }
    let mut statuses = Vec::new();
    let mut messages = Vec::new();
    let outcome = rdedupe::delete::execute_with_hooks(
        &work.path("plan"),
        &work.path("journal"),
        65_536,
        &Cancellation::default(),
        &LockSecondAfterFirst(fixture.path("two")),
        |o| {
            statuses.push(o.status);
            messages.push(o.message.clone().unwrap_or_default());
            Ok(())
        },
    );
    std::fs::set_permissions(fixture.path("two"), std::fs::Permissions::from_mode(0o755)).unwrap();
    // The failure happens before the action's intent record: nothing changed, so the
    // action is skipped (not `failed`) and the message carries the real error.
    outcome.unwrap();
    assert_eq!(
        statuses,
        [ActionStatus::Quarantined, ActionStatus::SkippedStale],
        "{messages:?}"
    );
    assert!(messages[1].contains("Permission denied"), "{messages:?}");
    assert!(!fixture.exists("one/b"));
    assert_eq!(fixture.read("two/a"), b"second group");
    assert_eq!(fixture.read("two/b"), b"second group");
    let restored = rdedupe::recovery::restore(&work.path("journal"), |_| {}).unwrap();
    assert_eq!(restored.conflicts + restored.missing, 0, "{restored:?}");
    assert_eq!(fixture.read("one/b"), b"first group");
}
