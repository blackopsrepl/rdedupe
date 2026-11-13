mod support;

use rdedupe::model::ActionStatus;
use rdedupe::{ActionKind, Policy};
use support::*;

struct Planned {
    fixture: Fixture,
    work: Fixture,
}

impl Planned {
    /// a (keeper), b and c are identical; the plan targets b then c.
    fn new() -> Self {
        let fixture = Fixture::new();
        fixture.write("a", b"same-bytes");
        fixture.write("b", b"same-bytes");
        fixture.write("c", b"same-bytes");
        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Quarantine).unwrap();
        Self { fixture, work }
    }

    fn apply(&self) -> Vec<ActionStatus> {
        let (_, outcomes) = apply(&self.work.path("plan"), &self.work.path("journal"));
        outcomes.iter().map(|o| o.status).collect()
    }

    fn all_present(&self) {
        for name in ["a", "b", "c"] {
            assert!(self.fixture.exists(name), "{name} must remain");
        }
    }
}

fn replace(path: &std::path::Path, bytes: &[u8]) {
    let temporary = path.with_extension("replacement");
    std::fs::write(&temporary, bytes).unwrap();
    std::fs::rename(&temporary, path).unwrap();
}

#[test]
fn changed_target_bytes_are_refused() {
    let p = Planned::new();
    let file = std::fs::File::options()
        .write(true)
        .open(p.fixture.path("b"))
        .unwrap();
    std::os::unix::fs::FileExt::write_at(&file, b"X", 0).unwrap();
    assert_eq!(p.apply()[0], ActionStatus::SkippedStale);
    p.all_present();
    assert_eq!(p.fixture.read("b"), b"Xame-bytes");
}

#[test]
fn replaced_target_with_equal_bytes_is_refused() {
    let p = Planned::new();
    replace(&p.fixture.path("b"), b"same-bytes");
    assert_eq!(
        p.apply(),
        vec![ActionStatus::SkippedStale, ActionStatus::SkippedGroup]
    );
    p.all_present();
}

#[test]
fn replaced_target_with_unequal_bytes_of_equal_size_is_refused() {
    let p = Planned::new();
    replace(&p.fixture.path("c"), b"diff-bytes");
    assert_eq!(
        p.apply(),
        vec![ActionStatus::Quarantined, ActionStatus::SkippedStale]
    );
    assert_eq!(p.fixture.read("c"), b"diff-bytes");
    assert!(p.fixture.exists("a"));
}

#[test]
fn replaced_keeper_stops_the_group() {
    let p = Planned::new();
    replace(&p.fixture.path("a"), b"same-bytes");
    assert_eq!(
        p.apply(),
        vec![ActionStatus::SkippedStale, ActionStatus::SkippedGroup]
    );
    p.all_present();
}

#[test]
fn missing_keeper_stops_the_group() {
    let p = Planned::new();
    std::fs::remove_file(p.fixture.path("a")).unwrap();
    assert_eq!(
        p.apply(),
        vec![ActionStatus::SkippedStale, ActionStatus::SkippedGroup]
    );
    assert!(p.fixture.exists("b") && p.fixture.exists("c"));
}

#[test]
fn target_turned_into_symlink_to_keeper_is_refused() {
    let p = Planned::new();
    std::fs::remove_file(p.fixture.path("b")).unwrap();
    p.fixture.symlink(p.fixture.path("a"), "b");
    assert_eq!(p.apply()[0], ActionStatus::SkippedStale);
    assert_eq!(p.fixture.read("a"), b"same-bytes");
}

#[test]
fn ancestor_substituted_by_symlink_is_refused() {
    let fixture = Fixture::new();
    fixture.write("dir/a", b"same");
    fixture.write("dir/b", b"same");
    let work = Fixture::new();
    let result = library_scan(&[fixture.root()], Policy::default());
    rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Quarantine).unwrap();
    std::fs::rename(fixture.path("dir"), fixture.path("moved")).unwrap();
    fixture.write("elsewhere/a", b"same");
    fixture.write("elsewhere/b", b"same");
    fixture.symlink(fixture.path("elsewhere"), "dir");
    let (summary, _) = apply(&work.path("plan"), &work.path("journal"));
    assert_eq!(summary.quarantined, 0);
    for name in ["moved/a", "moved/b", "elsewhere/a", "elsewhere/b"] {
        assert!(fixture.exists(name));
    }
}

#[test]
fn force_never_skips_validation_and_exit_status_reports_partial() {
    let p = Planned::new();
    replace(&p.fixture.path("b"), b"same-bytes");
    rdedupe()
        .args(["apply", "--force", "--format", "jsonl", "--plan"])
        .arg(p.work.path("plan"))
        .arg("--journal")
        .arg(p.work.path("journal"))
        .assert()
        .code(2);
    p.all_present();
}
