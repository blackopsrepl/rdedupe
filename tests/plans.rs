mod support;

use rdedupe::{ActionKind, Policy};
use std::path::Path;
use support::*;

fn planned(fixture: &Fixture, work: &Fixture) -> std::path::PathBuf {
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    fixture.write("c", b"same");
    fixture.write("x1", b"other");
    fixture.write("x2", b"other");
    let result = library_scan(&[fixture.root()], Policy::default());
    let plan = work.path("plan");
    assert_eq!(
        rdedupe::plan::write_plan(&result, &plan, ActionKind::Quarantine).unwrap(),
        3
    );
    plan
}

fn rejected(plan: &Path, needle: &str) {
    let error = rdedupe::plan::verify_plan(plan).expect_err("tampered plan must be rejected");
    assert!(
        error.to_string().contains(needle),
        "{error} does not mention {needle:?}"
    );
    // apply refuses before creating a journal or touching files
    let journal = plan.with_extension("journal");
    assert!(
        rdedupe::delete::execute(plan, &journal, 65_536, &Default::default(), |_| Ok(())).is_err()
    );
    assert!(!journal.exists());
}

#[test]
fn plan_round_trips_exactly_the_eligible_copies() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    let header = rdedupe::plan::verify_plan(&plan).unwrap();
    assert_eq!(header.action, ActionKind::Quarantine);
    let mut targets = std::collections::BTreeSet::new();
    rdedupe::plan::for_each_action(&plan, |_, _, action| {
        targets.insert(bytes(&action.target.to_path()?));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        targets,
        group([fixture.path("b"), fixture.path("c"), fixture.path("x2")])
    );
}

#[test]
fn existing_plan_files_are_never_overwritten() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    let before = std::fs::read(&plan).unwrap();
    let result = library_scan(&[fixture.root()], Policy::default());
    assert!(rdedupe::plan::write_plan(&result, &plan, ActionKind::Quarantine).is_err());
    assert_eq!(std::fs::read(&plan).unwrap(), before);
}

#[test]
fn checksum_mismatch_is_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    let text = std::fs::read_to_string(&plan)
        .unwrap()
        .replace("\"group\":1", "\"group\":9");
    std::fs::write(&plan, text).unwrap();
    rejected(&plan, "checksum");
}

#[test]
fn truncated_plan_and_trailing_data_are_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    let original = std::fs::read(&plan).unwrap();
    std::fs::write(&plan, &original[..original.len() - 1]).unwrap();
    rejected(&plan, "truncated");
    let footerless: Vec<u8> = {
        let text = String::from_utf8(original.clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        (lines[..lines.len() - 1].join("\n") + "\n").into_bytes()
    };
    std::fs::write(&plan, footerless).unwrap();
    rejected(&plan, "missing plan footer");
    let mut trailing = original.clone();
    trailing.extend_from_slice(b"{}\n");
    std::fs::write(&plan, trailing).unwrap();
    rejected(&plan, "after plan footer");
}

#[test]
fn oversized_lines_are_rejected_without_buffering_them() {
    let work = Fixture::new();
    let plan = work.path("plan");
    std::fs::write(&plan, vec![b'x'; 2 * 1024 * 1024]).unwrap();
    rejected(&plan, "maximum length");
}

#[test]
fn duplicate_targets_are_rejected_even_with_a_valid_checksum() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    rewrite_plan(&plan, |lines| {
        let copy = lines[1].clone();
        lines.push(copy);
    });
    rejected(&plan, "duplicate target");
}

#[test]
fn keeper_chains_and_cycles_are_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    rewrite_plan(&plan, |lines| {
        let first = lines[1]["target"].clone();
        let second = lines[2]["target"].clone();
        lines[1]["keeper"] = second;
        lines[2]["keeper"] = first;
    });
    rejected(&plan, "keeper chain or cycle");
}

#[test]
fn out_of_scope_and_dot_dot_paths_are_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    let original = std::fs::read(&plan).unwrap();
    rewrite_plan(&plan, |lines| {
        lines[1]["target"] = encoded(Path::new("/etc/passwd"))
    });
    rejected(&plan, "");
    std::fs::write(&plan, &original).unwrap();
    let escape = fixture.path("../escape");
    rewrite_plan(&plan, |lines| {
        lines[1]["target"] = encoded(&fixture.root().join("..").join("escape"))
    });
    rejected(&plan, "");
    assert!(!escape.exists());
    std::fs::write(&plan, &original).unwrap();
    rewrite_plan(&plan, |lines| {
        lines[1]["target"] = encoded(Path::new("relative/path"))
    });
    rejected(&plan, "");
}

#[test]
fn targets_under_protected_roots_are_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    rewrite_plan(&plan, |lines| {
        lines[0]["protect"] = serde_json::json!([encoded(&fixture.path("b"))]);
    });
    rejected(&plan, "protected");
}

#[test]
fn target_equal_to_keeper_is_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    rewrite_plan(&plan, |lines| {
        lines[1]["keeper"] = lines[1]["target"].clone()
    });
    rejected(&plan, "target equals keeper");
}

#[test]
fn unknown_schema_encoding_and_heuristic_headers_are_rejected() {
    let fixture = Fixture::new();
    let work = Fixture::new();
    let plan = planned(&fixture, &work);
    let original = std::fs::read(&plan).unwrap();
    rewrite_plan(&plan, |lines| {
        lines[0]["schema"] = "rdedupe-plan-v999".into()
    });
    rejected(&plan, "unsupported plan schema");
    std::fs::write(&plan, &original).unwrap();
    rewrite_plan(&plan, |lines| {
        lines[1]["target"]["encoding"] = "ebcdic".into()
    });
    rejected(&plan, "encoding");
    std::fs::write(&plan, &original).unwrap();
    rewrite_plan(&plan, |lines| lines[0]["exactness"] = "heuristic".into());
    rejected(&plan, "heuristic");
    std::fs::write(&plan, &original).unwrap();
    rewrite_plan(&plan, |lines| lines[0]["completeness"] = "partial".into());
    rejected(&plan, "partial");
}

#[test]
fn plans_from_relative_roots_apply_from_any_working_directory() {
    let fixture = Fixture::new();
    fixture.write("tree/a", b"same");
    fixture.write("tree/b", b"same");
    let plan = fixture.path("cwd.plan");
    rdedupe()
        .current_dir(fixture.root())
        .args(["plan", "tree", "--output", "cwd.plan"])
        .assert()
        .success();
    rdedupe()
        .current_dir("/")
        .args(["apply", "--force", "--plan"])
        .arg(&plan)
        .arg("--journal")
        .arg(fixture.path("cwd.journal"))
        .assert()
        .success();
    assert!(fixture.exists("tree/a"));
    assert!(!fixture.exists("tree/b"));
}

#[test]
fn partial_scans_produce_no_plan_file() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("SKIP: running as root, permission bits do not deny reads");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    // Same size as the duplicates, so it must be read; it is unrelated content.
    let locked = fixture.write("unrelated", b"zzzz");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let work = Fixture::new();
    let plan = work.path("plan");
    let output = rdedupe()
        .args(["plan", fixture.str(), "--output"])
        .arg(&plan)
        .output()
        .unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(output.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&output.stderr).contains("partial"));
    assert!(!plan.exists());
}
