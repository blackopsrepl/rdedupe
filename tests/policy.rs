mod support;

use rdedupe::{ActionKind, Policy};
use support::*;

fn keeper_and_targets(result: &rdedupe::ScanResult) -> (Vec<u8>, Vec<Vec<u8>>, String) {
    let (keeper, reason) = result.policy.select_streaming(&result.store, 1).unwrap();
    let mut targets = Vec::new();
    result
        .store
        .for_each_member(1, |m| {
            if m.id != keeper.id && !result.policy.is_protected(&m.path) {
                targets.push(bytes(&m.path));
            }
            Ok(())
        })
        .unwrap();
    (bytes(&keeper.path), targets, reason)
}

#[test]
fn protection_matches_whole_components_keep_is_not_keeper() {
    let fixture = Fixture::new();
    let kept = fixture.write("keep/z", b"same");
    let other = fixture.write("keeper/a", b"same");
    let policy = Policy {
        protect: vec![fixture.path("keep")],
        ..Policy::default()
    };
    let result = library_scan(&[fixture.root()], policy);
    let (keeper, targets, reason) = keeper_and_targets(&result);
    assert_eq!(keeper, bytes(&kept));
    assert_eq!(targets, vec![bytes(&other)]);
    assert_eq!(reason, "protected root");
}

#[test]
fn several_protected_members_all_survive_apply() {
    let fixture = Fixture::new();
    let p1 = fixture.write("protected/one", b"same");
    let p2 = fixture.write("protected/two", b"same");
    let copy = fixture.write("loose/copy", b"same");
    let policy = Policy {
        protect: vec![fixture.path("protected")],
        ..Policy::default()
    };
    let result = library_scan(&[fixture.root()], policy);
    let work = Fixture::new();
    let (summary, outcomes) = plan_and_apply(
        &result,
        &work.path("plan"),
        &work.path("journal"),
        ActionKind::Quarantine,
    );
    assert_eq!(summary.quarantined, 1);
    assert_eq!(outcomes[0].target.to_path().unwrap(), copy);
    assert_eq!(std::fs::read(&p1).unwrap(), b"same");
    assert_eq!(std::fs::read(&p2).unwrap(), b"same");
    assert!(!copy.exists());
}

#[test]
fn protected_hard_link_content_survives_when_its_alias_is_the_member() {
    let fixture = Fixture::new();
    fixture.write("z-protected/data", b"same");
    // The smaller path "a-alias" represents the object, outside the protected root.
    fixture.hard_link("z-protected/data", "a-alias");
    fixture.write("m-copy", b"same");
    let policy = Policy {
        protect: vec![fixture.path("z-protected")],
        ..Policy::default()
    };
    let result = library_scan(&[fixture.root()], policy);
    let work = Fixture::new();
    plan_and_apply(
        &result,
        &work.path("plan"),
        &work.path("journal"),
        ActionKind::Quarantine,
    );
    assert_eq!(fixture.read("z-protected/data"), b"same");
}

#[test]
fn preferred_roots_then_newest_then_native_order() {
    let fixture = Fixture::new();
    let old = fixture.write("a/old", b"same");
    let new = fixture.write("b/new", b"same");
    let preferred = fixture.write("c/preferred", b"same");
    let file = std::fs::File::options().write(true).open(&old).unwrap();
    file.set_modified(
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000),
    )
    .unwrap();
    drop(file);

    let by_path = library_scan(&[fixture.root()], Policy::default());
    let (keeper, _, reason) = keeper_and_targets(&by_path);
    assert_eq!(
        (keeper, reason.as_str()),
        (bytes(&old), "native path order")
    );

    let newest = library_scan(
        &[fixture.root()],
        Policy {
            prefer_newest: true,
            ..Policy::default()
        },
    );
    let (keeper, _, _) = keeper_and_targets(&newest);
    assert_ne!(keeper, bytes(&old));
    assert!(keeper == bytes(&new) || keeper == bytes(&preferred));

    let prefer = library_scan(
        &[fixture.root()],
        Policy {
            prefer: vec![fixture.path("c"), fixture.path("b")],
            prefer_newest: true,
            ..Policy::default()
        },
    );
    let (keeper, targets, reason) = keeper_and_targets(&prefer);
    assert_eq!(
        (keeper, reason.as_str()),
        (bytes(&preferred), "preferred root")
    );
    assert_eq!(targets.len(), 2);
}

#[test]
fn keeper_choice_is_independent_of_scan_order() {
    let fixture = Fixture::new();
    seeded_tree(&fixture, 9, 80);
    let d: Vec<_> = (0..4).map(|i| fixture.path(format!("d{i}"))).collect();
    let a = library_scan(&[&d[0], &d[1], &d[2], &d[3]], Policy::default());
    let b = library_scan(&[&d[3], &d[1], &d[0], &d[2]], Policy::default());
    let mut out_a = Vec::new();
    let mut out_b = Vec::new();
    rdedupe::report::write(&a, rdedupe::report::Format::Csv, &mut out_a).unwrap();
    rdedupe::report::write(&b, rdedupe::report::Format::Csv, &mut out_b).unwrap();
    assert_eq!(out_a, out_b);
}
