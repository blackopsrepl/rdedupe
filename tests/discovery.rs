mod support;

use std::os::unix::ffi::OsStrExt;
use support::*;

#[test]
fn native_byte_names_round_trip_through_every_output() {
    let fixture = Fixture::new();
    let names: [&[u8]; 9] = [
        "Foto_\u{e8}.jpg".as_bytes(),
        "gpj.\u{202e}exe".as_bytes(),
        b"\xff",
        b"\xfe-x",
        b"new\nline",
        b"esc\x1b[31m",
        b"quote\"'",
        b"back\\slash",
        b"plain",
    ];
    let raw = raw_names_supported();
    if !raw {
        eprintln!("SKIP (partial): this filesystem rejects names that are not valid UTF-8");
    }
    let names: Vec<&[u8]> = names
        .into_iter()
        .filter(|n| raw || std::str::from_utf8(n).is_ok())
        .collect();
    let paths: Vec<_> = names
        .iter()
        .map(|name| fixture.write_raw_name("n", name, b"same content"))
        .collect();
    let records = scan_jsonl([fixture.root()]);
    assert_eq!(machine_groups(&records), Groups::from([group(&paths)]));
    assert_eq!(machine_groups(&records), oracle(&[fixture.root()]));

    // NUL output: exactly the eligible copies (all but the keeper), as raw native bytes.
    let nul = rdedupe()
        .args(["scan", fixture.str(), "--format", "nul"])
        .output()
        .unwrap();
    assert!(nul.status.success());
    let listed: std::collections::BTreeSet<Vec<u8>> = nul
        .stdout
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty())
        .map(|p| p.to_vec())
        .collect();
    let keeper = paths.iter().map(|p| bytes(p)).min().unwrap();
    let mut expected = group(&paths);
    expected.remove(&keeper);
    assert_eq!(listed, expected);

    // Human output escapes every control byte; one line per member.
    let human = rdedupe().args(["scan", fixture.str()]).output().unwrap();
    assert!(human.status.success());
    assert!(
        !human.stdout.contains(&0x1b),
        "raw ESC reached the terminal"
    );
    let text = String::from_utf8(human.stdout).expect("human output is UTF-8");
    assert!(text.contains("new\\nline"));
    assert!(!raw || text.contains("\\xff"));
    assert!(
        text.contains("Foto_\u{e8}.jpg"),
        "valid UTF-8 stays readable"
    );
    assert!(
        text.contains("gpj.\\u{202e}exe"),
        "bidi override is escaped"
    );
    assert!(!text.contains('\u{202e}'));
    assert_eq!(
        text.lines()
            .filter(|l| l.starts_with("  KEEP") || l.starts_with("  COPY"))
            .count(),
        names.len()
    );
}

#[test]
fn hard_links_are_aliases_not_reclaimable_copies() {
    let fixture = Fixture::new();
    let a = fixture.write("a", b"same");
    fixture.hard_link("a", "alias");
    let copy = fixture.write("copy", b"same");
    let result = library_scan(&[fixture.root()], rdedupe::Policy::default());
    assert_eq!(store_groups(&result), Groups::from([group([&a, &copy])]));
    let m = &result.summary.metrics;
    assert_eq!(
        (m.paths, m.objects, m.aliases, m.eligible_copies),
        (3, 2, 1, 1)
    );
}

#[test]
fn overlapping_and_repeated_roots_inventory_each_path_once() {
    let fixture = Fixture::new();
    let a = fixture.write("a", b"x1");
    let b = fixture.write("sub/b", b"x1");
    let sub = fixture.path("sub");
    let records = scan_jsonl([fixture.root(), sub.as_path(), fixture.root()]);
    assert_eq!(machine_groups(&records), Groups::from([group([&a, &b])]));
    assert_eq!(summary(&records)["metrics"]["paths"], 2);
}

#[test]
fn symlinks_are_never_followed() {
    let outside = Fixture::new();
    outside.write("dup-1", b"outside");
    outside.write("dup-2", b"outside");
    let fixture = Fixture::new();
    let real = fixture.write("real", b"inside");
    fixture.symlink(&real, "link-to-file");
    fixture.symlink(outside.root(), "link-to-dir");
    let records = scan_jsonl([fixture.root()]);
    assert!(machine_groups(&records).is_empty());
    let metrics = &summary(&records)["metrics"];
    assert_eq!(metrics["paths"], 1);
    assert_eq!(metrics["symlinks_skipped"], 2);
}

#[test]
fn a_symlink_root_is_resolved_once() {
    let fixture = Fixture::new();
    let a = fixture.write("dir/a", b"same");
    let b = fixture.write("dir/b", b"same");
    fixture.symlink(fixture.path("dir"), "root-link");
    let records = scan_jsonl([fixture.path("root-link")]);
    assert_eq!(machine_groups(&records), Groups::from([group([&a, &b])]));
}

#[test]
fn special_files_are_skipped_without_blocking() {
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    let fifo = std::ffi::CString::new(fixture.path("fifo").as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let records = scan_jsonl([fixture.root()]);
    assert_eq!(summary(&records)["metrics"]["special_files_skipped"], 1);
    assert_eq!(machine_groups(&records).len(), 1);
}

#[test]
fn unreadable_file_makes_the_scan_partial() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("SKIP: running as root, permission bits do not deny reads");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    fixture.write("a", b"same");
    fixture.write("b", b"same");
    let locked = fixture.write("locked", b"same");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let output = rdedupe()
        .args(["scan", fixture.str(), "--format", "jsonl"])
        .output()
        .unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(output.status.code(), Some(2), "partial scans exit 2");
    let records = jsonl(&output.stdout);
    assert_eq!(summary(&records)["completeness"], "partial");
    let unresolved: Vec<_> = records
        .iter()
        .filter(|r| r["record"] == "unresolved")
        .collect();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(decode_path(&unresolved[0]["path"]), locked);
}

#[test]
fn unreadable_directory_is_unresolved() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("SKIP: running as root, permission bits do not deny traversal");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    fixture.write("locked/inner", b"x");
    let locked = fixture.path("locked");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let output = rdedupe()
        .args(["scan", fixture.str(), "--format", "jsonl"])
        .output()
        .unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(output.status.code(), Some(2));
    let records = jsonl(&output.stdout);
    assert!(records
        .iter()
        .any(|r| r["record"] == "unresolved" && decode_path(&r["path"]) == locked));
}

#[test]
fn empty_tree_is_complete_with_no_groups() {
    let fixture = Fixture::new();
    let records = scan_jsonl([fixture.root()]);
    assert!(machine_groups(&records).is_empty());
    assert_eq!(summary(&records)["completeness"], "complete");
    assert_eq!(records.last().unwrap()["groups"], 0);
}

#[test]
fn hidden_ancestors_and_lookalike_quarantine_dirs_are_scanned() {
    let fixture = Fixture::new();
    let a = fixture.write(".hidden/a", b"same");
    // A user directory that merely shares the quarantine name has no valid marker.
    let b = fixture.write(".rdedupe-quarantine/b", b"same");
    fixture.write(".rdedupe-quarantine/RDEDUPE-QUARANTINE", b"forged marker");
    let records = scan_jsonl([fixture.root()]);
    let marker = fixture.path(".rdedupe-quarantine/RDEDUPE-QUARANTINE");
    assert_eq!(machine_groups(&records), Groups::from([group([&a, &b])]));
    assert_eq!(
        summary(&records)["metrics"]["paths"],
        3,
        "{marker:?} is an ordinary file"
    );
}

#[test]
fn pattern_matches_full_path_substrings() {
    let fixture = Fixture::new();
    let a = fixture.write("photos/a.jpg", b"same");
    let b = fixture.write("backup/b.jpg", b"same");
    fixture.write("backup/c.txt", b"same");
    let records = scan_jsonl([
        fixture.root().as_os_str(),
        "--pattern".as_ref(),
        ".jpg".as_ref(),
    ]);
    assert_eq!(machine_groups(&records), Groups::from([group([&a, &b])]));
    let records = scan_jsonl([
        fixture.root().as_os_str(),
        "--pattern".as_ref(),
        "backup/".as_ref(),
    ]);
    assert_eq!(
        machine_groups(&records),
        Groups::from([group([
            fixture.path("backup/b.jpg"),
            fixture.path("backup/c.txt")
        ])])
    );
}

/// `--one-file-system` stops at mount points; without it the walk descends into them.
/// Needs a mount point below a scannable root: `/dev/shm` (its own tmpfs) under `/dev`.
#[test]
fn mount_policy_is_explicit() {
    use std::os::unix::fs::MetadataExt;
    let (parent, mount) = (
        std::path::Path::new("/dev"),
        std::path::Path::new("/dev/shm"),
    );
    let (Ok(p), Ok(m)) = (std::fs::metadata(parent), std::fs::metadata(mount)) else {
        eprintln!("SKIP: no /dev/shm mount point on this system");
        return;
    };
    if p.dev() == m.dev() {
        eprintln!("SKIP: /dev/shm is not a separate mount here");
        return;
    }
    let Ok(fixture) = std::panic::catch_unwind(|| Fixture::new_in(mount)) else {
        eprintln!("SKIP: /dev/shm is not writable");
        return;
    };
    fixture.write("a", b"on another filesystem");
    fixture.write("b", b"on another filesystem");
    let name = fixture
        .root()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let paths = |extra: &[&str]| {
        let mut args = vec!["/dev", "--pattern", name.as_str()];
        args.extend_from_slice(extra);
        let records = scan_jsonl(args);
        (
            summary(&records)["metrics"]["paths"].as_u64().unwrap(),
            machine_groups(&records).len(),
        )
    };
    assert_eq!(paths(&[]), (2, 1), "descends into the mount by default");
    assert_eq!(
        paths(&["--one-file-system"]),
        (0, 0),
        "--one-file-system stays on the root's filesystem"
    );
}

/// Hard-link aliases are named in every format, next to the object they belong to, and are
/// never listed as copies to act on.
#[test]
fn hard_link_aliases_are_named_in_every_output() {
    let fixture = Fixture::new();
    let a = fixture.write("a", b"same bytes");
    let link = fixture.hard_link("a", "a_link");
    let b = fixture.write("b", b"same bytes");
    let records = scan_jsonl([fixture.str()]);
    let alias: Vec<_> = records.iter().filter(|r| r["record"] == "alias").collect();
    assert_eq!(alias.len(), 1, "{records:?}");
    assert_eq!(decode_path(&alias[0]["path"]), link);
    assert_eq!(decode_path(&alias[0]["alias_of"]), a);
    assert_eq!(machine_groups(&records), Groups::from([group([&a, &b])]));

    let csv = rdedupe()
        .args(["scan", fixture.str(), "--format", "csv"])
        .output()
        .unwrap();
    let text = String::from_utf8(csv.stdout).unwrap();
    assert!(text.lines().next().unwrap().ends_with(",alias_of_hex"));
    let row = text
        .lines()
        .find(|l| l.contains("a_link"))
        .expect("alias row");
    assert!(row.ends_with(&hex::encode(bytes(&a))), "{row}");

    let human = rdedupe().args(["scan", fixture.str()]).output().unwrap();
    let human = String::from_utf8(human.stdout).unwrap();
    assert!(
        human.contains("LINK ") && human.contains("a_link"),
        "{human}"
    );

    let nul = rdedupe()
        .args(["scan", fixture.str(), "--format", "nul"])
        .output()
        .unwrap();
    let listed: Vec<_> = nul
        .stdout
        .split(|c| *c == 0)
        .filter(|p| !p.is_empty())
        .collect();
    assert_eq!(listed, [bytes(&b).as_slice()], "only the eligible copy");
}
