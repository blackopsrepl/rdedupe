//! Real FIDEDUPERANGE acceptance. Runs on the filesystem holding the system temporary
//! directory; on filesystems without extent sharing every test prints SKIP with the reason.

mod support;

#[cfg(target_os = "linux")]
mod linux {
    use super::support::*;
    use rdedupe::model::ActionStatus;
    use rdedupe::platform::linux::{dedupe_range, RangeOutcome};
    use rdedupe::{ActionKind, Policy};
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    /// Returns the reason extent sharing is unavailable on `dir`'s filesystem, if it is.
    fn unsupported(dir: &Path) -> Option<String> {
        let a = dir.join(".probe-a");
        let b = dir.join(".probe-b");
        std::fs::write(&a, vec![1u8; 65_536]).unwrap();
        std::fs::write(&b, vec![1u8; 65_536]).unwrap();
        let src = std::fs::File::open(&a).unwrap();
        let dst = std::fs::File::options().write(true).open(&b).unwrap();
        let outcome = dedupe_range(&src, &dst, 0, 65_536);
        std::fs::remove_file(&a).unwrap();
        std::fs::remove_file(&b).unwrap();
        match outcome {
            Ok(RangeOutcome::Shared(_)) => None,
            Ok(other) => Some(format!("probe returned {other:?}")),
            Err(error) => Some(error.to_string()),
        }
    }

    fn shared_extents(path: &Path) -> Option<bool> {
        let output = std::process::Command::new("filefrag")
            .arg("-v")
            .arg(path)
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).contains("shared"))
    }

    #[test]
    fn extent_plan_shares_storage_keeps_identities_and_isolates_writes() {
        let fixture = Fixture::new();
        if let Some(reason) = unsupported(fixture.root()) {
            eprintln!("SKIP: filesystem does not support FIDEDUPERANGE: {reason}");
            return;
        }
        // 40 MiB exceeds the 16 MiB request chunk, so progress spans several ioctls.
        let content: Vec<u8> = (0..40 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let a = fixture.write("a", &content);
        let b = fixture.write("b", &content);
        let c = fixture.write("c", &content[..content.len() - 1]);
        let ino_a = std::fs::metadata(&a).unwrap().ino();
        let ino_b = std::fs::metadata(&b).unwrap().ino();
        assert!(!shared_extents(&b).unwrap_or(false));

        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        let (summary, outcomes) = plan_and_apply(
            &result,
            &work.path("plan"),
            &work.path("journal"),
            ActionKind::Extent,
        );
        assert_eq!(summary.shared, 1);
        assert_eq!(outcomes[0].status, ActionStatus::Shared);
        assert_eq!(outcomes[0].target.to_path().unwrap(), b);
        assert_eq!(outcomes[0].logical_bytes, content.len() as u64);

        // Files stay separate objects with identical bytes; nothing was deleted or linked.
        assert_eq!(std::fs::metadata(&a).unwrap().ino(), ino_a);
        assert_eq!(std::fs::metadata(&b).unwrap().ino(), ino_b);
        assert_eq!(std::fs::metadata(&b).unwrap().nlink(), 1);
        assert_eq!(std::fs::read(&a).unwrap(), content);
        assert_eq!(std::fs::read(&b).unwrap(), content);
        assert!(c.exists());
        if let Some(shared) = shared_extents(&b) {
            assert!(
                shared,
                "filefrag reports no shared extents after FIDEDUPERANGE"
            );
        }

        // Copy-on-write: writing the target leaves the keeper's bytes untouched.
        let file = std::fs::File::options().write(true).open(&b).unwrap();
        std::os::unix::fs::FileExt::write_at(&file, b"independent", 1024).unwrap();
        drop(file);
        assert_eq!(std::fs::read(&a).unwrap(), content);

        // Journal recorded progress and completion.
        let mut states = Vec::new();
        rdedupe::journal::for_each(&work.path("journal"), |e| {
            states.push((e.state, e.bytes_completed));
            Ok(())
        })
        .unwrap();
        use rdedupe::journal::State::*;
        assert_eq!(states.first().unwrap().0, Prepared);
        assert_eq!(*states.last().unwrap(), (Committed, content.len() as u64));
        assert!(states.iter().filter(|(s, _)| *s == Applied).count() >= 3);
    }

    #[test]
    fn kernel_refuses_to_share_differing_ranges() {
        let fixture = Fixture::new();
        if let Some(reason) = unsupported(fixture.root()) {
            eprintln!("SKIP: filesystem does not support FIDEDUPERANGE: {reason}");
            return;
        }
        let a = fixture.write("a", &vec![5u8; 131_072]);
        let mut other = vec![5u8; 131_072];
        other[70_000] = 6;
        let b = fixture.write("b", &other);
        let src = std::fs::File::open(&a).unwrap();
        let dst = std::fs::File::options().write(true).open(&b).unwrap();
        assert_eq!(
            dedupe_range(&src, &dst, 0, 131_072).unwrap(),
            RangeOutcome::Differs
        );
        assert_eq!(std::fs::read(&b).unwrap(), other);
    }

    #[test]
    fn unsupported_filesystem_fails_without_any_fallback() {
        let shm = Path::new("/dev/shm");
        if !shm.is_dir() || unsupported(shm).is_none() {
            eprintln!("SKIP: no tmpfs without extent sharing at /dev/shm");
            return;
        }
        let fixture = Fixture::new_in(shm);
        let a = fixture.write("a", b"same bytes here");
        let b = fixture.write("b", b"same bytes here");
        let ino_b = std::fs::metadata(&b).unwrap().ino();
        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        let (summary, outcomes) = plan_and_apply(
            &result,
            &work.path("plan"),
            &work.path("journal"),
            ActionKind::Extent,
        );
        assert_eq!((summary.shared, summary.failed), (0, 1));
        assert_eq!(outcomes[0].status, ActionStatus::Failed);
        assert!(outcomes[0]
            .message
            .as_deref()
            .unwrap()
            .contains("FIDEDUPERANGE"));
        assert_eq!(std::fs::read(&a).unwrap(), b"same bytes here");
        assert_eq!(std::fs::read(&b).unwrap(), b"same bytes here");
        assert_eq!(
            std::fs::metadata(&b).unwrap().ino(),
            ino_b,
            "no delete/relink fallback"
        );
        assert!(
            !fixture.path(".rdedupe-quarantine").exists(),
            "no quarantine fallback"
        );
    }
}
