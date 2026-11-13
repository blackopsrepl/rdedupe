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

    /// Cancels the run right after the first per-chunk progress record is durable.
    struct CancelAfterFirstChunk(rdedupe::scheduler::Cancellation);

    impl rdedupe::journal::JournalHooks for CancelAfterFirstChunk {
        fn after_sync(&self, entry: &rdedupe::journal::JournalEntry) -> rdedupe::Result<()> {
            if entry.state == rdedupe::journal::State::Applied {
                self.0.cancel();
            }
            Ok(())
        }
    }

    #[test]
    fn interrupted_sharing_is_recorded_partially_and_resume_completes_it() {
        let fixture = Fixture::new();
        if let Some(reason) = unsupported(fixture.root()) {
            eprintln!("SKIP: filesystem does not support FIDEDUPERANGE: {reason}");
            return;
        }
        let content: Vec<u8> = (0..40 * 1024 * 1024).map(|i| (i % 241) as u8).collect();
        let a = fixture.write("a", &content);
        let b = fixture.write("b", &content);
        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Extent).unwrap();
        let cancel = rdedupe::scheduler::Cancellation::default();
        let outcome = rdedupe::delete::execute_in(
            &work.path("plan"),
            &work.path("journal"),
            &rdedupe::delete::ApplyOptions::new(65_536),
            &cancel,
            &CancelAfterFirstChunk(cancel.clone()),
            |_| Ok(()),
        );
        assert!(matches!(outcome, Err(rdedupe::Error::Cancelled)));
        let mut last = None;
        rdedupe::journal::for_each(&work.path("journal"), |e| {
            last = Some((e.state, e.bytes_completed));
            Ok(())
        })
        .unwrap();
        let (state, done) = last.unwrap();
        assert_eq!(state, rdedupe::journal::State::Applied);
        assert!(
            done > 0 && done < content.len() as u64,
            "partial progress recorded"
        );
        assert_eq!(std::fs::read(&a).unwrap(), content);
        assert_eq!(std::fs::read(&b).unwrap(), content);

        let mut options = rdedupe::delete::ApplyOptions::new(65_536);
        options.resume = true;
        let summary = rdedupe::delete::execute_in(
            &work.path("plan"),
            &work.path("journal"),
            &options,
            &Default::default(),
            &rdedupe::journal::NoHooks,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(summary.shared, 1);
        assert_eq!(std::fs::read(&b).unwrap(), content);
        assert_ne!(
            std::fs::metadata(&a).unwrap().ino(),
            std::fs::metadata(&b).unwrap().ino()
        );
    }

    #[test]
    fn keeper_and_target_on_different_filesystems_are_refused_untouched() {
        let fixture = Fixture::new();
        let shm = Path::new("/dev/shm");
        if unsupported(fixture.root()).is_some() || !shm.is_dir() {
            eprintln!("SKIP: needs extent sharing here and a separate /dev/shm filesystem");
            return;
        }
        let other = Fixture::new_in(shm);
        if std::fs::metadata(fixture.root()).unwrap().dev()
            == std::fs::metadata(other.root()).unwrap().dev()
        {
            eprintln!("SKIP: /dev/shm is on the same filesystem");
            return;
        }
        let a = fixture.write("a", b"identical across filesystems");
        let b = other.write("b", b"identical across filesystems");
        let (ino_a, ino_b) = (
            std::fs::metadata(&a).unwrap().ino(),
            std::fs::metadata(&b).unwrap().ino(),
        );
        let work = Fixture::new();
        let result = library_scan(&[fixture.root(), other.root()], Policy::default());
        let (summary, outcomes) = plan_and_apply(
            &result,
            &work.path("plan"),
            &work.path("journal"),
            ActionKind::Extent,
        );
        assert_eq!(summary.shared, 0);
        assert_eq!(outcomes[0].status, ActionStatus::SkippedStale);
        assert!(outcomes[0]
            .message
            .as_deref()
            .unwrap()
            .contains("different filesystems"));
        assert_eq!(std::fs::read(&a).unwrap(), b"identical across filesystems");
        assert_eq!(std::fs::read(&b).unwrap(), b"identical across filesystems");
        assert_eq!(std::fs::metadata(&a).unwrap().ino(), ino_a);
        assert_eq!(std::fs::metadata(&b).unwrap().ino(), ino_b);
    }

    #[test]
    fn concurrent_writes_during_sharing_are_never_lost() {
        let fixture = Fixture::new();
        if let Some(reason) = unsupported(fixture.root()) {
            eprintln!("SKIP: filesystem does not support FIDEDUPERANGE: {reason}");
            return;
        }
        let content: Vec<u8> = (0..48 * 1024 * 1024).map(|i| (i % 239) as u8).collect();
        let a = fixture.write("a", &content);
        let b = fixture.write("b", &content);
        let work = Fixture::new();
        let result = library_scan(&[fixture.root()], Policy::default());
        rdedupe::plan::write_plan(&result, &work.path("plan"), ActionKind::Extent).unwrap();
        // A writer stamps 0xFF into one byte of every 1 MiB of the target while the
        // kernel compares and shares ranges.
        let target = b.clone();
        let writer = std::thread::spawn(move || {
            let file = std::fs::File::options().write(true).open(&target).unwrap();
            for block in 0..48u64 {
                std::os::unix::fs::FileExt::write_at(&file, &[0xFF], block * 1024 * 1024 + 7)
                    .unwrap();
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
        let outcome = rdedupe::delete::execute(
            &work.path("plan"),
            &work.path("journal"),
            65_536,
            &Default::default(),
            |_| Ok(()),
        );
        writer.join().unwrap();
        // Whatever the kernel decided per range (shared before a write, differing after),
        // the keeper is untouched and every write landed in the target.
        let _ = outcome;
        let mut expected = content.clone();
        for block in 0..48usize {
            expected[block * 1024 * 1024 + 7] = 0xFF;
        }
        assert_eq!(std::fs::read(&a).unwrap(), content, "keeper unchanged");
        assert_eq!(std::fs::read(&b).unwrap(), expected, "no write lost");
    }
}
