//! `--descriptors` is enforced by the kernel, not only estimated. This lowers the limit of
//! the whole test process, so it lives alone in its own test binary.

#[test]
fn the_descriptor_budget_is_enforced_as_the_process_limit() {
    let dir = std::env::temp_dir();
    rdedupe::platform::limit_open_files(24).unwrap();
    let mut files = Vec::new();
    let mut refused = None;
    for _ in 0..64 {
        match std::fs::File::open(&dir) {
            Ok(file) => files.push(file),
            Err(error) => {
                refused = Some(error);
                break;
            }
        }
    }
    let error = refused.expect("opening past the budget must fail");
    assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
    assert!(files.len() < 24);
    // Never raised: asking for more leaves the lower limit in place.
    drop(files);
    rdedupe::platform::limit_open_files(4096).unwrap();
    let mut again = Vec::new();
    while let Ok(file) = std::fs::File::open(&dir) {
        again.push(file);
        assert!(again.len() < 24, "the limit was raised");
    }
}
