//! Shared fixtures and an independent oracle.
//!
//! The oracle never uses rdedupe code: it walks the tree with `std::fs`, collapses hard
//! links to their smallest native path, reads every regular file completely and partitions
//! files into equality classes by direct byte comparison. Production results are compared
//! with it as exact sets of path sets, never as counts.

#![allow(dead_code)]

use assert_cmd::Command;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub type Groups = BTreeSet<BTreeSet<Vec<u8>>>;

pub struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    pub fn new() -> Self {
        Self::new_in(std::env::temp_dir())
    }

    pub fn new_in(base: impl AsRef<Path>) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("rdedupe-test-")
            .tempdir_in(base)
            .expect("create fixture directory");
        let root = std::fs::canonicalize(dir.path()).expect("canonical fixture root");
        Self { _dir: dir, root }
    }

    /// Canonical root; every path rdedupe reports is under it.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self, relative: impl AsRef<Path>) -> PathBuf {
        self.root.join(relative)
    }

    pub fn write(&self, relative: impl AsRef<Path>, bytes: &[u8]) -> PathBuf {
        let path = self.path(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        std::fs::write(&path, bytes).expect("write fixture file");
        path
    }

    /// Writes a file whose final name is arbitrary native bytes.
    pub fn write_raw_name(&self, directory: &str, name: &[u8], bytes: &[u8]) -> PathBuf {
        let parent = self.path(directory);
        std::fs::create_dir_all(&parent).expect("create parent");
        let path = parent.join(OsStr::from_bytes(name));
        std::fs::write(&path, bytes).expect("write raw-name file");
        path
    }

    pub fn hard_link(&self, existing: impl AsRef<Path>, new: impl AsRef<Path>) -> PathBuf {
        let target = self.path(new);
        std::fs::hard_link(self.path(existing), &target).expect("hard link");
        target
    }

    pub fn symlink(&self, pointee: impl AsRef<Path>, link: impl AsRef<Path>) -> PathBuf {
        let target = self.path(link);
        std::os::unix::fs::symlink(pointee, &target).expect("symlink");
        target
    }

    pub fn read(&self, relative: impl AsRef<Path>) -> Vec<u8> {
        std::fs::read(self.path(relative)).expect("read fixture file")
    }

    pub fn exists(&self, relative: impl AsRef<Path>) -> bool {
        std::fs::symlink_metadata(self.path(relative)).is_ok()
    }

    pub fn str(&self) -> &str {
        self.root.to_str().expect("utf-8 fixture root")
    }
}

/// Whether the filesystem holding the temporary directory accepts names that are not valid
/// UTF-8 (Linux filesystems do; APFS on macOS rejects them with EILSEQ).
pub fn raw_names_supported() -> bool {
    let fixture = Fixture::new();
    std::fs::write(fixture.root().join(OsStr::from_bytes(b"probe-\xff")), b"").is_ok()
}

pub fn bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

pub fn from_bytes(bytes: Vec<u8>) -> PathBuf {
    PathBuf::from(std::ffi::OsString::from_vec(bytes))
}

/// Builds an expected group from paths.
pub fn group<P: AsRef<Path>>(paths: impl IntoIterator<Item = P>) -> BTreeSet<Vec<u8>> {
    paths.into_iter().map(|p| bytes(p.as_ref())).collect()
}

pub fn rdedupe() -> Command {
    Command::cargo_bin("rdedupe").expect("rdedupe binary")
}

/// The groups the independent oracle finds under `roots`, skipping rdedupe-owned
/// quarantine directories (those holding the marker file).
pub fn oracle(roots: &[&Path]) -> Groups {
    let mut by_inode: BTreeMap<(u64, u64), Vec<u8>> = BTreeMap::new();
    let mut stack: Vec<PathBuf> = roots.iter().map(|p| p.to_path_buf()).collect();
    while let Some(path) = stack.pop() {
        let metadata = std::fs::symlink_metadata(&path).expect("oracle metadata");
        if metadata.is_dir() {
            if path.join("RDEDUPE-QUARANTINE").exists() {
                continue;
            }
            for entry in std::fs::read_dir(&path).expect("oracle read_dir") {
                stack.push(entry.expect("oracle entry").path());
            }
        } else if metadata.is_file() {
            let name = bytes(&path);
            by_inode
                .entry((metadata.dev(), metadata.ino()))
                .and_modify(|current| {
                    if name < *current {
                        *current = name.clone();
                    }
                })
                .or_insert(name);
        }
    }
    let mut classes: Vec<(Vec<u8>, BTreeSet<Vec<u8>>)> = Vec::new();
    for path in by_inode.into_values() {
        let content = std::fs::read(from_bytes(path.clone())).expect("oracle read");
        match classes.iter_mut().find(|(c, _)| *c == content) {
            Some((_, members)) => {
                members.insert(path);
            }
            None => classes.push((content, BTreeSet::from([path]))),
        }
    }
    classes
        .into_iter()
        .map(|(_, members)| members)
        .filter(|members| members.len() > 1)
        .collect()
}

pub fn decode_path(value: &Value) -> PathBuf {
    assert_eq!(value["encoding"], "unix-bytes", "path encoding");
    from_bytes(hex::decode(value["data"].as_str().expect("hex path")).expect("hex"))
}

pub fn jsonl(stdout: &[u8]) -> Vec<Value> {
    stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("machine output line is JSON"))
        .collect()
}

/// Groups from machine output, as native path bytes.
pub fn machine_groups(records: &[Value]) -> Groups {
    let mut groups: BTreeMap<u64, BTreeSet<Vec<u8>>> = BTreeMap::new();
    for record in records.iter().filter(|r| r["record"] == "member") {
        groups
            .entry(record["group"].as_u64().unwrap())
            .or_default()
            .insert(bytes(&decode_path(&record["path"])));
    }
    groups.into_values().collect()
}

pub fn summary(records: &[Value]) -> &Value {
    let first = records.first().expect("summary record");
    assert_eq!(first["record"], "summary");
    first
}

/// Runs `rdedupe scan ARGS --format jsonl`, requires success, returns parsed records.
pub fn scan_jsonl<S: AsRef<OsStr>>(args: impl IntoIterator<Item = S>) -> Vec<Value> {
    let output = rdedupe()
        .arg("scan")
        .args(args)
        .args(["--format", "jsonl"])
        .output()
        .expect("run rdedupe");
    assert!(
        output.status.success(),
        "scan failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = jsonl(&output.stdout);
    assert_eq!(records.last().unwrap()["record"], "end");
    records
}

pub fn library_scan(roots: &[&Path], policy: rdedupe::Policy) -> rdedupe::ScanResult {
    library_scan_with(
        roots,
        policy,
        rdedupe::Budget::default(),
        &rdedupe::fingerprint::SampleFingerprinter,
    )
}

pub fn library_scan_with(
    roots: &[&Path],
    policy: rdedupe::Policy,
    budget: rdedupe::Budget,
    fingerprinter: &dyn rdedupe::fingerprint::Fingerprinter,
) -> rdedupe::ScanResult {
    rdedupe::pipeline::scan_with(
        scan_config(roots, budget),
        policy,
        &rdedupe::scheduler::Cancellation::default(),
        fingerprinter,
    )
    .expect("library scan")
}

pub fn scan_config(roots: &[&Path], budget: rdedupe::Budget) -> rdedupe::ScanConfig {
    rdedupe::ScanConfig {
        roots: roots.iter().map(|p| p.to_path_buf()).collect(),
        pattern: String::new(),
        one_file_system: false,
        budget,
        cache: None,
        fresh: false,
        trust_cache: false,
        progress: false,
    }
}

/// The smallest budget the engine accepts.
pub fn minimum_budget() -> rdedupe::Budget {
    rdedupe::Budget {
        memory_bytes: 8 * 1024 * 1024,
        buffer_bytes: 4096,
        readers: 1,
        descriptors: rdedupe::config::FIXED_DESCRIPTORS + 1,
        temp_bytes: 16 * 1024 * 1024,
        scratch_dir: None,
        queue_rows: *rdedupe::config::QUEUE_ROWS.start(),
    }
}

/// Groups held in a scan result's store, as native path bytes.
pub fn store_groups(result: &rdedupe::ScanResult) -> Groups {
    let mut groups = Groups::new();
    result
        .store
        .for_each_group(|group| {
            let mut members = BTreeSet::new();
            result.store.for_each_member(group, |member| {
                members.insert(bytes(&member.path));
                Ok(())
            })?;
            groups.insert(members);
            Ok(())
        })
        .expect("iterate groups");
    groups
}

/// Plan + apply through the library; returns the apply summary and every outcome.
pub fn plan_and_apply(
    result: &rdedupe::ScanResult,
    plan: &Path,
    journal: &Path,
    kind: rdedupe::ActionKind,
) -> (
    rdedupe::model::ApplySummary,
    Vec<rdedupe::model::ActionOutcome>,
) {
    rdedupe::plan::write_plan(result, plan, kind).expect("write plan");
    apply(plan, journal)
}

pub fn apply(
    plan: &Path,
    journal: &Path,
) -> (
    rdedupe::model::ApplySummary,
    Vec<rdedupe::model::ActionOutcome>,
) {
    let mut outcomes = Vec::new();
    let summary = rdedupe::delete::execute(
        plan,
        journal,
        64 * 1024,
        &rdedupe::scheduler::Cancellation::default(),
        |outcome| {
            outcomes.push(outcome.clone());
            Ok(())
        },
    )
    .expect("apply plan");
    (summary, outcomes)
}

/// Rewrites a plan's lines and recomputes its footer, checksum and seal with this user's
/// key: it simulates a tamperer who holds the key, so structural validation must still
/// reject the edit on its own.
pub fn rewrite_plan(plan: &Path, edit: impl FnOnce(&mut Vec<Value>)) {
    rewrite_plan_with(plan, edit, true)
}

/// Like [`rewrite_plan`], but keeps the original seal (a tamperer without the key who
/// still recomputes the public checksum).
pub fn rewrite_plan_unsealed(plan: &Path, edit: impl FnOnce(&mut Vec<Value>)) {
    rewrite_plan_with(plan, edit, false)
}

fn rewrite_plan_with(plan: &Path, edit: impl FnOnce(&mut Vec<Value>), reseal: bool) {
    let text = std::fs::read_to_string(plan).expect("read plan");
    let mut lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("plan line"))
        .collect();
    let old_footer = lines.pop().expect("footer");
    edit(&mut lines);
    let mut hash = blake3::Hasher::new();
    let mut output = Vec::new();
    for value in &lines {
        let line = serde_json::to_vec(value).unwrap();
        hash.update(&line);
        hash.update(b"\n");
        output.extend(line);
        output.push(b'\n');
    }
    let checksum = hash.finalize().to_hex().to_string();
    let seal = if reseal {
        rdedupe::plan::seal_for(&checksum).expect("seal")
    } else {
        old_footer["seal"].as_str().unwrap().to_string()
    };
    let footer = serde_json::json!({
        "schema": "rdedupe-plan-footer-v1",
        "actions": lines.len() - 1,
        "checksum": checksum,
        "seal": seal,
    });
    output.extend(serde_json::to_vec(&footer).unwrap());
    output.push(b'\n');
    std::fs::write(plan, output).expect("write tampered plan");
}

pub fn encoded(path: &Path) -> Value {
    serde_json::to_value(rdedupe::paths::EncodedPath::from_path(path)).unwrap()
}

/// Generates a seeded tree designed to stress candidate elimination: many files share
/// sizes, and a third differ from a shared base by one byte, often outside the sampled
/// fingerprint windows.
pub fn seeded_tree(fixture: &Fixture, seed: u64, files: usize) {
    let mut rng = StdRng::seed_from_u64(seed);
    let sizes = [0usize, 1, 7, 4096, 65_535, 65_536, 65_537, 200_000, 300_001];
    let mut bases: Vec<Vec<u8>> = Vec::new();
    for _ in 0..12 {
        let size = sizes[rng.gen_range(0..sizes.len())];
        let mut content = vec![0u8; size];
        rng.fill(&mut content[..]);
        bases.push(content);
    }
    for index in 0..files {
        let mut content = bases[rng.gen_range(0..bases.len())].clone();
        if !content.is_empty() && rng.gen_bool(0.33) {
            let position = if content.len() > 3 * 65_536 + 2 {
                65_536 + rng.gen_range(1..32)
            } else {
                rng.gen_range(0..content.len())
            };
            content[position] ^= 1 + rng.gen_range(0..254u8);
        }
        let directory = format!("d{}", rng.gen_range(0..4));
        fixture.write(format!("{directory}/f{index:04}"), &content);
    }
}
