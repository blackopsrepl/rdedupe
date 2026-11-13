# Formats

All machine formats are UTF-8 JSON Lines: one JSON object per `\n`-terminated line.
Version identifiers are part of every format. Readers must reject unknown versions;
additive fields may appear within a version, and readers must ignore fields they do not
know.

## Native paths

Every path in a machine format is an object:

```json
{"encoding": "unix-bytes", "data": "2f746d702f61"}
```

`data` is the lowercase hex of the exact bytes the kernel uses, so any name (invalid
UTF-8, newlines, control bytes) round-trips. A NUL byte is invalid. Readers on a platform
whose encoding differs reject the path. Display strings (human output, CSV
`path_display`) keep valid UTF-8 readable and escape the rest: `\\` for a backslash;
`\n`, `\r`, `\t`; `\xNN` for other ASCII controls and for bytes that are not valid
UTF-8; `\u{XXXX}` for C1 controls and invisible or bidirectional formatting characters
(such as U+202E, which could disguise a name). They are never parsed back. Every human
line (reports, unresolved messages, apply/restore/purge/recover outcomes and the
`error:` line on stderr) is escaped the same way, except that backslashes in free text
are left as they are, so an OS error message that embeds a raw path cannot inject
terminal control sequences or start a forged line. Machine formats carry messages
unescaped as JSON strings.

## Scan output: `rdedupe-machine-v1`

`summary`, then `member*` (by group, then path), each member immediately followed by its
`alias*` records (other hard-linked paths to the same file, by path), then
`unresolved*` (by path), then `end`. Field lists are in `skills/rdedupe/references/commands.md`. `end` repeats the
group and unresolved counts; a stream without it is truncated.

## CSV

Header: `group,path_encoding,path_hex,path_display,size,allocated,device,inode,links,keeper,protected,reason,alias_of_hex`.
Standard RFC 4180 quoting. `path_hex` is lossless; `path_display` is for people. One row
per group member, each followed by one row per hard-link alias of it; an alias row has
`keeper` false and `alias_of_hex` set to its member's `path_hex` (empty on member rows).

## NUL list

Raw native paths of eligible copies (never keepers, retained members or hard-link
aliases), each followed by one NUL byte: exactly the targets a plan would contain.

## Plans: `rdedupe-plan-v1`

1. Header: `schema`, `binary_version`, `roots` (canonical), `protect`, `prefer`,
   `prefer_newest`, `include_empty`, `completeness` (must be `complete`), `exactness` (must be `exact`),
   `action` (`quarantine` or `extent`).
2. One action per line: `group`, `target`, `target_observation`, `keeper`,
   `keeper_observation`, `reason`. An observation is `identity` (`device`, `inode`),
   `size`, `allocated`, `modified_ns`, `changed_ns`, `mode`, `links`.
3. Footer: `schema: "rdedupe-plan-footer-v1"`, `actions` (count), `checksum` (BLAKE3 hex
   over every preceding line including its newline), `seal` (keyed BLAKE3 hex, key = the
   user's `seal.key`, over the domain `rdedupe-plan-v1`, a NUL byte and the checksum hex).

Validation (`verify_plan`, run again by `apply` on the same open file):

- lines at most 1 MiB, every line newline-terminated, nothing after the footer;
- known schemas and path encoding; complete and exact header;
- every path absolute and byte-identical to its normalized form (no `.`/`..`, `//` or
  trailing `/`), under one of the header's roots;
- no target under a protected root, target ≠ keeper;
- each target appears once; no keeper is also a target (no chains or cycles), checked
  with a disk-backed index so plan size does not bound memory;
- footer count, checksum and seal match.

During execution each action line is matched against the verified index before use, so a
plan modified after verification stops at the first altered action. The checksum detects
corruption; the seal rejects plans not produced by this user's rdedupe. Neither replaces
the live revalidation of every action. The verification index lives under `$TMPDIR`.

## Journals: `rdedupe-journal-v1`

One record per line: `schema`, `sequence` (strictly increasing), `action` (plan action
number), `group`, `state`, `kind` (`quarantine`/`extent`), `target`, `keeper`,
`quarantine` (destination path or null), `target_observation`, `bytes_completed`,
`message`, `seal`. The seal is a keyed BLAKE3 (domain `rdedupe-journal-v1`) over the
previous record's seal, a newline, and this record serialized with an empty `seal`; the
first record chains from the empty string. Sequences must be 1, 2, 3, …; any record that
fails its seal or sequence makes every command refuse the journal.

State machine per action. Recorded states are `prepared`, `applied`, `committed`,
`failed`, `restored` and `purged`:

```
quarantine: prepared → applied → committed        (or prepared → failed,
                                                   or prepared → restored if the keeper
                                                   changed during the move)
extent:     prepared → applied(bytes)* → committed (or … → failed)
recovery:   … → restored | purged
```

`indeterminate` is never written. It is the assessment `recover`, `restore`, `purge`
and `apply --resume` reach when the last recorded state and the filesystem facts do not
identify where the object is (for example the quarantine slot holds a different object,
or the original object is gone and its path reoccupied). Recovery never infers success
from a record alone: it locates the journaled object by identity and creation time (or
size and modification time where no creation time exists) at its original path and in
its quarantine slot, and an indeterminate action stops the command with exit 2 and an
explanation instead of acting.

Durability ordering for a quarantine action: create the quarantine directory and marker
(synced); append+fsync `prepared`; rename without replacement; fsync both directories;
append+fsync `applied`; append+fsync `committed`. An extent action appends+fsyncs
`prepared`, then `applied` with `bytes_completed` after every shared chunk (16 MiB), then
`committed`.

### What survives a crash and what survives power loss

- **Process crash** (`kill -9`, panic, OOM kill): tested. `tests/recovery.rs` kills a
  child process after every durable record of apply (quarantine and extent), restore,
  purge and resume, and checks file contents after recovery. The guarantees hold on any
  filesystem, because a crash loses no data the kernel accepted.
- **Write and sync failures** (disk full, quota, I/O error): tested where a real failure
  can be produced without a dedicated filesystem.
  - **Journal write failures:** a file-size limit (`RLIMIT_FSIZE`) makes journal writes
    fail with a real `EFBIG`, the same failed-`write` path as `ENOSPC`. This is tested
    part-way through `apply` (the run stops before its next mutation and `restore`
    returns everything) and on the first append of `restore` and `purge`, after the
    filesystem step it records has happened (running the command again finishes it,
    with nothing lost or duplicated).
  - **Quarantine-directory creation:** creation fails for real (`EACCES`) mid-run; the
    action is skipped with nothing changed, and earlier actions stay restorable.
  - **Scratch space:** exhausting it fails the scan explicitly
    (`tests/resources.rs`).
  - **fsync failures:** an `fsync` that returns an error is treated like a failed write,
    since every sync result is propagated. It cannot be provoked without a faulty
    device, so the tests inject a failure immediately after a successful sync instead,
    which is the latest point at which a sync error could surface.
- **Power loss or kernel crash**: argued from the ordering above, not tested. It holds
  where `fsync` of a file and of its directory makes the file's content and the
  directory's entries durable and a rename is atomic in the persisted state: ext4 (any
  `data=` mode), XFS and Btrfs on Linux, and APFS on macOS, where rdedupe uses
  `F_FULLFSYNC` for files (through Rust's `sync_all`) and directories, because plain
  `fsync` there leaves data in the drive cache. It does not hold on storage that
  acknowledges flushes it has not performed (some USB bridges, virtual disks with
  `cache=unsafe`), on ext4 with `nobarrier`, or on network and FUSE filesystems (NFS, SMB,
  sshfs), whose rename and flush semantics depend on the server; use quarantine there
  only if losing the quarantine state after a power failure is acceptable.

Corruption handling: a final line without `\n` is a torn record and is truncated away by
the next `restore`/`purge`; any other unparsable line, or a record with an unknown schema,
is corruption and every command refuses the journal.

## Fingerprint cache: `rdedupe-cache-v2`

Header `{schema, layout, authority}` where `layout` is the fingerprint layout
(`xxh3-128-sample-layout-v1`); then one entry per path: `path`, `device`, `inode`, `size`,
`modified_ns`, `changed_ns`, `digest`. Written to a temporary sibling
`<cache>.tmp-<pid>-<nanos>`, synced, and published only into an absent path (hard link,
never replacing) or over a single-link file that still carries an rdedupe cache header
and the identity seen before the scan. A file with another schema or layout, or any
corrupt entry, is ignored as a whole. Entries are used only with `--trust-cache`, and
only when every validator matches.

Limits of the validators, and what rdedupe does about them. None of these can produce a
false group, because groups are always byte-compared; they can only make a
`--trust-cache` scan miss a duplicate, which is why such scans are `heuristic` and plans
refuse them. An exact scan (the default) reads current content and never uses entries.

- **Inode reuse**: a deleted file's inode number can be given to a new file. The new file
  also needs the same device, size, modification time and change time to match; the
  change time is set by the kernel when the file is created and cannot be set by users.
- **Restored or preserved timestamps**: `cp -p`, `rsync -t`, `tar` and backup restores
  set the modification time, but writing the file changes its change time, so the entry
  no longer matches.
- **Coarse timestamps**: filesystems with 1 s or 2 s resolution (FAT, some network
  filesystems) can show identical timestamps for a same-size rewrite within one tick. A
  trusted scan can then reuse a stale digest. Use the default exact mode, or `--fresh`,
  on such filesystems.
- **Network filesystems**: clients cache attributes, so validators can be stale for
  seconds; another client's writes may not change them at all until the cache expires.
  Do not use `--trust-cache` on NFS/SMB.
- **Interrupted updates**: an interrupted scan (SIGINT, `kill -9`, power loss) never
  replaces the published cache; at worst a temporary sibling is left, and the next scan
  with the same `--cache` removes temporaries whose writer process no longer exists
  (`tests/cache.rs`).
