# Design

rdedupe 3 answers one question exactly: which regular files under the given roots have
byte-identical content. It then lets you act on the answer through a saved plan that is
re-validated against the filesystem before every action.

## Terms

- **Path**: one directory entry. **Object**: one filesystem object, identified by
  `(device, inode)`. **Alias**: an additional path to an object (a hard link).
- **Group**: two or more distinct objects with identical content. Aliases of one object
  never form a group by themselves and never count as reclaimable copies; each object
  takes part through its smallest native path.
- **Keeper**: the group member that stays. **Eligible copy**: every other member that is
  not under a protected root.
- **Complete scan**: every inventoried candidate was resolved. **Exact scan**: no cached
  fingerprint replaced a fresh read.

## Invariants

1. Membership in a group requires a complete byte comparison of the two objects made
   during this scan, with both objects unchanged (by `fstat`) for its whole duration.
   Fingerprints and sizes only ever separate candidates.
2. Any read or comparison failure makes the object unresolved and the scan partial; it
   never joins or leaves a group on partial evidence.
3. Output order is deterministic: groups and members follow native path byte order, and
   keepers follow a total order, independent of traversal order, worker count, buffer
   size or memory budget.
4. Mutations happen only through a plan from a complete, exact scan, and each action
   re-establishes its preconditions from current facts immediately before acting.
5. No action ever overwrites an existing directory entry.

## Pipeline

```
discover → size classes → fingerprints → exact comparison → groups → accounting
 (walk)     (SQL index)    (bounded       (one representative          (policy,
                            readers)       per equality class)          metrics)
```

- **Discovery** (`src/fs.rs`) walks each canonical root without following symlinks, in
  sorted order, with at most two directory handles open. Overlapping roots are
  inventoried once. Regular files are recorded with their full observation (identity,
  size, allocation, timestamps in nanoseconds, mode, link count); symlinks and special
  files are counted and skipped; traversal and metadata errors become unresolved records.
  Owned artifacts (scratch workspace, cache file, quarantine directories carrying a valid
  marker) are excluded by exact path, never by name pattern.
- **Size elimination** (`src/store.rs`, `src/candidates.rs`): objects whose size is unique
  are never opened. Empty files are grouped like any other size, but the policy retains
  every empty non-keeper unless `--include-empty` is given, so plans never act on them.
- **Fingerprints** (`src/fingerprint.rs`): BLAKE3 over the layout name, the size and three
  fixed 64 KiB windows (start, middle, end), read in buffer-sized chunks; small files are
  read once. Readers run in a bounded pool (`src/scheduler.rs`), each owning one buffer.
- **Exact comparison** (`src/compare.rs`, `src/candidates.rs`): inside each bucket, members
  are compared with one representative of every equality class found so far, so a bucket
  holding several distinct contents (a collision) still yields separate, exact groups. One
  representative is compared against a batch of `readers + 1` members in a single
  lockstep pass (each file read once per pass; a member drops out at its first differing
  chunk), so an all-identical bucket of N files costs about N + N/(readers + 1) file reads.
  Fingerprint readers are idle during comparison, so the batch reuses their descriptors and
  buffers.
- **Refinement**: a sampled bucket of files larger than two windows that shows more than
  two equality classes means the samples are blind to the differences. Its comparison
  stops, every member's complete content is hashed (streamed BLAKE3), and comparison
  restarts inside the full-content buckets, so N files that agree on every sampled window
  but differ elsewhere cost N extra reads, never one comparison per class. Every open
  for fingerprinting, refinement and comparison checks the object against its inventory
  observation (identity, size, mtime, ctime); an object that changed after discovery is
  unresolved and makes the scan partial, never "unique".
  Reads retry short and interrupted reads; a short read is never taken for a difference.
- **Accounting** (`src/accounting.rs`) counts paths, objects, aliases, groups,
  participants, eligible copies, logical redundant bytes and allocated bytes of
  single-link copies.

## Bounded resources

All inventory, fingerprint, comparison-class and group state lives in a private SQLite
database (`src/store.rs`) in a fresh `0700` workspace under `--scratch-dir`, removed on
exit. Every consumer pages through it with keyset cursors of 256 rows; nothing collects
the inventory in memory (a digest bucket that fits in one 256-row page is classified with
its equality classes in memory; larger buckets keep them in the database), including keeper selection, plan writing, reporting, plan
verification (which uses its own disk-backed index) and recovery (which streams the
journal).

`Budget` (`src/config.rs`) validates and derives the limits:

- memory: SQLite page cache = `--memory-mib` − (readers + 1) × buffer − 4 MiB allowance,
  at least 2 MiB;
- descriptors: 13 fixed (3 stdio, 1 output file, 4 reserved for SQLite database and
  temporary files — the scan database keeps no journal or shared-memory file, the plan
  index used by `apply` does, 2 comparison inputs, 2 walker directory handles,
  1 directory handle for `fsync`) + one per reader, checked against `--descriptors` and
  enforced by lowering the process `RLIMIT_NOFILE` soft limit to it;
- disk: SQLite `max_page_count` from `--temp-mib`; exhaustion fails the scan with an
  explicit error and the workspace is still removed.

The budget bounds the engine's working set. Measured whole-process RSS additionally
includes the binary, the allocator and libc; `tests/resources.rs` runs a 4,000-member
group plus two 256 MiB files under `RLIMIT_NOFILE=16` and asserts peak RSS below 64 MiB
(observed: about 12 MiB with a 16 MiB budget).

## Scan-time versus action-time evidence

A scan establishes equality at one moment. A plan records, for every target and keeper,
the observation made at scan time. `apply` treats that as a claim to re-check: it
re-resolves both paths, requires identical observations, distinct identities and a fresh
complete byte comparison, and only then acts. Anything else is `skipped_stale` and the
rest of that group stops. See [safety](safety.md) for what this does and does not
guarantee on a live tree.

## Cancellation and partial outcomes

SIGINT sets a shared flag checked between reads and between actions. A cancelled scan
returns a `cancelled` summary with the groups established so far (still exact) and
refuses to plan. A cancelled `apply` stops before its next action; every started action
is journaled.

## Platforms

- Linux: everything, including extent sharing (`FIDEDUPERANGE`) and `renameat2
  (RENAME_NOREPLACE)` quarantine moves.
- Other Unix (macOS, BSD): scanning, plans and quarantine. Quarantine moves use
  link-then-unlink, which never replaces an entry; an interruption between the two steps
  leaves the object under both names, which `restore` resolves. Extent sharing reports
  unsupported.
- Windows: not supported (no native-byte paths, no no-replace rename or advisory lock in
  the current implementation); the code fails explicitly rather than degrading.

Paths are handled as native bytes end to end; see [formats](formats.md).
