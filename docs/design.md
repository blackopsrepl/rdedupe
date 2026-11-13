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
- **Fingerprints** (`src/fingerprint.rs`): xxh3-128 over the layout name, the size and
  three fixed 64 KiB windows (start, middle, end), read in buffer-sized chunks; small
  files are read once. Readers run in a bounded pool (`src/scheduler.rs`); each leases one
  buffer from the scan's buffer pool (allocated once, `readers + 1` buffers) for a page of
  work. *Hash choice:* sampled digests only separate candidates, and hashing them was about
  half of all CPU samples with BLAKE3. xxh3 runs about four times faster (measured in
  [benchmarks](benchmarks.md#hash-choice)) and halved the user-space instructions of
  fingerprint-heavy scans. xxh3 is not collision resistant, so it is never trusted to keep
  work bounded: see refinement below. *Incremental continuation:* files up to two windows
  are read whole by their samples; a sampled bucket of them with at most two equality
  classes is decided by comparison alone and never read again for hashing. For larger files
  the windows are non-contiguous, so no prefix of a full-content hash exists to continue
  from; refinement streams the whole file once.
- **Exact comparison** (`src/compare.rs`, `src/candidates.rs`): inside each bucket, members
  are compared with one representative of every equality class found so far, so a bucket
  holding several distinct contents (a collision) still yields separate, exact groups. One
  representative is compared against a batch of `readers + 1` members in a single
  lockstep pass (each file read once per pass; a member drops out at its first differing
  chunk), so an all-identical bucket of N files costs about N + N/(readers + 1) file reads.
  Fingerprint readers are idle during comparison, so the batch reuses their descriptors and
  leases its buffers from the same pool, splitting each buffer into two halves (one per
  open file): `readers + 2` open files fit in `readers + 1` buffers.
- **Refinement**: a sampled bucket that shows more than two equality classes means the
  samples are blind to the differences (files larger than two windows) or the sample
  digests collided (possible with xxh3, including by crafting files). Its comparison
  stops, every member's complete content is hashed with BLAKE3 (collision resistant, so
  crafted inputs cannot keep distinct files together past this step), and comparison
  restarts inside the full-content buckets. N files that agree on every sampled window but
  differ elsewhere, or that were crafted to collide in xxh3, cost N extra reads, never one
  comparison per class (`tests/exactness.rs` checks both). Every open
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
exit. The workspace carries a marker with the owner's pid; a later run removes
workspaces whose owner no longer exists (left by `kill -9` or power loss) and reports how
many in the `abandoned_scratch_removed` metric. Every consumer pages through the database
with keyset cursors of `--queue` rows (default 256): the queue depth is the only batch
handed between stages, so there is no unbounded queue between discovery, readers and
later stages, and nothing collects the inventory in memory (a digest bucket that fits in
one page is classified with its equality classes in memory; larger buckets keep them in
the database). The same holds for keeper selection, plan writing, reporting, plan
verification (its own disk-backed index under `apply --scratch-dir`, capped by
`apply --temp-mib`) and recovery (which streams the journal).

`Budget` (`src/config.rs`) validates and derives the limits:

- memory: SQLite page cache = `--memory-mib` − (readers + 1) × buffer − `--queue` × 4 KiB
  (a record is bounded by `PATH_MAX`) − 3 MiB allowance, at least 2 MiB;
- queue: 16 to 65,536 records per page;
- descriptors: 13 fixed (3 stdio, 1 output file, 4 reserved for SQLite database and
  temporary files — the scan database keeps no journal or shared-memory file, the plan
  index used by `apply` does, 2 comparison inputs, 2 walker directory handles,
  1 directory handle for `fsync`) + one per reader, checked against `--descriptors` and
  enforced by lowering the process `RLIMIT_NOFILE` soft limit to it;
- disk: SQLite `max_page_count` from `--temp-mib`; exhaustion fails the scan with an
  explicit error and the workspace is still removed.

The budget bounds the engine's working set. Measured whole-process RSS additionally
includes the binary, the allocator and libc, so the two are related but not identical.
`tests/resources.rs` runs a 4,000-member group plus two 256 MiB files under
`RLIMIT_NOFILE=16` and asserts peak RSS below 64 MiB (observed: about 13 MiB with a
16 MiB budget), and scans 60,600 paths at a 7 MiB budget with `--queue 64`, where the inventory
database ends about 14 times larger than the page cache (`inventory_bytes` versus
`sqlite_page_cache_bytes` in the summary), asserting exact groups, peak RSS below 32 MiB
(observed: about 12 MiB) and at most 16 open descriptors sampled from `/proc` (observed:
6).

## Empty files, metadata differences and storage

- **Empty files** are exact duplicates of each other but free nothing; they are grouped
  and reported, and the policy retains them unless `--include-empty`.
- **Metadata differences.** Equality is content only. Permissions, owner, group and
  extended attributes are not part of it, so two members of a group may differ in them.
  Removing a copy loses its metadata, so every report member carries
  `metadata_differs_from_keeper` (which of permissions/owner/group/extended attributes
  differ) and every plan action's `reason` says so ("differs from the keeper in owner (not
  kept when this copy is removed)"). Timestamps are not compared: they differ between copies as a rule.
- **Sparse and shared storage.** Content is compared logically: a sparse file and a dense
  file with the same bytes are duplicates. Space figures are conservative.
  `logical_redundant_bytes` is the logical length of eligible copies;
  `estimated_allocated_bytes` counts allocated blocks (`st_blocks`) only of single-link
  copies, because removing one name of a multi-link object frees nothing. Neither number
  is promised as reclaimable: blocks already shared through reflinks or extent sharing,
  snapshots, compression and open descriptors can all keep storage alive, and rdedupe
  cannot see them. Extent mode never claims savings; the kernel reports what it shared.

## Scan-time versus action-time evidence

A scan establishes equality at one moment. A plan records, for every target and keeper,
the observation made at scan time. `apply` treats that as a claim to re-check: it
re-resolves both paths, requires identical observations, distinct identities and a fresh
complete byte comparison, and only then acts. Anything else is `skipped_stale` and the
rest of that group stops. See [safety](safety.md) for what this does and does not
guarantee on a live tree.

## Cancellation and partial outcomes

SIGINT sets a shared flag checked for every directory entry during discovery, between
reads in every stage, while loading a cache, during accounting, and between actions. A
cancelled scan exits 130 with a `cancelled` summary holding the groups established so far
(still exact) and refuses to plan; the existing cache is left untouched. A cancelled
`apply` stops before its next action; every started action is journaled, and
`apply --resume` continues it. `tests/cli.rs` sends a real SIGINT mid-comparison.

## Dependencies

Each runtime dependency replaces code that would otherwise be written and audited here:

- `rusqlite` with bundled SQLite: the disk-backed inventory. It provides paged B-tree
  indexes, keyset cursors, a page cache whose size is a budget, and `max_page_count` as
  a scratch-size limit, which is exactly the external-memory sorting and grouping the
  engine needs. It is bundled so the static release binary has no system library
  dependency, and so the version is fixed by `Cargo.lock` rather than by the host.
- `blake3`: collision-resistant full-content digests for refinement, plan checksums and
  the keyed seals on plans and journals (its keyed mode is a MAC, so no separate HMAC
  dependency is needed).
- `xxhash-rust` (xxh3 only): fast sampled fingerprints (see Pipeline).
- `walkdir`: directory traversal with a bounded number of open handles and no symlink
  following.
- `clap`: argument parsing and the `--help` text that the skill tests compare against.
- `serde`/`serde_json`, `csv`, `hex`: the machine formats (JSON Lines, RFC 4180 CSV,
  lossless hex paths).
- `libc`: the system calls the standard library does not expose (`renameat2`,
  `renameatx_np`, `fstatat`, `unlinkat`, `FIDEDUPERANGE`, `flock`, `setrlimit`,
  `listxattr`, `F_FULLFSYNC`).
- `ctrlc`: one SIGINT handler that sets the cancellation flag.
- `thiserror`: error type derivation.

Nothing pulls in an async runtime or a thread pool; parallelism is scoped threads under
the reader budget. Development-only dependencies (`assert_cmd`, `predicates`,
`tempfile`, `rand`) never ship.

## Platforms

Quarantine, restore and purge open the containing directories once (`O_DIRECTORY |
O_NOFOLLOW`) and check, rename and unlink by name relative to those descriptors
(`fstatat`, `renameat2`/`renameatx_np`, `unlinkat`), so a parent swapped for a symlink
after validation cannot redirect an operation. `rdedupe capabilities --path DIR` probes a
real filesystem.

- Linux: everything, including extent sharing (`FIDEDUPERANGE`) and `renameat2
  (RENAME_NOREPLACE)` quarantine moves.
- macOS: scanning, plans and quarantine, with `renameatx_np(RENAME_EXCL)` moves. Extent
  sharing reports unsupported.
- Other Unix (BSD and others): scanning, reports and plans. Quarantine is unsupported:
  without an atomic no-replace rename the quarantine root cannot be published safely, so
  `capabilities` says `unsupported` and apply fails explicitly.
- Windows: not supported (no native-byte paths, no no-replace rename or advisory lock in
  the current implementation); the code fails explicitly rather than degrading.

Paths are handled as native bytes end to end; see [formats](formats.md).
