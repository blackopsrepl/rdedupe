# Benchmarks

Every number here was measured on the revisions named below, on identical fixtures, with
the commands in [Reproducing](#reproducing). Nothing is extrapolated.

## Revisions and builds

| name | revision | notes |
|---|---|---|
| baseline-2.0.0 | `6ae1d4d` (main before this work) | Polars/MD5 engine; clean worktree, no local patches (the uncommitted 2.0.1 logging change in the original checkout is not included); its own release profile (`opt-level = "z"`, fat LTO) |
| v3-initial | `81efe19` | first complete 3.0 engine |
| v3-review | `e134eb7` | after the independent review fixes (full-content refinement for every bucket of three or more) |
| v3-perf | `8fcd6fb` | after the performance work: lockstep one-to-many comparison, on-demand refinement, lean scratch database, 4 readers |
| v3-final | `bb571da` | shipped engine: v3-perf plus the issue-audit work (descriptor-relative quarantine, owner/group/creation-time observations, metadata-difference reporting, cancellation everywhere, scratch-marker cleanup, scan-owned buffer pool, queue budget, resume) |
| v3-final-optz | `bb571da` | same source with `CARGO_PROFILE_RELEASE_OPT_LEVEL=z` (size-oriented) |

All are built with rustc 1.98.1 and `cargo build --release --locked` with 4 jobs, each in
its own target directory; the binaries' SHA-256 hashes were checked to be distinct. The
3.0 release profile is `opt-level = 3`, thin LTO, one codegen unit, `panic = "abort"`,
stripped.

| | baseline-2.0.0 | v3-final |
|---|---:|---:|
| clean release build, no compiler cache, 4 jobs | 109.1 s | 58.5 s |
| release binary | 8,204,200 bytes | 3,580,520 bytes |

## Host and conditions

- CPU: 12th Gen Intel Core i5-12400 (6 cores, 12 threads); 62 GiB RAM; Linux 6.12.
- Storage: fixtures on Samsung MZVL2512HCJQ NVMe, Btrfs (`ssd,discard=async`). The 3.0
  scratch database lives in the same filesystem (`$TMPDIR`).
- Shared host: an unrelated compile job kept the load average between 7 and 9 during the
  final run. Every binary ran interleaved within each repeat, so the drift affects all of
  them equally; absolute times are higher than on an idle machine.
- **warm**: fixture pages resident. **cold**: before every run, each fixture file is
  evicted with `posix_fadvise(POSIX_FADV_DONTNEED)`; no global cache is ever dropped.
- 5 repeats per binary, case and condition; tables show medians. Wall time comes from a
  blocking wait (no polling), peak RSS from `/usr/bin/time`.
- **content MB read** and **peak fds** come from one extra untimed run under `strace`:
  - content MB read is the bytes returned by `read`/`pread64`/`readv` on descriptors
    opened under the fixture directory;
  - peak fds is the maximum number of descriptors open at once in the whole process.
- Each binary runs its legacy report route, `rdedupe DIR`, which both versions accept.
  The 2.x engine hashes every same-size file with MD5 and never compares bytes; 3.0
  compares every group byte for byte, so the two do different amounts of verification.

## Results

### hardlinks

300 distinct 64 KiB files, each with a second hard link (600 entries, 39.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 16.4 | 19.6 | 10,872 | 39.2 | 15 |
| v3-initial | 35.1 | 53.9 | 6,216 | 19.7 | 8 |
| v3-review | 35.5 | 54.2 | 6,372 | 19.7 | 8 |
| v3-perf | 9.6 | 19.8 | 6,652 | 19.6 | 8 |
| v3-final | 10.2 | 21.1 | 6,220 | 19.6 | 8 |
| v3-final-optz | 11.2 | 21.6 | 5,204 | 19.5 | 8 |

### large-duplicates

12 identical 16 MiB files (12 entries, 201.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 106.7 | 152.7 | 201,592 | 201.3 | 15 |
| v3-initial | 76.5 | 99.5 | 5,704 | 371.5 | 8 |
| v3-review | 112.0 | 130.6 | 6,416 | 572.8 | 8 |
| v3-perf | 32.7 | 59.6 | 6,488 | 237.2 | 10 |
| v3-final | 33.3 | 66.9 | 6,436 | 237.2 | 10 |
| v3-final-optz | 34.7 | 59.1 | 5,228 | 237.2 | 10 |

### same-size-distinct

1,000 distinct 64 KiB files (1,000 entries, 65.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 24.2 | 34.2 | 10,944 | 65.3 | 15 |
| v3-initial | 59.7 | 113.3 | 6,780 | 65.5 | 8 |
| v3-review | 52.4 | 112.8 | 7,120 | 65.5 | 8 |
| v3-perf | 19.1 | 55.3 | 7,504 | 65.3 | 8 |
| v3-final | 20.0 | 55.3 | 6,760 | 65.3 | 8 |
| v3-final-optz | 22.3 | 58.6 | 5,580 | 65.3 | 8 |

### sample-collisions

200 files of 300 KiB, identical in every sampled window, each different at offset 100,000 (200 entries, 61.4 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 24.9 | 30.5 | 15,712 | 61.1 | 15 |
| v3-initial | 604.7 | 653.0 | 6,200 | 5256.0 | 8 |
| v3-review | 52.6 | 89.9 | 6,600 | 100.8 | 8 |
| v3-perf | 16.9 | 37.0 | 7,124 | 101.9 | 9 |
| v3-final | 18.0 | 37.2 | 6,912 | 101.9 | 9 |
| v3-final-optz | 18.6 | 40.7 | 5,680 | 101.9 | 9 |

### shared-prefix

1,000 files of 64 KiB sharing a 32 KiB prefix (1,000 entries, 65.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 34.2 | 36.6 | 10,980 | 65.4 | 15 |
| v3-initial | 64.4 | 113.8 | 6,860 | 65.5 | 8 |
| v3-review | 62.5 | 113.2 | 7,044 | 65.5 | 8 |
| v3-perf | 20.4 | 53.0 | 7,616 | 65.2 | 8 |
| v3-final | 21.5 | 54.0 | 6,676 | 65.5 | 8 |
| v3-final-optz | 22.5 | 55.7 | 5,708 | 65.4 | 8 |

### sparse

two 64 MiB holes and one 64 MiB + 1 byte hole (3 entries, 201.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 167.8 | 206.4 | 201,892 | 201.3 | 6 |
| v3-initial | 48.8 | 81.8 | 5,748 | 134.6 | 8 |
| v3-review | 40.8 | 72.5 | 5,888 | 134.6 | 8 |
| v3-perf | 19.4 | 51.6 | 5,920 | 134.6 | 6 |
| v3-final | 20.3 | 50.0 | 5,824 | 134.6 | 6 |
| v3-final-optz | 20.0 | 50.3 | 4,980 | 134.6 | 6 |

### tiny-duplicates

6,000 files of 80 bytes, 2,000 triples (6,000 entries, 0.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 16.0 | 20.4 | 16,284 | 0.5 | 15 |
| v3-initial | 301.3 | 316.3 | 12,492 | 1.1 | 8 |
| v3-review | 448.3 | 461.1 | 13,736 | 1.6 | 9 |
| v3-perf | 170.4 | 160.1 | 14,084 | 1.0 | 8 |
| v3-final | 183.8 | 192.8 | 13,636 | 1.0 | 8 |
| v3-final-optz | 204.9 | 219.7 | 12,516 | 1.0 | 8 |

### unique-sizes

1,200 files, every size distinct (32,768 + i bytes) (1,200 entries, 40.0 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 19.7 | 32.3 | 10,788 | 39.8 | 15 |
| v3-initial | 33.7 | 34.0 | 6,100 | 0.0 | 7 |
| v3-review | 34.1 | 34.0 | 6,040 | 0.0 | 7 |
| v3-perf | 9.0 | 9.1 | 6,224 | 0.0 | 5 |
| v3-final | 10.0 | 10.0 | 6,424 | 0.0 | 5 |
| v3-final-optz | 11.2 | 11.3 | 5,268 | 0.0 | 5 |

## Attribution

Each row compares measured revisions (same run) on the workload that isolates the change.

| change | workload | before | after |
|---|---|---|---|
| size-first elimination (files of unique size are never opened) | unique-sizes | baseline: 39.8 MB read | v3-final: 0.0 MB read |
| hard links resolved as aliases before reading | hardlinks | baseline: 39.2 MB read | v3-final: 19.6 MB read |
| size elimination plus streaming comparison | sparse | baseline: 201.3 MB read, 201,892 KiB RSS | v3-final: 134.6 MB read, 5,824 KiB RSS |
| streaming, bounded buffers instead of whole-file reads | large-duplicates | baseline: 201,592 KiB RSS | v3-final: 6,436 KiB RSS |
| full-content refinement of colliding samples (review finding 6) | sample-collisions | v3-initial: 5256.0 MB read, 604.7 ms warm | v3-review: 100.8 MB read, 52.6 ms warm |
| lockstep one-to-many comparison, refinement only past two classes | large-duplicates | v3-review: 572.8 MB read | v3-perf: 237.2 MB read |
| scratch database without fsync, prepared statements, in-memory one-page classes, no rollback journal, 4 readers | unique-sizes / tiny-duplicates | v3-review: 34.1 / 448.3 ms warm | v3-perf: 9.0 / 170.4 ms warm |
| issue-audit work (safety, metadata, cancellation, resume) | tiny-duplicates | v3-perf: 170.4 ms warm | v3-final: 183.8 ms warm |

The last row is a cost, not a gain. On tiny-duplicates, where 6,000 report members are the
work, `perf stat` on the legacy route (5 runs) attributes it to kernel time. Reporting
whether each copy's extended attributes differ from its keeper's takes one `llistxattr`
per member. The call resolves the full path, and Linux 6.12 offers no
directory-relative variant. It added about 149M kernel instructions per run before
`bb571da`, which skips the second call for files without attributes and stops waking the
buffer pool's condition variable when nobody waits; together those removed about 70M of
them. The remaining cost is the price of not silently dropping permissions, ownership or
attributes.

Steps inside the database row, measured while developing it (`rdedupe DIR` on
tiny-duplicates, warm, medians of 7 runs unless noted):

| step | before | after |
|---|---:|---:|
| prepared-statement cache for hot store statements (debug-symbol build; 1 run before, 3 after) | 600 ms | 260 ms |
| SQLite opened without mutexes | 194.7 ms | 181.3 ms |
| exclusive locking mode | 181.3 ms | 167.2 ms |
| no rollback journal for the disposable scratch database | 167.2 ms | 154.4 ms |

Setting `synchronous=OFF` before switching the scratch database's journal mode removed
the four `fsync` calls every scan used to make (`strace -f -e trace=fsync,fdatasync`
counts 4 before, 0 after); its time effect is included in the row above.

### Readers and buffers

Measured on `8fcd6fb` while choosing the defaults and not re-run at `bb571da` (whose
buffer pool changes how buffers are allocated, not how many there are or their size). `rdedupe scan`, median of 5, warm / cold ms, 256 KiB buffer:

| --readers | same-size-distinct | large-duplicates | tiny-duplicates |
|---:|---:|---:|---:|
| 1 | 37.9 / 148.5 | 35.6 / 68.8 | 168.4 / 175.8 |
| 2 | 24.8 / 84.0 | 32.4 / 54.5 | 156.3 / 182.4 |
| 4 | 17.9 / 54.7 | 30.7 / 49.4 | 150.6 / 154.4 |
| 8 | 15.6 / 38.6 | 29.4 / 48.7 | 148.8 / 155.2 |

`--readers 4` is the default because it is faster than 2, warm and cold, on this NVMe
device. Eight readers are faster still for many small files read cold. But every reader
costs a buffer and a descriptor, and concurrent random reads can lose on rotational disks
(the command reference recommends `--readers 1` there), so the default stays moderate.

`rdedupe scan`, median of 5, warm / cold ms, 4 readers:

| --buffer-kib | large-duplicates | sparse |
|---:|---:|---:|
| 64KiB | 42.9 / 56.6 | 17.9 / 52.8 |
| 256KiB | 32.5 / 52.8 | 17.9 / 48.2 |
| 1024KiB | 36.8 / 64.0 | 17.3 / 53.6 |
| 4096KiB | 42.2 / 66.2 | 20.2 / 57.1 |

256 KiB stays the default.

### Release profile

`opt-level = 3` is kept. The size-oriented build (`z`) is
1,132,640 bytes smaller
(2,446,280 vs 3,578,920), but it is slower
where CPU matters (tiny-duplicates 204.9 vs
183.8 ms warm) and slower or equal on the other warm
workloads.

### Hash choice

BLAKE3 is used for sampled and full-content digests. Digests only partition candidates,
so the choice affects speed, never correctness. On these workloads hashing is not the
bottleneck: the in-process pipeline below spends its time in discovery, the scratch
database and comparison reads. No hash microbenchmark is used to claim anything.

## In-process pipeline (`cargo bench --bench pipeline`)

These are library scans of generated fixtures: fixed seeds, a fresh temporary directory,
5 repeats, and the default budget unless the workload name says otherwise. Revision
`bb571da`. "page cache" is the SQLite page-cache cap derived from the budget;
"inventory" is the final size of the on-disk inventory database.

| workload | median ms | paths | groups | fingerprint MB | full-content MB | comparison MB | page cache MiB | inventory MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| unique-sizes | 7.6 | 1,200 | 0 | 0.0 | 0.0 | 0.0 | 58.8 | 0.5 |
| same-size-distinct | 17.1 | 1,000 | 0 | 65.5 | 0.0 | 0.0 | 58.8 | 0.7 |
| shared-prefix | 16.8 | 1,000 | 0 | 65.5 | 0.0 | 0.0 | 58.8 | 0.7 |
| tiny-duplicates | 126.8 | 6,000 | 2,000 | 0.5 | 0.0 | 0.5 | 58.8 | 4.9 |
| large-duplicates | 32.9 | 12 | 1 | 2.4 | 0.0 | 234.9 | 58.8 | 0.1 |
| large-group | 99.9 | 5,000 | 1 | 0.1 | 0.0 | 0.2 | 58.8 | 4.4 |
| hard-links | 8.3 | 600 | 0 | 19.7 | 0.0 | 0.0 | 58.8 | 0.4 |
| sparse | 17.9 | 3 | 1 | 0.4 | 0.0 | 134.2 | 58.8 | 0.1 |
| sample-collisions | 15.4 | 200 | 0 | 39.3 | 61.4 | 1.2 | 58.8 | 0.2 |
| tiny-duplicates-8mib-budget | 172.7 | 6,000 | 2,000 | 0.5 | 0.0 | 0.6 | 3.9 | 4.9 |
| large-inventory-7mib-budget | 1812.2 | 60,000 | 20,000 | 5.7 | 0.0 | 3.8 | 3.2 | 47.5 |

`large-inventory-7mib-budget` runs 60,000 files (20,000 duplicate pairs) at the 7 MiB
minimum memory budget with `--queue 64`. The inventory ends
14.6 times
larger than the page cache, so every stage works from paged, on-disk state, and the
groups stay exact (`tests/resources.rs` checks the same shape against an independent
oracle).

## Repeat scans with a cache

This is measured separately from fresh verification, as the issue requires.
`repeat-scan-cache` holds 1,000 distinct 64 KiB files plus 200 duplicates, scanned warm
after one cache-writing warm-up. Each mode is the median of 5 (min–max):

| mode | exactness | median ms | fingerprint MB read | cached fingerprints reused |
|---|---|---:|---:|---:|
| fresh | exact | 38.2 (32.5–39.2) | 78.6 | 0 |
| exact-with-cache | exact | 55.4 (48.7–59.4) | 78.6 | 0 |
| trusted-cache | heuristic | 42.0 (39.6–47.8) | 0.0 | 1,200 |

- **No cache win on this host.** With the files' pages resident, loading and rewriting
  the cache costs more than re-reading them.
- **Exact mode** (`--cache` without `--trust-cache`) never uses cached fingerprints,
  because the exactness contract requires current content. It can therefore only add
  cost: it pays for maintaining the cache and reads everything anyway.
- **`--trust-cache`** skips every fingerprint read. It is still slower than fresh here, and
  its scans are `heuristic`, so plans refuse them.
- **Where it could pay off:** a trusted cache could help on storage where reads are
  expensive (cold rotational disks, network filesystems). That was not measured here,
  so it is not claimed.

## Memory and descriptors under a small budget

The v3-final release binary scanned the 4,000-member group plus two identical 256 MiB
files with `--memory-mib 16 --format jsonl` five times; peak RSS was
10,544–10,972 KiB (median 10,636).

`tests/resources.rs`, run against the debug test binary at `bb571da`:

- **One large group plus two big files:** a 4,000-member group of 32-byte files plus two
  identical 256 MiB files, scanned with `--memory-mib 16 --buffer-kib 64 --readers 2`
  under `RLIMIT_NOFILE=16`. Peak RSS was 13,428 KiB.
- **Large inventory:** 60,600 paths at `--memory-mib 7 --queue 64 --readers 1` under
  `RLIMIT_NOFILE=16`.
  - The inventory database ended at 47,714,304 bytes against a 3,407,872-byte page cache.
  - Peak RSS was 12,128 KiB.
  - At most 6 descriptors were open, sampled from `/proc/<pid>/fd` every millisecond.

The engine's budget covers the SQLite page cache, the read buffers, the queue and a fixed
allowance. Whole-process RSS adds the binary and the allocator overhead.

## Frozen targets

These are derived from the measurements above; a change that exceeds one needs its own
measured justification:

- content bytes read per workload no higher than v3-final's column above;
- peak RSS under the default budget no higher than 16 MiB on every workload here;
- peak open descriptors no higher than 13 + `--readers`;
- no `fsync` during `scan`.

Where 3.0 is slower than 2.x, the cost is the work 2.x skipped:

- **Where:** tiny-duplicates (6,000 tiny files), and cold runs of many 64 KiB files.
- **Why:** exact byte verification, stability checks around every read, the disk-backed
  inventory that keeps memory bounded, and per-copy metadata comparison.
- **Decision:** these are accepted. They are not regressions to fix by weakening
  exactness.

## Reproducing

```sh
# Release binary of the revision under test
cargo build --release --locked
# CLI before/after on generated fixtures (writes compare.json):
make benchmark-compare BASELINE=/path/to/rdedupe-2.0.0 BENCH_DIR=/path/with/space
# or with several binaries (build each revision in its own CARGO_TARGET_DIR):
python3 benches/compare.py FIXTURE_DIR OUT.json baseline=/path/a v3=/path/b
# In-process workloads, including the large inventory and the cache modes:
make benchmark
```

`benches/compare.py` regenerates the fixtures deterministically (the manifest records a
SHA-256 per workload) and needs `/usr/bin/time` and `strace`.
