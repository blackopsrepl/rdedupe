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
| v3-audit | `bb571da` | v3-perf plus the issue-audit work (descriptor-relative quarantine, owner/group/creation-time observations, metadata-difference reporting, cancellation everywhere, scratch-marker cleanup, scan-owned buffer pool, queue budget, resume) |
| v3-final | `5e5f35b` | shipped engine: v3-audit plus xxh3 sampled fingerprints (BLAKE3 refinement at any size) and hard-link aliases in the outputs |
| v3-final-optz | `5e5f35b` | same source with `CARGO_PROFILE_RELEASE_OPT_LEVEL=z` (size-oriented) |

All are built with rustc 1.98.1 and `cargo build --release --locked` with 4 jobs, each in
its own target directory; the binaries' SHA-256 hashes were checked to be distinct. The
3.0 release profile is `opt-level = 3`, thin LTO, one codegen unit, `panic = "abort"`,
stripped.

| | baseline-2.0.0 | v3-final |
|---|---:|---:|
| clean release build, no compiler cache, 4 jobs | 109.1 s | 53.9 s |
| release binary | 8,204,200 bytes | 3,613,288 bytes |

## Host and conditions

- CPU: 12th Gen Intel Core i5-12400 (6 cores, 12 threads); 62 GiB RAM; Linux 6.12.
- Storage: fixtures on Samsung MZVL2512HCJQ NVMe, Btrfs (`ssd,discard=async`). The 3.0
  scratch database lives in the same filesystem (`$TMPDIR`).
- Shared host: unrelated compile jobs kept the load average around 5 during the final
  run. Every binary ran interleaved within each repeat, so the drift affects all of
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
| baseline-2.0.0 | 14.0 | 18.5 | 10,788 | 39.2 | 15 |
| v3-initial | 35.1 | 53.7 | 6,192 | 19.7 | 8 |
| v3-review | 35.3 | 53.8 | 6,280 | 19.7 | 8 |
| v3-perf | 9.4 | 19.8 | 6,696 | 19.7 | 8 |
| v3-audit | 10.0 | 20.6 | 6,104 | 19.6 | 8 |
| v3-final | 9.0 | 19.2 | 6,168 | 19.5 | 8 |
| v3-final-optz | 10.1 | 20.3 | 5,264 | 19.7 | 8 |

### large-duplicates

12 identical 16 MiB files (12 entries, 201.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 74.2 | 93.8 | 201,664 | 201.3 | 15 |
| v3-initial | 74.7 | 93.0 | 5,752 | 371.5 | 8 |
| v3-review | 106.5 | 117.0 | 6,424 | 572.8 | 8 |
| v3-perf | 33.7 | 47.9 | 6,520 | 237.2 | 10 |
| v3-audit | 34.3 | 46.8 | 6,456 | 237.2 | 10 |
| v3-final | 33.3 | 51.0 | 6,424 | 237.2 | 10 |
| v3-final-optz | 33.1 | 49.1 | 5,364 | 237.2 | 10 |

### same-size-distinct

1,000 distinct 64 KiB files (1,000 entries, 65.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 20.4 | 33.2 | 10,980 | 65.5 | 15 |
| v3-initial | 59.4 | 111.6 | 6,732 | 65.5 | 8 |
| v3-review | 52.4 | 113.0 | 7,096 | 65.5 | 8 |
| v3-perf | 19.3 | 55.1 | 7,532 | 65.2 | 8 |
| v3-audit | 19.9 | 55.4 | 6,600 | 65.4 | 8 |
| v3-final | 16.2 | 53.6 | 6,584 | 65.1 | 8 |
| v3-final-optz | 18.3 | 55.5 | 5,628 | 65.1 | 8 |

### sample-collisions

200 files of 300 KiB, identical in every sampled window, each different at offset 100,000 (200 entries, 61.4 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 19.6 | 22.9 | 15,712 | 61.4 | 15 |
| v3-initial | 581.5 | 599.6 | 6,200 | 5256.0 | 8 |
| v3-review | 55.2 | 92.4 | 6,516 | 100.8 | 8 |
| v3-perf | 16.0 | 36.4 | 7,080 | 101.9 | 9 |
| v3-audit | 16.5 | 37.0 | 6,652 | 101.9 | 9 |
| v3-final | 14.2 | 34.8 | 6,864 | 101.9 | 9 |
| v3-final-optz | 15.4 | 35.7 | 5,792 | 101.9 | 9 |

### shared-prefix

1,000 files of 64 KiB sharing a 32 KiB prefix (1,000 entries, 65.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 26.1 | 36.2 | 10,752 | 65.5 | 15 |
| v3-initial | 58.9 | 111.6 | 6,804 | 65.5 | 8 |
| v3-review | 52.4 | 112.9 | 6,960 | 65.5 | 8 |
| v3-perf | 19.4 | 52.1 | 7,460 | 65.5 | 8 |
| v3-audit | 19.9 | 52.9 | 6,680 | 65.5 | 8 |
| v3-final | 16.4 | 49.0 | 6,712 | 65.3 | 8 |
| v3-final-optz | 19.1 | 52.4 | 5,532 | 65.3 | 8 |

### sparse

two 64 MiB holes and one 64 MiB + 1 byte hole (3 entries, 201.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 165.8 | 172.0 | 202,020 | 201.3 | 6 |
| v3-initial | 41.5 | 75.0 | 5,612 | 134.6 | 8 |
| v3-review | 41.4 | 76.2 | 5,880 | 134.6 | 8 |
| v3-perf | 18.4 | 52.7 | 5,796 | 134.6 | 6 |
| v3-audit | 19.3 | 55.0 | 5,796 | 134.6 | 6 |
| v3-final | 19.6 | 56.3 | 5,968 | 134.6 | 6 |
| v3-final-optz | 19.7 | 55.0 | 4,900 | 134.6 | 6 |

### tiny-duplicates

6,000 files of 80 bytes, 2,000 triples (6,000 entries, 0.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 14.0 | 17.0 | 16,500 | 0.5 | 15 |
| v3-initial | 290.9 | 299.2 | 12,440 | 1.1 | 8 |
| v3-review | 439.7 | 447.0 | 13,784 | 1.6 | 9 |
| v3-perf | 155.5 | 157.8 | 14,104 | 1.0 | 8 |
| v3-audit | 170.6 | 174.7 | 13,700 | 1.0 | 8 |
| v3-final | 169.5 | 174.7 | 13,628 | 1.0 | 8 |
| v3-final-optz | 209.3 | 207.2 | 12,644 | 1.0 | 8 |

### unique-sizes

1,200 files, every size distinct (32,768 + i bytes) (1,200 entries, 40.0 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 14.6 | 32.0 | 10,696 | 39.9 | 15 |
| v3-initial | 33.4 | 34.0 | 6,076 | 0.0 | 7 |
| v3-review | 33.4 | 34.2 | 6,012 | 0.0 | 7 |
| v3-perf | 8.7 | 9.0 | 6,140 | 0.0 | 5 |
| v3-audit | 9.7 | 10.7 | 6,420 | 0.0 | 5 |
| v3-final | 9.6 | 9.9 | 6,460 | 0.0 | 5 |
| v3-final-optz | 10.8 | 11.2 | 5,272 | 0.0 | 5 |
## Attribution

Each row compares measured revisions (same run) on the workload that isolates the change.

| change | workload | before | after |
|---|---|---|---|
| size-first elimination (files of unique size are never opened) | unique-sizes | baseline: 39.9 MB read | v3-final: 0.0 MB read |
| hard links resolved as aliases before reading | hardlinks | baseline: 39.2 MB read | v3-final: 19.5 MB read |
| size elimination plus streaming comparison | sparse | baseline: 201.3 MB read, 202,020 KiB RSS | v3-final: 134.6 MB read, 5,968 KiB RSS |
| streaming, bounded buffers instead of whole-file reads | large-duplicates | baseline: 201,664 KiB RSS | v3-final: 6,424 KiB RSS |
| full-content refinement of colliding samples (review finding 6) | sample-collisions | v3-initial: 5256.0 MB read, 581.5 ms warm | v3-review: 100.8 MB read, 55.2 ms warm |
| lockstep one-to-many comparison, refinement only past two classes | large-duplicates | v3-review: 572.8 MB read | v3-perf: 237.2 MB read |
| scratch database without fsync, prepared statements, in-memory one-page classes, no rollback journal, 4 readers | unique-sizes / tiny-duplicates | v3-review: 33.4 / 439.7 ms warm | v3-perf: 8.7 / 155.5 ms warm |
| issue-audit work (safety, metadata, cancellation, resume) | tiny-duplicates | v3-perf: 155.5 ms warm | v3-audit: 170.6 ms warm |
| xxh3 sampled fingerprints | same-size-distinct / shared-prefix / sample-collisions | v3-audit: 19.9 / 19.9 / 16.5 ms warm | v3-final: 16.2 / 16.4 / 14.2 ms warm |

The issue-audit row is a cost, not a gain. On tiny-duplicates, where 6,000 report members are the
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

Measured on `8fcd6fb` while choosing the defaults and not re-run at `5e5f35b` (whose
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
1,134,496 bytes smaller
(2,477,128 vs 3,611,624), but it is slower
where CPU matters (tiny-duplicates 209.3 vs
169.5 ms warm) and slower or equal on the other warm
workloads.

### Hash choice

Throughput over the same 64 MiB in 64 KiB windows (the sampling window), one thread, 5
repeats (`cargo bench --bench pipeline hash`):

| hash | median ms | MiB/s |
|---|---:|---:|
| blake3 | 17.05 | 3,753 |
| xxh3-64 | 3.81 | 16,806 |
| xxh3-128 | 3.84 | 16,658 |
| siphash-1-3 (std DefaultHasher) | 12.45 | 5,143 |

With BLAKE3 for sampled fingerprints (v3-audit), BLAKE3 functions were 43.6% of CPU
samples on same-size-distinct and 56.0% on sample-collisions, measured with `perf record`
over five scans each with a symbolized release build. On large-duplicates, where
comparison dominates, they were 2.7%. Hashing was the bottleneck of fingerprint-heavy
scans.

Switching sampled fingerprints to xxh3-128 (v3-final) changed user-space instructions per
scan (`perf stat -e instructions:u`, 7 runs, which is insensitive to host load) as follows:

| workload | v3-audit | v3-final |
|---|---:|---:|
| same-size-distinct | 275.8M | 133.3M |
| shared-prefix | 275.6M | 132.5M |
| sample-collisions | 312.8M | 227.4M |
| hardlinks | 99.1M | 56.2M |
| large-duplicates | 39.5M | 34.3M |
| tiny-duplicates | 1,335.7M | 1,327.8M |

Digests never decide equality, so the choice cannot change results. It can change worst-
case cost: xxh3 is not collision resistant, so crafted files could share a sampled digest.
Full-content refinement therefore stays BLAKE3, and any sampled bucket with more than two
equality classes is refined whatever the file size. Crafted collisions then cost one extra
read per file, never one comparison per class
(`tests/exactness.rs::crafted_collisions_of_small_files_cost_linear_reads`: 36,864
comparison bytes for both 40 and 80 crafted files).

### After the final audit (`c6049a4`)

The last code changes (charging comparison failures to the representative, escaping every
human message with a printable-ASCII fast path) were checked against `5e5f35b`, the
revision the tables above measure, with user-space instructions per scan
(`perf stat -e instructions:u`, 5 runs, insensitive to host load):

| workload | `5e5f35b` | `c6049a4` |
|---|---:|---:|
| hardlinks | 56.2M | 56.2M |
| large-duplicates | 34.4M | 34.2M |
| same-size-distinct | 133.3M | 133.2M |
| sample-collisions | 227.4M | 227.4M |
| shared-prefix | 132.5M | 132.5M |
| sparse | 15.8M | 15.8M |
| tiny-duplicates | 1,327.0M | 1,280.5M |
| unique-sizes | 57.6M | 57.6M |

No workload got slower. tiny-duplicates, whose human report is 6,000 lines, runs 3.5%
fewer instructions because of the fast path. The wall-clock tables were not re-run.

## In-process pipeline (`cargo bench --bench pipeline`)

These are library scans of generated fixtures: fixed seeds, a fresh temporary directory,
5 repeats, and the default budget unless the workload name says otherwise. Revision
`5e5f35b`. "page cache" is the SQLite page-cache cap derived from the budget;
"inventory" is the final size of the on-disk inventory database.

| workload | median ms | paths | groups | fingerprint MB | full-content MB | comparison MB | page cache MiB | inventory MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| unique-sizes | 7.0 | 1,200 | 0 | 0.0 | 0.0 | 0.0 | 58.8 | 0.5 |
| same-size-distinct | 12.7 | 1,000 | 0 | 65.5 | 0.0 | 0.0 | 58.8 | 0.7 |
| shared-prefix | 12.7 | 1,000 | 0 | 65.5 | 0.0 | 0.0 | 58.8 | 0.7 |
| tiny-duplicates | 128.7 | 6,000 | 2,000 | 0.5 | 0.0 | 0.5 | 58.8 | 4.9 |
| large-duplicates | 26.0 | 12 | 1 | 2.4 | 0.0 | 234.9 | 58.8 | 0.1 |
| large-group | 90.8 | 5,000 | 1 | 0.1 | 0.0 | 0.2 | 58.8 | 4.4 |
| hard-links | 6.4 | 600 | 0 | 19.7 | 0.0 | 0.0 | 58.8 | 0.4 |
| sparse | 14.8 | 3 | 1 | 0.4 | 0.0 | 134.2 | 58.8 | 0.1 |
| sample-collisions | 11.3 | 200 | 0 | 39.3 | 61.4 | 1.2 | 58.8 | 0.2 |
| tiny-duplicates-8mib-budget | 142.6 | 6,000 | 2,000 | 0.5 | 0.0 | 0.6 | 3.9 | 4.9 |
| large-inventory-7mib-budget | 1713.1 | 60,000 | 20,000 | 5.7 | 0.0 | 3.8 | 3.2 | 47.4 |

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
| fresh | exact | 22.0 (21.8–23.3) | 78.6 | 0 |
| exact-with-cache | exact | 35.0 (34.9–42.6) | 78.6 | 0 |
| trusted-cache | heuristic | 37.8 (37.6–39.7) | 0.0 | 1,200 |

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
10,732–11,032 KiB (median 10,952).

`tests/resources.rs`, run against the debug test binary at `5e5f35b`:

- **One large group plus two big files:** a 4,000-member group of 32-byte files plus two
  identical 256 MiB files, scanned with `--memory-mib 16 --buffer-kib 64 --readers 2`
  under `RLIMIT_NOFILE=16`. Peak RSS was 13,864 KiB.
- **Large inventory:** 60,600 paths at `--memory-mib 7 --queue 64 --readers 1` under
  `RLIMIT_NOFILE=16`.
  - The inventory database ended at 47,603,712 bytes against a 3,407,872-byte page cache.
  - Peak RSS was 12,296 KiB.
  - At most 5 descriptors were open, sampled from `/proc/<pid>/fd` every millisecond.

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
