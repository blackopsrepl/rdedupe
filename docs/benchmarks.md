# Benchmarks

Every number here was measured on the revisions named below, on identical fixtures, with
the commands in [Reproducing](#reproducing). Nothing is extrapolated.

## Revisions and builds

| name | revision | notes |
|---|---|---|
| baseline-2.0.0 | `6ae1d4d` (main before this work) | Polars/MD5 engine; clean worktree, no local patches (the uncommitted 2.0.1 logging change in the original checkout is not included); its own release profile (`opt-level = "z"`, fat LTO) |
| v3-initial | `81efe19` | first complete 3.0 engine |
| v3-review | `e134eb7` | after the independent review fixes (full-content refinement for every bucket of three or more) |
| v3-final | `8fcd6fb` | shipped engine: lockstep one-to-many comparison, on-demand refinement, lean scratch database, 4 readers |
| v3-final-optz | `8fcd6fb` | same source with `CARGO_PROFILE_RELEASE_OPT_LEVEL=z` (size-oriented) |

All built with rustc 1.98.1, `cargo build --release --locked`, 4 jobs. The 3.0 release
profile is `opt-level = 3`, thin LTO, one codegen unit, `panic = "abort"`, stripped.

| | baseline-2.0.0 | v3-final |
|---|---:|---:|
| clean release build, no compiler cache, 4 jobs | 109.1 s | 52.7 s |
| release binary | 8,204,200 bytes | 3,491,624 bytes |

## Host and conditions

- CPU: 12th Gen Intel Core i5-12400 (6 cores, 12 threads); 62 GiB RAM; Linux 6.12.
- Storage: fixtures on Samsung MZVL2512HCJQ NVMe, Btrfs (`ssd,discard=async`). The 3.0
  scratch database lives in the same filesystem (`$TMPDIR`).
- Shared host: other workloads kept the load average at about 5 during the runs;
  every binary ran interleaved within each repeat, so drift affects all of them equally.
- **warm**: fixture pages resident. **cold**: before every run, each fixture file is
  evicted with `posix_fadvise(POSIX_FADV_DONTNEED)`; no global cache is ever dropped.
- 5 repeats per binary, case and condition; tables show medians. Wall time from a
  blocking wait (no polling), peak RSS from `/usr/bin/time`.
- **content MB read** and **peak fds** come from one extra untimed run under `strace`:
  bytes returned by `read`/`pread64`/`readv` on descriptors opened under the fixture
  directory, and the maximum number of descriptors open at once in the whole process.
- Each binary runs its legacy report route, `rdedupe DIR`, which both versions accept.
  The 2.x engine hashes every same-size file with MD5 and never compares bytes; 3.0
  compares every group byte for byte, so the two do different amounts of verification.

## Results

### hardlinks

300 distinct 64 KiB files, each with a second hard link (600 entries, 39.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 15.6 | 19.1 | 10,592 | 39.0 | 13 |
| v3-initial | 34.8 | 53.2 | 6,208 | 19.7 | 8 |
| v3-review | 35.1 | 53.3 | 6,464 | 19.7 | 8 |
| v3-final | 9.5 | 19.2 | 6,600 | 19.7 | 8 |
| v3-final-optz | 10.1 | 19.8 | 5,696 | 19.7 | 8 |

### large-duplicates

12 identical 16 MiB files (12 entries, 201.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 86.0 | 110.9 | 168,880 | 201.3 | 13 |
| v3-initial | 72.0 | 92.2 | 5,684 | 371.3 | 8 |
| v3-review | 99.5 | 123.8 | 6,464 | 572.8 | 8 |
| v3-final | 29.9 | 54.0 | 6,376 | 237.2 | 10 |
| v3-final-optz | 30.6 | 52.8 | 5,436 | 237.2 | 10 |

### same-size-distinct

1,000 distinct 64 KiB files (1,000 entries, 65.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 35.6 | 37.0 | 10,652 | 65.3 | 13 |
| v3-initial | 61.0 | 111.3 | 6,620 | 65.5 | 8 |
| v3-review | 59.1 | 111.6 | 7,076 | 65.5 | 8 |
| v3-final | 24.2 | 55.3 | 7,608 | 65.4 | 8 |
| v3-final-optz | 24.4 | 57.2 | 6,452 | 65.4 | 8 |

### sample-collisions

200 files of 300 KiB, identical in every sampled window, each different at offset 100,000 (200 entries, 61.4 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 22.6 | 27.8 | 14,464 | 61.4 | 13 |
| v3-initial | 547.5 | 600.2 | 6,092 | 5256.0 | 8 |
| v3-review | 55.8 | 91.8 | 6,468 | 100.8 | 8 |
| v3-final | 16.7 | 36.6 | 7,132 | 101.9 | 9 |
| v3-final-optz | 17.3 | 37.1 | 6,076 | 101.9 | 9 |

### shared-prefix

1,000 files of 64 KiB sharing a 32 KiB prefix (1,000 entries, 65.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 24.3 | 38.7 | 10,800 | 65.3 | 13 |
| v3-initial | 50.6 | 115.0 | 6,812 | 65.5 | 8 |
| v3-review | 52.1 | 117.0 | 7,192 | 65.5 | 8 |
| v3-final | 18.9 | 51.9 | 7,696 | 65.3 | 8 |
| v3-final-optz | 20.8 | 54.5 | 6,404 | 65.3 | 8 |

### sparse

two 64 MiB holes and one 64 MiB + 1 byte hole (3 entries, 201.3 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 144.8 | 167.6 | 201,864 | 201.3 | 6 |
| v3-initial | 48.4 | 84.9 | 5,784 | 134.6 | 8 |
| v3-review | 40.1 | 75.0 | 5,900 | 134.6 | 8 |
| v3-final | 18.7 | 53.5 | 5,888 | 134.6 | 6 |
| v3-final-optz | 18.6 | 52.5 | 4,980 | 134.6 | 6 |

### tiny-duplicates

6,000 files of 80 bytes, 2,000 triples (6,000 entries, 0.5 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 16.4 | 16.1 | 15,992 | 0.5 | 13 |
| v3-initial | 303.0 | 299.3 | 12,636 | 1.1 | 8 |
| v3-review | 445.4 | 467.9 | 14,044 | 1.6 | 9 |
| v3-final | 157.9 | 165.6 | 14,224 | 1.0 | 8 |
| v3-final-optz | 187.7 | 190.9 | 13,152 | 1.0 | 8 |

### unique-sizes

1,200 files, every size distinct (32,768 + i bytes) (1,200 entries, 40.0 MB logical).

| binary | warm ms | cold ms | peak RSS KiB | content MB read | peak fds |
|---|---:|---:|---:|---:|---:|
| baseline-2.0.0 | 17.4 | 32.9 | 10,368 | 39.8 | 13 |
| v3-initial | 34.0 | 33.6 | 6,068 | 0.0 | 7 |
| v3-review | 33.3 | 33.7 | 6,140 | 0.0 | 7 |
| v3-final | 8.5 | 8.7 | 6,204 | 0.0 | 5 |
| v3-final-optz | 9.7 | 9.7 | 5,172 | 0.0 | 5 |

## Attribution

Each row compares measured revisions on the workload that isolates the change.

| change | workload | before | after |
|---|---|---|---|
| size-first elimination (files of unique size are never opened) | unique-sizes | baseline: 39.8 MB read | v3-final: 0.0 MB read |
| hard links resolved as aliases before reading | hardlinks | baseline: 39.0 MB read | v3-final: 19.7 MB read |
| size elimination plus streaming comparison | sparse | baseline: 201.3 MB read, 201,864 KiB RSS | v3-final: 134.6 MB read, 5,888 KiB RSS |
| streaming, bounded buffers instead of whole-file reads | large-duplicates | baseline: 168,880 KiB RSS | v3-final: 6,376 KiB RSS |
| full-content refinement of colliding samples (review finding 6) | sample-collisions | v3-initial: 5256.0 MB read, 547.5 ms warm | v3-review: 100.8 MB read, 55.8 ms warm |
| lockstep one-to-many comparison, refinement only past two classes | large-duplicates | v3-review: 572.8 MB read | v3-final: 237.2 MB read |
| scratch database without fsync, prepared statements, in-memory one-page classes, no rollback journal, 4 readers | unique-sizes / tiny-duplicates | v3-review: 33.3 / 445.4 ms warm | v3-final: 8.5 / 157.9 ms warm |

Steps inside the last row, measured while developing it (`rdedupe DIR` on
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

`rdedupe scan`, median of 5, warm / cold ms, 256 KiB buffer:

| --readers | same-size-distinct | large-duplicates | tiny-duplicates |
|---:|---:|---:|---:|
| 1 | 37.9 / 148.5 | 35.6 / 68.8 | 168.4 / 175.8 |
| 2 | 24.8 / 84.0 | 32.4 / 54.5 | 156.3 / 182.4 |
| 4 | 17.9 / 54.7 | 30.7 / 49.4 | 150.6 / 154.4 |
| 8 | 15.6 / 38.6 | 29.4 / 48.7 | 148.8 / 155.2 |

`--readers 4` is the default: it is faster than 2 warm and cold on this NVMe device. Eight
readers are faster still for many small files read cold, but every reader costs a buffer
and a descriptor, and concurrent random reads can lose on rotational disks (the command
reference recommends `--readers 1` there), so the default stays moderate.

`rdedupe scan`, median of 5, warm / cold ms, 4 readers:

| --buffer-kib | large-duplicates | sparse |
|---:|---:|---:|
| 64KiB | 42.9 / 56.6 | 17.9 / 52.8 |
| 256KiB | 32.5 / 52.8 | 17.9 / 48.2 |
| 1024KiB | 36.8 / 64.0 | 17.3 / 53.6 |
| 4096KiB | 42.2 / 66.2 | 20.2 / 57.1 |

256 KiB stays the default.

### Release profile

`opt-level = 3` is kept: the size-oriented build (`z`) is 1,111,392 bytes
smaller (2,380,232 vs 3,489,960) but slower where CPU matters
(tiny-duplicates 187.7 vs
157.9 ms warm) and 1–15% slower on the other warm workloads.

### Hash choice

BLAKE3 is used for sampled and full-content digests. Digests only partition candidates,
so the choice affects speed, never correctness. On these workloads hashing is not the
bottleneck: the in-process pipeline below spends its time in discovery, the scratch
database and comparison reads. No hash microbenchmark is used to claim anything.

## In-process pipeline (`cargo bench --bench pipeline`)

Library scans of generated fixtures (fixed seeds, fresh temporary directory, 5 repeats,
default budget unless named), revision `8fcd6fb`:

| workload | median ms | paths | groups | fingerprint MB | full-content MB | comparison MB | eliminated by size |
|---|---:|---:|---:|---:|---:|---:|---:|
| unique-sizes | 5.8 | 1,200 | 0 | 0.0 | 0.0 | 0.0 | 1,200 |
| same-size-distinct | 15.1 | 1,000 | 0 | 65.5 | 0.0 | 0.0 | 0 |
| shared-prefix | 14.9 | 1,000 | 0 | 65.5 | 0.0 | 0.0 | 0 |
| tiny-duplicates | 111.8 | 6,000 | 2,000 | 0.5 | 0.0 | 0.5 | 0 |
| large-duplicates | 26.1 | 12 | 1 | 2.4 | 0.0 | 234.9 | 0 |
| large-group | 81.5 | 5,000 | 1 | 0.1 | 0.0 | 0.2 | 0 |
| hard-links | 7.4 | 600 | 0 | 19.7 | 0.0 | 0.0 | 0 |
| sparse | 13.1 | 3 | 1 | 0.4 | 0.0 | 134.2 | 1 |
| sample-collisions | 12.8 | 200 | 0 | 39.3 | 61.4 | 1.2 | 0 |
| tiny-duplicates-8mib-budget | 123.3 | 6,000 | 2,000 | 0.5 | 0.0 | 0.6 | 0 |

## Memory and descriptors under a small budget

A 4,000-member group of 32-byte files plus two identical 256 MiB files, scanned by
v3-final with `--memory-mib 16 --format jsonl`, 5 runs: peak RSS
11,472–11,692 KiB (median 11,652), 10 descriptors
open at most, 537.5 MB of content read. The engine's budget
covers the SQLite page cache (11,272,192 bytes here), read buffers and a fixed
allowance; whole-process RSS adds the binary and allocator overhead.

## Frozen targets

Derived from the measurements above; a change that exceeds one needs its own measured
justification:

- content bytes read per workload no higher than v3-final's column above;
- peak RSS under the default budget no higher than 16 MiB on every workload here;
- peak open descriptors no higher than 13 + `--readers`;
- no `fsync` during `scan`.

Where 3.0 is slower than 2.x — tiny-duplicates, and cold runs of many 64 KiB files — the
cost is exact byte verification, stability checks around every read, and the disk-backed
inventory that keeps memory bounded. These are accepted, not regressions to fix by
weakening exactness.

## Reproducing

```sh
# Release binary of the revision under test
cargo build --release --locked
# CLI before/after on generated fixtures (writes compare.json):
make benchmark-compare BASELINE=/path/to/rdedupe-2.0.0 BENCH_DIR=/path/with/space
# or with several binaries:
python3 benches/compare.py FIXTURE_DIR OUT.json baseline=/path/a v3=/path/b
# In-process workloads:
make benchmark
```

`benches/compare.py` regenerates the fixtures deterministically (the manifest records a
SHA-256 per workload) and needs `/usr/bin/time` and `strace`.
