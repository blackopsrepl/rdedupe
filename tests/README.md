# Tests

`make test` runs every suite (`cargo test --locked --all-targets`). Correctness is
asserted as exact sets of paths against an independent oracle, never as counts within a
tolerance.

## The oracle

`tests/support/mod.rs::oracle` walks the tree with `std::fs`, collapses hard links to
their smallest path, reads every file completely and partitions by direct byte
comparison. It shares no code with rdedupe. Seeded trees (`seeded_tree`) mix shared
sizes, one-byte differences outside the sampled fingerprint windows, and sizes around
the 64 KiB window boundaries.

## Suites

| File | Covers |
| --- | --- |
| `exactness.rs` | oracle equality over seeds, readers, buffers and budgets; forced collisions (A B B C A C D → three pairs); unsampled differences with proven fingerprint collisions; buffer-boundary lengths; zero reads for unique sizes; empty files; read failures |
| `discovery.rs` | invalid UTF-8 and control-byte names through JSONL, NUL and human output; hard links; overlapping roots; symlinks; FIFOs; unreadable files and directories; hidden and look-alike quarantine directories; `--pattern` |
| `cli.rs` | legacy routes, version, exit codes, CSV quoting, refusing to overwrite sources through symlinks and hard links, broken pipes, prompts, progress on stderr |
| `policy.rs` | protection by path component (`keep` vs `keeper`), several protected survivors, preferred roots, newest, order independence |
| `plans.rs` | round trip, corruption, truncation, oversized lines, duplicate targets, keeper cycles, scope escapes, protected targets, unknown schema or encoding, heuristic and partial headers (all with recomputed checksums), working-directory independence |
| `mutation.rs` | changes between plan and apply: content, replacement with equal or unequal bytes, keeper replaced or deleted, symlink substitution of targets and ancestors, `--force` |
| `recovery.rs` | quarantine/restore/purge read-back, restore conflicts, altered and missing quarantine objects, keeper changes before purge, journal locking, torn versus corrupt records, journal write failure, a real process abort after each of the six durable journal records, rename-before-record and link-then-unlink interruptions |
| `cache.rs` | stale negative fingerprints cannot hide duplicates in exact mode; trusted mode is heuristic and cannot plan; validator changes; corrupt and foreign caches; fresh mode; atomic writes |
| `extent_dedupe.rs` | real `FIDEDUPERANGE`: multi-chunk sharing, identities kept, `filefrag` shared extents, copy-on-write isolation, kernel refusal of differing ranges, unsupported filesystem with no fallback |
| `resources.rs` | 4,000-member group and 256 MiB files under `RLIMIT_NOFILE=16` with measured peak RSS; budget parity; long paths; cancellation; scratch exhaustion and cleanup |
| `integration.rs` | generate → plan → apply → rescan → restore with exact survivor sets |
| `agent_skill.rs` | the shipped skill: frontmatter, links, no local paths, documented flags equal `--help` both ways, exit codes, schemas, every workflow command executed, helper script on adversarial names |
| `agent_workflows.rs` | scripted automation contracts: report-only, protected cleanup, partial scan, stale plan, interrupted recovery, purge confirmation |

## Capability-gated tests

Some tests print `SKIP:` and return when the environment cannot exercise them:
permission tests when running as root (permission bits do not deny root), extent tests
when the temporary directory's filesystem lacks `FIDEDUPERANGE`, the no-fallback test when
`/dev/shm` is missing, and the helper-script tests without `python3`. Run with
`-- --nocapture` to see which ran. `make filesystem-acceptance` runs the extent suite
verbosely; point `TMPDIR` at a Btrfs or XFS directory to exercise it.

Process crash tests abort a real child process; they do not demonstrate power-loss
durability, which depends on the filesystem.
