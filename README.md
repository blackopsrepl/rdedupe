# rdedupe

Exact, resource-bounded, recoverable duplicate-file deduplication.

rdedupe finds files whose content is byte-for-byte identical, tells you which copies are
redundant, and, only when you ask, moves those copies into a recoverable quarantine or
makes them share storage with the original. Every reported duplicate is verified by a
complete byte comparison; hashes are only used to rule candidates out.

```console
$ rdedupe scan ~/Pictures --protect ~/Pictures/2024
rdedupe 3.0.0 scan: Complete, Exact

group 1 (2481152 bytes each):
  KEEP ~/Pictures/2024/beach.jpg (keeper: protected root)
  COPY ~/Pictures/Downloads/beach (1).jpg (eligible copy of ~/Pictures/2024/beach.jpg)

group 2 (1048576 bytes each):
  KEEP ~/Pictures/2024/dog-link.jpg (keeper: protected root)
  COPY ~/Pictures/Backup/dog.jpg (eligible copy of ~/Pictures/2024/dog-link.jpg)

summary: 45 paths, 44 objects, 1 hard-link aliases, 2 groups, 2 eligible copies
space: 3529728 logical redundant bytes; 3530752 allocated bytes in single-link copies (an estimate, not a promise of reclaimed space)
reads: 786432 fingerprint bytes, 0 full-content bytes, 7059456 comparison bytes; 40 objects eliminated by size without reading
```

(Real output from a 45-file fixture; its absolute root is abbreviated to `~`.)

## Guarantees

- **Exact.** A group means every member was compared byte for byte with another during
  this scan, and neither changed while it was read. Fingerprints (BLAKE3 over the size and
  three fixed 64 KiB windows) only separate candidates; files with a unique size are never
  opened.
- **Complete or explicit.** Unreadable or changing files are listed as unresolved, the scan
  is marked partial (exit 2), and partial scans cannot be turned into plans.
- **Bounded.** Inventory and grouping live in a private on-disk SQLite workspace with a
  memory budget (`--memory-mib`, default 64), a descriptor budget and a scratch-size limit.
  No file is ever read whole into memory. A 4,000-member group plus two 256 MiB files scan
  in about 11.5 MiB peak RSS with a 16 MiB budget ([measured](docs/benchmarks.md)).
- **Deterministic.** Output and keeper choice depend only on the files, never on traversal
  order, thread count or budget.
- **Recoverable.** Changes go through a saved plan; every action is re-checked against the
  filesystem immediately before it runs and is journaled durably first. Plans and journals
  are sealed with a private per-user key, so an edited plan or journal is refused. Empty
  files are reported but never acted on unless `--include-empty` is given. Quarantine is a
  rename within the same directory tree, so restore is exact and nothing is copied.

## Workflow

```console
rdedupe scan /data                                         # report (modifies nothing)
rdedupe plan /data --protect /data/originals --output cleanup.plan
rdedupe apply --plan cleanup.plan --journal cleanup.journal   # quarantine the copies
rdedupe restore --journal cleanup.journal                  # undo
rdedupe purge --journal cleanup.journal                    # or delete permanently
```

- **Keepers**: `--protect DIR` never touches members under DIR; `--prefer DIR` picks keepers
  there first; `--prefer-newest` prefers the newest copy; otherwise the smallest native
  path wins. Every member's role and reason is printed.
- **Quarantine** moves each copy into a `.rdedupe-quarantine/` directory beside it. It frees
  no space until `purge`, which deletes a quarantined copy only if its keeper still exists
  and is still identical.
- **Extent sharing** (`plan --action extent`, Linux with Btrfs or XFS) keeps every file and
  asks the kernel to share identical blocks via `FIDEDUPERANGE`. If the filesystem cannot,
  the action fails and nothing else is tried.
- **Recovery**: `rdedupe recover --journal FILE` explains the state of every action after a
  crash; `restore` and `purge` are safe to rerun.

## Machine output

`--format jsonl` emits versioned records (`summary`, `member`, `unresolved`, `end`) with
every path as hex of its native bytes, so any file name round-trips. `--format csv` adds a
lossless `path_hex` column; `--format nul` lists eligible copies for `xargs -0`. Exit codes:
0 complete, 2 partial, 3 declined, 4 error, 64 bad invocation, 130 cancelled, 141 broken
pipe. See [docs/cli.md](docs/cli.md) and [docs/formats.md](docs/formats.md).

## Agent skill

[`skills/rdedupe`](skills/rdedupe/SKILL.md) is an [Agent Skills](https://agentskills.io)
package that lets coding agents operate rdedupe on their own: authorization levels, exact
commands, refusal rules, tested workflows, recovery, and a completion-report template.
It ships in every release archive next to the binary of the same version. See
[docs/agent-operation.md](docs/agent-operation.md) for installation and the evaluation.

## Install

```console
cargo install --locked --path .        # from a checkout; Rust 1.85 or newer
```

Release archives (`rdedupe-VERSION-TARGET.tar.gz`) contain the binary, this README, the
docs and the skill, with a SHA-256 checksum.

Platforms: Linux (everything); macOS (everything except extent sharing); other Unix
systems (scan, report and plan; no quarantine, because they lack an atomic no-replace
rename for directories). `rdedupe capabilities --path DIR` probes a specific filesystem.

## Documentation

- [Design](docs/design.md): pipeline, invariants, resource bounds.
- [Safety](docs/safety.md): what is checked, what cannot be guaranteed on live trees.
- [CLI](docs/cli.md) and [formats](docs/formats.md).
- [Migration from 2.x](docs/migration.md).
- [Benchmarks](docs/benchmarks.md).
- [Tests](tests/README.md).

## Development

```console
make check                    # format-check, clippy -D warnings, all tests
make filesystem-acceptance    # FIDEDUPERANGE tests (TMPDIR on Btrfs/XFS)
make benchmark                # cargo bench workloads
make dist dist-check          # release archive and its verification
```

## License

[CC0-1.0](LICENSE), including the agent skill. Originally created by Noah Gift
(`noahgift/rdedupe`); maintained by Vittorio Distefano.
