# Workflows

Each recipe lists prerequisites, side effects, stop conditions and the checks that prove
completion. Replace `ROOT`, `PLAN`, `JOURNAL` with real paths. PLAN and JOURNAL must be
outside the scanned roots or will simply be ignored there; they are never treated as
duplicates of each other because each run creates a new file.

## 1. Exact duplicate report (report level)

- Prerequisites: read access to ROOT.
- Side effects: none in ROOT; a scratch workspace is created and removed.
- Run: `rdedupe scan ROOT --format jsonl > scan.jsonl; echo "exit=$?"`
- Stop: exit 4 (read the stderr message), or no `end` record (output was cut).
- Exit 2: report the unresolved records alongside the groups; the groups shown are exact.
- Complete when: you state completeness, groups, eligible copies,
  `logical_redundant_bytes`, and every unresolved path. `python3 scripts/summarize.py scan.jsonl`
  prints all of these with decoded paths.

## 2. Large tree on a small machine (report level)

- Prerequisites: free space for scratch (about 1 KiB per file) on `--scratch-dir`.
- Run: `rdedupe scan ROOT --format jsonl --memory-mib 16 --buffer-kib 64 --readers 1 --descriptors 16 --scratch-dir DIR --progress > scan.jsonl`
- Stop: exit 4 with "database or disk is full" means `--temp-mib` or the scratch
  filesystem is too small; raise `--temp-mib` or pick another `--scratch-dir`.
- Complete when: as recipe 1, and DIR contains nothing left by rdedupe.

## 3. Protected-source cleanup into quarantine (quarantine level)

- Prerequisites: explicit request to remove copies; the user named what to keep; the tree
  is quiescent.
- Side effects: eligible copies move into `.rdedupe-quarantine/` beside them; a plan and a
  journal are created.
- Run:
  1. `rdedupe scan ROOT --protect SOURCE --format jsonl > scan.jsonl` (same policy flags as the plan, so the report matches it)
  2. `rdedupe plan ROOT --protect SOURCE --output PLAN`
  3. `python3 scripts/summarize.py PLAN` and confirm `targets_under_protected_roots` is 0.
  4. Record a baseline for read-back, e.g. `sha256sum` of every file under ROOT.
  5. `rdedupe apply --plan PLAN --journal JOURNAL --force --format jsonl > apply.jsonl; echo "exit=$?"`
  6. `python3 scripts/summarize.py apply.jsonl`
- Stop: `plan` exits non-zero (partial scan, existing PLAN); the plan contains a target you
  did not expect; `apply` exits 4 (plan rejected or journal exists).
- Exit 2 from apply: some actions were skipped or failed and left their files untouched;
  list them with their messages.
- Complete when: every `quarantined` target is absent from its original path, its
  quarantined object has the baseline hash, every keeper and protected file matches the
  baseline, and you have reported PLAN and JOURNAL.

## 4. Restore (quarantine level)

- Run: `rdedupe restore --journal JOURNAL --format jsonl > restore.jsonl; echo "exit=$?"`
- Side effects: quarantined objects move back to their original paths.
- Exit 2: at least one conflict (the original path is occupied, or the quarantine object
  was altered) or missing object. Those are left exactly as found; report them.
- Complete when: each `restored` path reads back; a second restore reports
  `restored: 0`.

## 5. Purge (purge level, separate authorization)

- Prerequisites: the user explicitly asked for permanent deletion after reviewing the
  quarantine.
- Run: `rdedupe purge --journal JOURNAL --force --format jsonl > purge.jsonl; echo "exit=$?"`
- Exit 2: a keeper is missing or changed, or a quarantine object was altered; those
  copies were kept because they may be the last ones. Report them; never delete them by
  hand.
- Complete when: each `purged` object is gone, every keeper still reads back.

## 6. Extent sharing (extent level, Linux)

- Prerequisites: the `extent=` line of `rdedupe capabilities` starts with `FIDEDUPERANGE`
  (`unsupported` means the platform cannot); the filesystem
  supports it (Btrfs, XFS with reflink); the user asked to share storage while keeping
  every file.
- Side effects: no file is moved, renamed or deleted; their extents become shared, and a
  later write to either file copies only the written blocks.
- Run: `rdedupe plan ROOT --action extent --output PLAN`, then `apply` as in recipe 3.
- Stop: outcomes with `failed` and a FIDEDUPERANGE message mean the filesystem refused;
  files are untouched. Do not retry as quarantine without new authorization.
- Complete when: every `shared` target still exists with its own inode and identical
  content. `filefrag -v FILE` shows `shared` extents where available.

## 7. Repeat scans with a cache (report level)

- `rdedupe scan ROOT --cache CACHE --format jsonl` refreshes CACHE; results stay exact.
- `--trust-cache` reuses fingerprints for speed and marks the scan `heuristic`: suitable
  for a quick look, not for planning. `--fresh` ignores CACHE contents.
- Complete when: you report `exactness` and `cache_provenance` from the summary.

## 8. Interrupted or timed-out run

Follow `recovery.md`. Always start with `rdedupe recover --journal JOURNAL`.

## 9. Partial scan

- Unreadable files or directories produce `unresolved` records and exit 2.
- Report them. Ask the user whether to fix permissions, exclude the path (scan narrower
  roots or use `--pattern`), or stop. Never chmod or chown on your own initiative.
- A plan needs a complete scan; there is no flag to override this.
