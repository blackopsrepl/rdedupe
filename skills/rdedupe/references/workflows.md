# Workflows

Each recipe lists prerequisites, side effects, stop conditions and the checks that prove
completion. Replace `ROOT`, `SOURCE`, `PLAN`, `JOURNAL`, `CACHE` and `DIR` with real
paths. Put PLAN, JOURNAL and output files outside ROOT: they are ordinary files to a later
scan of ROOT (rdedupe excludes only its scratch workspace, its cache file and marked
quarantine directories), and keeping them apart keeps reports about the user's data
only. Existing PLAN, JOURNAL and `--output` paths are refused, never overwritten.
`tests/agent_skill.rs` runs every command below, in order, against a fixture.

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
- Side effects: as recipe 1; the workspace is created under DIR.
- Run: `rdedupe scan ROOT --format jsonl --memory-mib 16 --buffer-kib 64 --readers 1 --descriptors 16 --scratch-dir DIR --progress > scan.jsonl`
- Stop: exit 4 with "database or disk is full" means `--temp-mib` or the scratch
  filesystem is too small; raise `--temp-mib` or pick another `--scratch-dir`. Exit 4
  with `error-category: budget` names the budget that cannot work.
- Complete when: as recipe 1, and DIR contains nothing left by rdedupe. The summary's
  `inventory_bytes` above `sqlite_page_cache_bytes` shows the inventory was paged to disk.

## 3. Protected-source cleanup into quarantine (quarantine level)

- Prerequisites: explicit request to remove copies; the user named what to keep (or accepts
  the default keeper order); no evidence that the tree is being written (see
  `safety.md`; do not ask about it as a routine question).
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

- Prerequisites: a JOURNAL from `apply`; a request to undo, roll back or put files back.
- Side effects: quarantined objects move back to their original paths; empty quarantine
  directories are removed; `restored` records are appended to JOURNAL.
- Run: `rdedupe restore --journal JOURNAL --format jsonl > restore.jsonl; echo "exit=$?"`
- Stop: exit 4 (journal corrupt, locked by another process, or sealed by another user).
  Never edit or delete the journal.
- Exit 2: at least one conflict (the original path is occupied, or the quarantine object
  was altered) or missing object. Those are left exactly as found; report them.
- Complete when: each `restored` path reads back with its baseline hash, and a second
  `restore` reports `restored: 0`.

## 5. Purge (purge level, separate authorization)

- Prerequisites: the user explicitly asked for permanent deletion after reviewing the
  quarantine. A cleanup request is not enough.
- Side effects: quarantined objects are deleted for good; `purged` records are appended.
- Run: `rdedupe purge --journal JOURNAL --force --format jsonl > purge.jsonl; echo "exit=$?"`
- Stop: exit 4 as in recipe 4.
- Exit 2: a keeper is missing or changed, or a quarantine object was altered; those
  copies were kept because they may be the last ones. Report them; never delete them by
  hand.
- Complete when: each `purged` object is gone and every keeper still reads back with its
  baseline hash.

## 6. Applying a saved plan later (quarantine level)

A plan can be reviewed and applied later; `apply` re-validates everything.

- Prerequisites: PLAN was written by the `plan` command run as the same user (the plan is sealed
  with the user's key); the user authorized applying it.
- Side effects: as recipe 3.
- Run (the plan was saved earlier with `rdedupe plan ROOT --protect SOURCE --output PLAN`):
  1. `python3 scripts/summarize.py PLAN`: check `completeness` is `complete`, `exactness`
     is `exact`, the action kind and root list are what the user approved, and
     `targets_under_protected_roots` is 0.
  2. `rdedupe apply --plan PLAN --journal JOURNAL --force --format jsonl > apply-later.jsonl; echo "exit=$?"`
- Stop: exit 4 with "plan seal mismatch" or any other rejection: the plan was edited,
  truncated or made by another user; nothing changed and no journal was created. Do not
  repair the plan; re-scan and re-plan.
- Exit 2 with `skipped_stale` outcomes: the tree changed since planning (a target or keeper
  was modified, replaced, re-linked or removed). Those files are untouched and the rest of
  their group stopped. Re-scan and re-plan if the user still wants them handled.
- Complete when: as recipe 3, plus every `skipped_stale` target reported with its message.

## 7. Extent sharing (extent level, Linux)

- Prerequisites: `rdedupe capabilities --path ROOT` prints `extent-probe=supported`
  (Btrfs, XFS with reflink); the user asked to share storage while keeping every file.
- Side effects: no file is moved, renamed or deleted; their extents become shared, and a
  later write to either file copies only the written blocks. A plan and journal are
  created.
- Run: `rdedupe plan ROOT --action extent --output PLAN`, then
  `rdedupe apply --plan PLAN --journal JOURNAL --force --format jsonl > extent.jsonl`.
- Stop: outcomes with `failed` and a FIDEDUPERANGE message mean the filesystem refused;
  files are untouched. Do not retry as quarantine without new authorization.
- Complete when: every `shared` target still exists with its own inode and identical
  content. `filefrag -v FILE` shows `shared` extents where available.

## 8. Repeat scans with a cache (report level)

- Prerequisites: a writable location for CACHE outside ROOT that is either absent or an
  rdedupe cache (anything else is refused and left untouched).
- Side effects: CACHE is created or replaced atomically after a complete scan; an
  interrupted scan leaves the old CACHE intact.
- Run: `rdedupe scan ROOT --cache CACHE --format jsonl > cached.jsonl` refreshes CACHE;
  results stay exact. `rdedupe scan ROOT --cache CACHE --trust-cache --format jsonl > quick.jsonl`
  reuses fingerprints and marks the scan `heuristic` as soon as one cached fingerprint
  replaced a read: suitable for a quick look, not for planning. `rdedupe scan ROOT --cache CACHE --fresh --format jsonl > fresh.jsonl` ignores
  CACHE contents.
- Stop: exit 4 "refusing --cache": CACHE is not an rdedupe cache; choose another path.
  Do not use `--trust-cache` on network or FAT filesystems.
- Complete when: you report `exactness` and `cache_provenance` from the summary.

## 9. Interrupted or timed-out run (recovery)

- Prerequisites: the JOURNAL of the interrupted command; no rdedupe process still using
  it.
- Side effects: `recover` none. `apply --resume` continues the original apply under its
  original authorization.
- Run:
  1. `rdedupe recover --journal JOURNAL --format jsonl > recover.jsonl; echo "exit=$?"`
  2. Read `recovery.md` for each assessment.
  3. To finish the run: `rdedupe apply --plan PLAN --journal JOURNAL --resume --force --format jsonl > resume.jsonl; echo "exit=$?"`.
     To undo it instead: recipe 4.
- Stop: `recover` exits 2 (an `indeterminate` assessment), `--resume` exits 4, or the
  journal is locked or corrupt: escalate to a human with the journal path and
  `recover.jsonl`.
- Complete when: `recover` exits 0 and every action's last inspection is `quarantined`,
  `in place`, `restored`, `purged` or `extent sharing completed`.

## 10. Partial scan

- Prerequisites: as recipe 1.
- Side effects: none.
- Run: `rdedupe scan ROOT --format jsonl > partial.jsonl; echo "exit=$?"`
- Exit 2: unreadable files or directories produced `unresolved` records. Report each
  path with its `operation` and `message`. Ask the user whether to fix permissions,
  exclude the path (scan narrower roots or use `--pattern`), or stop. Never chmod or chown
  on your own initiative.
- Stop: `plan` on a partial scan exits 4 and prints the human report; there is no flag
  to override this.
- Complete when: you reported the groups (exact for what was read), the unresolved paths
  and the `partial` completeness.
