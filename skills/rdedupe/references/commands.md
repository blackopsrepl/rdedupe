# rdedupe 3.x command reference

Verify the installed version first: `rdedupe --version` prints `rdedupe 3.x.y`, and
`rdedupe capabilities` prints `key=value` lines: `platform`, `plan-schema`,
`journal-schema`, `machine-schema`, `fingerprint-layout`, `path-encoding`, `quarantine`
(starts with `supported` on Linux and macOS, otherwise `unsupported`) and `extent`
(`FIDEDUPERANGE (filesystem-dependent; …)` on Linux, otherwise `unsupported`). With
`--path DIR` it also probes that directory's filesystem with throwaway files and prints
`quarantine-probe=` and `extent-probe=` (`supported` or `unsupported (reason)`).
`rdedupe COMMAND --help` is the live source of truth; this file lists every flag it shows.

## Commands and flags

### `rdedupe scan ROOT...`

Reports exact duplicate groups. Never modifies the scanned tree (with `--cache` it writes
only the cache file).

| Flag | Meaning |
| --- | --- |
| `--format human\|csv\|jsonl\|nul` | Output format (default `human`). Use `jsonl` for automation. |
| `--output FILE` | Write the report to a new file; refuses existing paths, including symlinks and hard links to scanned files. |
| `--pattern TEXT` | Only inventory paths whose full path contains this byte substring. |
| `--protect DIR` | Never make members under DIR action targets (repeatable). Matches whole path components: `/data/keep` does not cover `/data/keeper`. |
| `--prefer DIR` | Prefer keepers under DIR; earlier occurrences win (repeatable). |
| `--prefer-newest` | Prefer the most recently modified member before native path order. Every supported (Unix) platform records a modification time with nanoseconds; times before 1970 are negative and order correctly; equal times fall back to native path order. |
| `--include-empty` | Make empty files eligible copies. By default empty-file groups are reported but every non-keeper is `retained: empty file`, so plans never act on them (they are often markers such as `__init__.py` or `.gitkeep`). |
| `--one-file-system` | Do not cross into other filesystems below each root. |
| `--cache FILE` | Fingerprint cache to refresh after the scan. Never consulted unless `--trust-cache`. FILE must be absent or an existing rdedupe cache; any other file, a symlink or a file with other hard links is refused (exit 4) before scanning and left untouched. |
| `--trust-cache` | Reuse cached fingerprints whose validators match. The scan is `heuristic` as soon as one is reused (`cached_fingerprints_reused` > 0); plans refuse it. |
| `--fresh` | Ignore existing cache contents (the cache is still rewritten). |
| `--progress` | Stage progress on stderr. |
| `--memory-mib N` | Working-set budget: SQLite page cache + read buffers + queue + 3 MiB allowance (default 64). |
| `--queue N` | Queue depth: records per batch handed between stages, 16 to 65536 (default 256). Each record is charged 4 KiB of the memory budget. |
| `--buffer-kib N` | Size of each read buffer, 4 to 8192 (default 256). |
| `--readers N` | Concurrent fingerprint readers, 1 to 64 (default 4); comparison batches `readers + 1` members per pass. On rotational disks prefer `--readers 1`. |
| `--descriptors N` | Maximum open file descriptors, enforced as the process `RLIMIT_NOFILE` soft limit (never raised); must be at least 13 + readers (default 32). |
| `--temp-mib N` | Maximum scratch database size, at least 16 (default 8192). Exceeding it fails the scan with "database or disk is full". |
| `--scratch-dir DIR` | Where the private scratch workspace is created (default: the system temporary directory). It is removed on exit. |

### `rdedupe plan ROOT... --output PLAN`

Scans like `scan` (same selection, policy, cache and budget flags, including
`--progress`) and saves a plan. Refuses partial, cancelled and heuristic scans and
never overwrites an existing file. On a partial scan it prints the human report to
stderr, writes no plan, and exits 4.

| Flag | Meaning |
| --- | --- |
| `--output FILE` | New plan file. |
| `--action quarantine\|extent` | `quarantine` (default) moves copies into a recoverable quarantine; `extent` shares their storage with the keeper (Linux, FIDEDUPERANGE). |

### `rdedupe apply --plan PLAN --journal JOURNAL`

Verifies the plan, then for each action re-checks current facts (same paths, same
identities and metadata as planned, keeper and target distinct, bytes identical now),
journals the intent durably, and acts. Refused actions change nothing.

| Flag | Meaning |
| --- | --- |
| `--plan FILE` | Plan from `rdedupe plan`. |
| `--journal FILE` | New journal; must not exist (unless `--resume`). Keep it. |
| `--force` | Skip the confirmation prompt (required when stdin is not a terminal). Validation still runs. |
| `--buffer-kib N` | Buffer for the pre-action byte comparison (default 256). |
| `--resume` | Continue an interrupted apply of the same plan in its existing JOURNAL: finished actions are not repeated, interrupted ones are reconciled from the filesystem, and anything indeterminate stops the run (exit 4) with a pointer to `recover`/`restore`. A journal from another plan is refused. |
| `--scratch-dir DIR` | Where the plan-verification index is created (default: the system temporary directory). |
| `--temp-mib N` | Maximum size of the plan-verification index (default 8192). |
| `--format human\|jsonl` | Result format. |

### `rdedupe restore --journal JOURNAL`

Moves quarantined objects back. Never replaces an entry that now exists at an original
path. Idempotent.

| Flag | Meaning |
| --- | --- |
| `--journal FILE` | Journal written by `apply`. |
| `--format human\|jsonl` | Result format. |

### `rdedupe purge --journal JOURNAL`

Permanently removes quarantined objects. Each removal requires the keeper to still exist,
to be a different object, and to be byte-identical to the quarantined copy; otherwise
that copy is kept. Idempotent.

| Flag | Meaning |
| --- | --- |
| `--journal FILE` | Journal written by `apply`. |
| `--force` | Skip the confirmation prompt. Keeper verification still runs. |
| `--buffer-kib N` | Buffer for the keeper comparison (default 256). |
| `--format human\|jsonl` | Result format. |

### `rdedupe recover --journal JOURNAL`

Read-only. For each action, prints its last recorded state and where its object is now.

| Flag | Meaning |
| --- | --- |
| `--journal FILE` | Journal written by `apply`. |
| `--format human\|jsonl` | Result format. |

### `rdedupe capabilities`

Prints the version, platform, schemas and platform-level quarantine/extent support.

| Flag | Meaning |
|---|---|
| `--path DIR` | Also probe quarantine and extent support on DIR's filesystem, using throwaway files in a private scratch directory there (removed afterwards). |

### Legacy form (2.x compatibility)

| Flag | Meaning |
| --- | --- |
| `rdedupe [DIR]...` | Same as `scan` with human output (default directory `.`). |
| `--pattern TEXT` | As in `scan`. |
| `--csv FILE` | Also write the CSV report to a new file. |
| `--delete` | No longer deletes: exits 4 with the plan/apply migration message. |
| `--dry-run` | With `--delete`: report only, exit 0. |
| `--force` | Accepted with `--delete`; deletion still refuses. |

## Streams and exit codes

stdout carries results only. stderr carries diagnostics, prompts and `--progress`.

| Code | Meaning |
| --- | --- |
| 0 | Complete: scan complete, every action succeeded, recovery clean. |
| 2 | Partial: unresolved paths, skipped or failed actions, restore or purge conflicts, indeterminate journal states. Results printed are still correct. |
| 3 | Confirmation declined (or stdin was not a terminal and `--force` was absent). Nothing changed. |
| 4 | Error: invalid budget, unreadable root, plan rejected, journal locked or corrupt, refused output path. |
| 64 | Invalid invocation (unknown flag, missing argument). |
| 130 | Cancelled by SIGINT. A cancelled scan prints a `cancelled` summary. |
| 141 | stdout closed early (for example piped into `head`). |

Every exit 4 prints two stderr lines: `error: MESSAGE`, then
`error-category: CATEGORY; retry-safe: yes|no (...)`. CATEGORY is one of `traversal`,
`metadata`, `read`, `mutation`, `changed`, `budget`, `partial_execution`, `config`,
`plan`, `journal`, `capability`, `cancelled`, `not_authoritative`, `output`, `internal`.
`retry-safe: yes` means nothing on disk changed and the same command may be retried after
fixing the cause. `retry-safe: no` (mutation, partial execution, journal) means files may
have changed: run `rdedupe recover --journal JOURNAL` before anything else.

## Paths

Machine output carries every path as `{"encoding":"unix-bytes","data":"<hex>"}`: the
hex of the exact native bytes, so names with newlines, invalid UTF-8 or control bytes
round-trip. Decode with `bytes.fromhex(data)` (Python) or `hex::decode`. Human and CSV
`path_display` columns escape control bytes (`\n`, `\xff`, `\x1b`) and are for reading
only; never pass a display string back to a command. `--format nul` emits raw native
paths of eligible copies, each followed by a NUL byte, for `xargs -0`.

## Machine records (`rdedupe-machine-v1`)

Every line is one JSON object with a `record` field. Identify records by that field,
never by key order or line position within a kind.

`scan --format jsonl`, in order:

- `summary` (first): `schema`, `binary_version`, `completeness` (`complete`/`partial`/`cancelled`/`failed`), `exactness` (`exact`/`heuristic`), `unresolved` (count), `cache_provenance`, and `metrics` (`sqlite_page_cache_bytes` is the page-cache cap derived
from the budget, not memory actually used): `paths`, `objects`, `aliases`, `symlinks_skipped`, `special_files_skipped`, `owned_dirs_skipped`, `candidate_objects`, `eliminated_by_size`, `eliminated_by_fingerprint` (unique sampled or full-content digest), `eliminated_by_comparison` (shared a digest but matched nothing byte for byte), `fingerprint_bytes_read`, `full_hash_bytes_read`, `comparison_bytes_read`, `cached_fingerprints_reused`, `groups`, `participants`, `eligible_copies`, `logical_redundant_bytes`, `estimated_allocated_bytes`, `sqlite_page_cache_bytes`, `inventory_bytes` (final size of the on-disk inventory; above `sqlite_page_cache_bytes` the inventory was paged rather than held in memory).
- `member` (one per group member, ordered by group then path): `group`, `path`, `identity` (`device`, `inode`), `size`, `allocated`, `links`, `is_keeper`, `protected`, `reason`, `metadata_differs_from_keeper` (list of `permissions`, `owner`, `group`, `extended attributes` that differ from the keeper's; empty for the keeper). Removing a copy loses its metadata; plan reasons say so.
- `alias` (right after its member, one per other hard-linked path to the same file): `group`, `path`, `alias_of` (the member's path), `protected`. An alias is the same file under another name: it is never a keeper or a copy, and removing it frees nothing.
- `unresolved`: `path`, `operation` (`traverse`, `metadata`, `fingerprint`, `compare`), `message`, `retry_safe`.
- `end` (last): `schema`, `groups`, `unresolved`. A stream without `end` was cut short.

`apply --format jsonl`: one `outcome` per action (`group`, `target`, `kind`, `status`,
`logical_bytes`, `message`), then `apply_summary` (`actions`, `quarantined`, `shared`,
`skipped`, `failed`, `logical_bytes_moved_or_shared`). Statuses: `quarantined`, `shared`,
`skipped_stale` (current facts no longer match the plan, or a precondition such as creating the quarantine directory failed before the action started; nothing changed; `message` says which),
`skipped_group` (an earlier action in the group was refused), `failed` (the mutation
started and stopped; see the journal).

`restore`/`purge --format jsonl`: one `finding` per action (`action`, `target`,
`location`, `result`), then `restore_summary` or `purge_summary` (`actions`, `restored`,
`purged`, `already_in_place`, `not_quarantined`, `missing`, `conflicts`,
`truncated_tail_dropped`).

`recover --format jsonl`: one `inspection` per action and state run (`action`, `group`,
`kind`, `last_state`, `bytes_completed`, `target`, `location`, `assessment`).

Locations: `in_place`, `in_quarantine`, `in_quarantine_original_occupied`, `both_names`,
`quarantine_altered`, `replaced`, `absent`. Journal states: `prepared`, `applied`,
`committed`, `failed`, `restored`, `purged`.

## Quarantine layout

Each target moves to `<its directory>/.rdedupe-quarantine/<journal id>/<action>-<hex>`,
where `<action>` is the 16-digit hex action number and `<hex>` the hex of the original
name, truncated to 160 characters (so a long name may lose its extension there). Restore
and purge locate objects through the journal, not by name, and refuse any journal record
whose quarantine path is not exactly this slot. A `.rdedupe-quarantine` that rdedupe did
not create (no marker) is never written to or chmodded: that action is `skipped_stale`.
Quarantine directories with a valid marker are excluded from scans and counted in
`owned_dirs_skipped`. Emptied quarantine directories are removed by `restore` and
`purge`; one that still holds objects of any journal keeps its marker.

## Plans (`rdedupe-plan-v1`)

JSONL. Line 1 is the header (`schema`, `binary_version`, `roots`, `protect`, `prefer`,
`prefer_newest`, `completeness`, `exactness`, `action`). Then one action per line
(`group`, `target`, `target_observation`, `keeper`, `keeper_observation`, `reason`).
The header also carries `include_empty`. The last line is the footer
(`schema: rdedupe-plan-footer-v1`, `actions`, `checksum`, `seal`). The checksum detects
corruption; the seal is a keyed BLAKE3 under the user's private key, so a plan edited by
anyone (even with the checksum recomputed) or created by another user is rejected.
Validation also rejects out-of-scope, relative or non-normalized paths (`//`, `/./`,
trailing `/`), protected targets, duplicate targets, keepers that are also targets,
unknown schemas or encodings, and partial or heuristic headers. While `apply` runs, its
verification index lives in the system temporary directory (`$TMPDIR`).

## Key and state directory

Plans and every journal record are sealed with a random key created on first use at
`$RDEDUPE_STATE_DIR/seal.key`, else `$XDG_STATE_HOME/rdedupe/seal.key`, else
`~/.local/state/rdedupe/seal.key` (directory 0700, file 0600). `apply`, `restore`,
`purge` and `recover` need the same key that wrote the plan or journal: run them as the
same user with the same environment. Deleting the key makes existing plans and journals
unusable (quarantined files stay on disk and can be moved back by hand).

## Budgets

Defaults suit most machines. For very large trees or small machines use
`--memory-mib 16 --buffer-kib 64 --readers 1 --descriptors 16`; the minimum accepted is
`--memory-mib 6 --buffer-kib 4 --readers 1 --descriptors 14 --temp-mib 16 --queue 16`
(`--memory-mib 7` with the default queue). The budget
bounds the engine's working set; whole-process RSS adds the binary and allocator overhead
(measured at about 10.7 MiB peak for a 16 MiB budget on a 4,000-member group plus two
256 MiB files; see docs/benchmarks.md).
