# CLI

```console
rdedupe scan /data                                  # report exact duplicate groups
rdedupe scan /data --format jsonl > groups.jsonl    # machine-readable
rdedupe plan /data --protect /data/originals --output cleanup.plan
rdedupe apply --plan cleanup.plan --journal cleanup.journal
rdedupe recover --journal cleanup.journal           # inspect (read-only)
rdedupe restore --journal cleanup.journal           # undo quarantine
rdedupe purge --journal cleanup.journal             # permanently remove quarantined copies
rdedupe plan /data --action extent --output share.plan   # share storage instead (Linux)
rdedupe capabilities
```

Every flag, record field and exit code is documented in
[`skills/rdedupe/references/commands.md`](../skills/rdedupe/references/commands.md),
which `tests/agent_skill.rs` checks against `--help` in both directions.

## Streams

- stdout: results only (reports, records, summaries).
- stderr: diagnostics, `--progress`, confirmation prompts.
- Machine output never contains progress or prompts.

## Prompts and terminals

`apply` and `purge` ask for `yes` on a terminal. When stdin is not a terminal they refuse
with exit 3 unless `--force` is given. `--force` never skips validation. `scan`, `plan`,
`restore` and `recover` never prompt.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Complete. |
| 2 | Partial: unresolved paths, skipped or failed actions, recovery conflicts or indeterminate states. Output is correct for what it covers. |
| 3 | Confirmation declined. |
| 4 | Error (invalid budget, unreadable root, rejected plan, locked or corrupt journal, refused output path). |
| 64 | Invalid invocation. |
| 130 | Cancelled (SIGINT). |
| 141 | stdout closed early. |

## Output files

`--output` and `--csv` create new files only (`O_CREAT|O_EXCL`): an existing path,
including a symlink or hard link to a scanned file, is refused with exit 4 and nothing is
truncated. A report that fails midway is removed rather than left half-written.

## Compatibility routes

`rdedupe DIR [--pattern P] [--csv FILE]` works as in 2.x and prints the human report.
`--delete --dry-run` reports and exits 0. `--delete` without `--dry-run` no longer
deletes; it exits 4 and prints the plan/apply migration. See [migration](migration.md).
