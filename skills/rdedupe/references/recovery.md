# Recovery

Use after a crash, kill, timeout, power loss, or any `apply`/`restore`/`purge` that did
not exit cleanly. Recovery decisions come from what is on disk now; the journal says what
was intended, not what happened.

## Authorization

Inspecting (`recover`) is report level. A request to undo, roll back or "put everything
back" authorizes `restore` (quarantine level). Nothing in recovery authorizes `purge`.

## Rules

1. Never start a new `apply` with the same journal (it refuses), and never re-plan and
   apply before the old journal is reconciled.
2. Start with `rdedupe recover --journal JOURNAL --format jsonl`. It is read-only and safe
   to repeat.
3. `restore` and `purge` are idempotent: each run re-derives every decision from the
   filesystem, so running them again after an interruption is safe.
4. To finish an interrupted apply, run `rdedupe apply --plan PLAN --journal JOURNAL
   --resume` with the same PLAN and JOURNAL, and only within the original authorization.
   Finished actions are not repeated; an action interrupted mid-way is reconciled from
   the filesystem (a moved object is recorded, an unmoved one is validated and moved, an
   extent action continues from `bytes_completed`). Anything indeterminate stops the
   resume with exit 4 before it changes anything more. `--resume` is idempotent: running
   it again after it finished changes nothing. A journal from a different plan is
   refused.
5. If another rdedupe process holds the journal, commands fail with "in use by another
   rdedupe process". Wait for it or confirm it is gone; never delete the journal.

## Journal states

Records are appended and synced before each step, so after a crash the journal always
holds the intent of any step that may have started.

| Last state | Meaning |
| --- | --- |
| `prepared` | Intent recorded. The move may or may not have happened. |
| `applied` | The move happened (extent: `bytes_completed` bytes shared so far). |
| `committed` | The action finished. |
| `failed` | The action stopped with an error after `prepared`. |
| `restored`, `purged` | Recorded by `restore` / `purge`. |

A final line without a newline is a record that was being written at the crash. It is
ignored, removed on the next `restore`/`purge`, and reported as
`truncated_tail_dropped: true`; its action never started. Any other malformed line is
corruption: every command stops. Escalate to a human with the journal file.

## Assessments from `recover`

| Assessment starts with | Do |
| --- | --- |
| `not moved; nothing to recover` | Nothing. |
| `moved but not recorded as applied (interrupted); restorable` | `restore` (if undoing) or leave quarantined. |
| `quarantined; restorable or purgeable` | Normal state after apply. |
| `interrupted move left the object under both names` | `restore` removes the extra name. |
| `in place` | Nothing. |
| `purged` | Nothing. |
| `extent sharing completed` | Nothing. |
| `extent sharing incomplete` | Files are intact and independent; `apply --resume` finishes sharing. |
| `quarantined; original path reoccupied` | Ask the user which file they want; restore will refuse. |
| `indeterminate` | Stop. Report the target path and the assessment to a human. |

`recover` exits 2 when any action is indeterminate.

`recover` prints one inspection per action and per run of states: an action that was
applied and later restored appears twice (its apply state, then `restored`). Read the
last inspection of each action. A plan action with no journal record at all never
started; its files were never touched and `recover` does not list it.

After a crash, either finish the run with `apply --resume` (same plan, same journal) or
undo it with `restore`. After a `restore`, do not reuse the old plan: files have moved and
back, so re-scan and re-plan, with a new journal, if the user still wants the cleanup.

## Common situations

- **apply timed out in your harness.** The process may still be running. Check for it;
  if it finished, read its journal with `recover`. Do not start a second apply.
- **Disk full or permission error mid-apply.** If a journal write failed, apply stops
  with exit 4 and `retry-safe: no` before its next mutation; otherwise the failing action
  is `failed` or `skipped_stale` and its file is untouched. Run `recover`, free space or
  fix permissions outside the scanned tree, then `apply --resume` (same plan and journal)
  or `restore`.
- **Keeper changed or vanished after apply.** `purge` refuses those copies. `restore`
  them, or ask the user.
- **Restore conflict.** Something new exists at the original path. Both versions are
  kept: the new file in place, the old copy still in quarantine. Report both paths.
- **Extent run interrupted.** Each range is compared and shared atomically by the kernel;
  there is nothing to undo. `apply --resume` shares the rest.

## Escalate when

- `recover` reports any `indeterminate` assessment.
- The journal is corrupt or missing while quarantine directories exist.
- A quarantine object is altered or a keeper is missing and the user has not decided.

Give the human: the journal path, the `recover` output, and the paths involved.
