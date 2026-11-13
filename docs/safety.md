# Safety model

This document states what rdedupe protects against, what it cannot, and what each
mutating command does. It applies to rdedupe 3.

## Operating model

rdedupe is designed for trees that are quiescent while you act on them: no other process
creates, modifies, renames or deletes files under the roots between `plan` and the end of
`apply`. It is safe to scan a live tree; results describe the moment of the scan and any
file that changes while being read becomes unresolved.

For mutation on a tree that cannot be stopped, scan and plan against a filesystem
snapshot to understand the duplicates, but apply against the live tree only during a
maintenance window. A snapshot does not make actions on the live tree safe.

Without knowing whether a tree is idle, an apply is still bounded: every action is
re-verified immediately before it runs, and a write that lands after that check ends up
in quarantine with its new bytes, restorable, while `purge` refuses anything that no
longer matches its keeper. The agent skill therefore proceeds with authorized quarantine
runs unless there is evidence of concurrent writers, and reports that activity was not
confirmed.

## What is checked before every action

For each action, in order, immediately before any change:

1. The target and keeper paths still resolve, through `realpath`, to exactly the planned
   paths (so a directory swapped for a symlink is refused).
2. Their current observations (device, inode, size, allocation, mtime, ctime, mode, link
   count) equal the planned ones.
3. They are different objects.
4. A complete byte comparison finds them identical now, with both unchanged during it.
5. The intent record is appended to the journal and synced.
6. The move is done with a no-replace rename into the quarantine directory; afterwards
   the moved object's identity is checked, and if anything else was swapped in, it is
   moved back and the action fails.

A failure at steps 1 to 4 skips the action and every later action in its group; nothing
is changed. `--force` skips only the confirmation prompt.

## What is not guaranteed

- **Races after the last check.** A write to the target between step 4 and step 6 is not
  detected. The object moves into quarantine with the new bytes, where it stays
  restorable; it is not destroyed. A write to the keeper after step 4 changes the
  surviving copy; `purge` re-verifies keeper equality and refuses in that case.
- **Other names and open handles.** Quarantine renames one path. Other hard links and
  open file descriptors still reach the object; a writer holding it open keeps writing
  to the quarantined object.
- **Metadata.** Equal content does not imply equal ownership, permissions, ACLs,
  extended attributes or timestamps. The keeper's metadata is what survives.
- **Whole-run atomicity.** Each action is individually journaled; a run is not one
  transaction. Recovery is per action.
- **Power-loss durability** depends on the filesystem honoring `fsync` on files and
  directories. rdedupe syncs each journal record before the step it describes and syncs
  both directories after every move. Process crashes at every journal boundary are
  tested (`tests/recovery.rs`); power loss is not.

## Quarantine

Each target moves to `<its directory>/.rdedupe-quarantine/<journal id>/<sequence>-<hex of
original name>`, on its own filesystem by construction, so no copy is ever made. The
quarantine root is `0700` and carries an `RDEDUPE-QUARANTINE` marker with fixed content;
only directories with that exact marker are skipped by later scans. Quarantine frees no
space.

## Restore

`restore` moves each quarantined object back with a no-replace rename. If the original
path is occupied, or the quarantine slot holds a different object, that action is a
conflict and nothing is moved. Running it again is safe.

## Purge

`purge` permanently removes a quarantined object only when the keeper still exists, is a
different object from the quarantined one, and is byte-identical to it now. Otherwise the
quarantined object may be the last copy and is kept. Running it again is safe.

## Extent sharing

`--action extent` asks the kernel (`FIDEDUPERANGE`) to compare and share each 16 MiB range
of target and keeper. The kernel compares each range under its own locks and shares it
only if identical, so a concurrent write can make a range fail but never corrupts data.
Files keep their own identities, names, metadata and link counts; a later write to either
one copies only the blocks written. Progress is journaled per range. There is no undo and
no need for one: content never changes. If the filesystem refuses, the action is
`failed` and nothing else is tried: no hard link, no deletion, no quarantine.

## Journals

The journal is append-only JSONL, opened with `O_APPEND`, locked with `flock` for the
whole run so that two processes can never execute or recover the same journal, and
synced after every record. A final record without a newline is a torn write from a crash
and is dropped (its step never started); any other malformed record stops every command.
Recovery never trusts a record alone: each decision compares the journal's expectations
with the objects found at the original and quarantine paths.

## Disk exhaustion and permissions

- Scratch exhaustion fails the scan explicitly; the workspace is removed.
- A full disk or permission error during `apply` makes that action fail or skip and is
  recorded; journal write or sync failure stops the run before the next mutation.
- rdedupe never changes permissions or ownership. A quarantine root is created atomically
  (marker written in a fresh private directory, then renamed into place without
  replacement); an existing `.rdedupe-quarantine` without the marker is never written
  to, and that action is skipped as stale.
- `--cache` writes only to an absent path or an existing single-link rdedupe cache, and
  re-checks the slot before publishing; it refuses any other file before scanning.

## Untrusted input

File names are arbitrary bytes and are carried as hex in every machine format and escaped
in human output, so they cannot inject terminal control sequences or fake records.

Plans and journals instruct rdedupe to move and delete files, so a well-formed file with a
matching checksum is not enough. Each plan footer and each journal record carries a seal:
a keyed BLAKE3 under a random 256-bit key in the user's private state directory
(`$RDEDUPE_STATE_DIR`, else `$XDG_STATE_HOME/rdedupe`, else `~/.local/state/rdedupe`;
directory 0700, key 0600). Journal seals are chained over the previous record, so records
cannot be edited, inserted, removed or reordered. A plan whose roots, protection or
actions were edited, or a journal pointing `purge` at an arbitrary file, is rejected even
when its checksum is recomputed, unless the editor can read the user's key, in which case
they already hold the user's file permissions.

Sealed input is still validated structurally: schema, encoding, scope (absolute paths,
byte-identical to their normalized form, under the plan's canonical roots), protection,
uniqueness and absence of keeper chains, a 1 MiB line limit, and for journals the exact
quarantine slot of each action inside a marker-owned quarantine root.

## Concurrent runs

Each journal is locked exclusively. Two applies of different plans over the same files
can still race between validation and rename; after every move `apply` re-checks that the
keeper still exists with its planned identity and moves the target back if it does not,
so a race never leaves content without a live copy. rdedupe still assumes the tree is
quiescent while it runs.
