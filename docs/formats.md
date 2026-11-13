# Formats

All machine formats are UTF-8 JSON Lines: one JSON object per `\n`-terminated line.
Version identifiers are part of every format. Readers must reject unknown versions;
additive fields may appear within a version, and readers must ignore fields they do not
know.

## Native paths

Every path in a machine format is an object:

```json
{"encoding": "unix-bytes", "data": "2f746d702f61"}
```

`data` is the lowercase hex of the exact bytes the kernel uses, so any name (invalid
UTF-8, newlines, control bytes) round-trips. A NUL byte is invalid. Readers on a platform
whose encoding differs reject the path. Display strings (human output, CSV
`path_display`) keep valid UTF-8 readable and escape the rest: `\\` for a backslash;
`\n`, `\r`, `\t`; `\xNN` for other ASCII controls and for bytes that are not valid
UTF-8; `\u{XXXX}` for C1 controls and invisible or bidirectional formatting characters
(such as U+202E, which could disguise a name). They are never parsed back.

## Scan output: `rdedupe-machine-v1`

`summary`, then `member*` (by group, then path), then `unresolved*` (by path), then
`end`. Field lists are in `skills/rdedupe/references/commands.md`. `end` repeats the
group and unresolved counts; a stream without it is truncated.

## CSV

Header: `group,path_encoding,path_hex,path_display,size,allocated,device,inode,links,keeper,protected,reason`.
Standard RFC 4180 quoting. `path_hex` is lossless; `path_display` is for people.

## NUL list

Raw native paths of eligible copies (never keepers or protected members), each followed
by one NUL byte.

## Plans: `rdedupe-plan-v1`

1. Header: `schema`, `binary_version`, `roots` (canonical), `protect`, `prefer`,
   `prefer_newest`, `include_empty`, `completeness` (must be `complete`), `exactness` (must be `exact`),
   `action` (`quarantine` or `extent`).
2. One action per line: `group`, `target`, `target_observation`, `keeper`,
   `keeper_observation`, `reason`. An observation is `identity` (`device`, `inode`),
   `size`, `allocated`, `modified_ns`, `changed_ns`, `mode`, `links`.
3. Footer: `schema: "rdedupe-plan-footer-v1"`, `actions` (count), `checksum` (BLAKE3 hex
   over every preceding line including its newline), `seal` (keyed BLAKE3 hex, key = the
   user's `seal.key`, over the domain `rdedupe-plan-v1`, a NUL byte and the checksum hex).

Validation (`verify_plan`, run again by `apply` on the same open file):

- lines at most 1 MiB, every line newline-terminated, nothing after the footer;
- known schemas and path encoding; complete and exact header;
- every path absolute and byte-identical to its normalized form (no `.`/`..`, `//` or
  trailing `/`), under one of the header's roots;
- no target under a protected root, target ≠ keeper;
- each target appears once; no keeper is also a target (no chains or cycles), checked
  with a disk-backed index so plan size does not bound memory;
- footer count, checksum and seal match.

During execution each action line is matched against the verified index before use, so a
plan modified after verification stops at the first altered action. The checksum detects
corruption; the seal rejects plans not produced by this user's rdedupe. Neither replaces
the live revalidation of every action. The verification index lives under `$TMPDIR`.

## Journals: `rdedupe-journal-v1`

One record per line: `schema`, `sequence` (strictly increasing), `action` (plan action
number), `group`, `state`, `kind` (`quarantine`/`extent`), `target`, `keeper`,
`quarantine` (destination path or null), `target_observation`, `bytes_completed`,
`message`, `seal`. The seal is a keyed BLAKE3 (domain `rdedupe-journal-v1`) over the
previous record's seal, a newline, and this record serialized with an empty `seal`; the
first record chains from the empty string. Sequences must be 1, 2, 3, …; any record that
fails its seal or sequence makes every command refuse the journal.

State machine per action:

```
quarantine: prepared → applied → committed        (or prepared → failed,
                                                   or prepared → restored if the keeper
                                                   changed during the move)
extent:     prepared → applied(bytes)* → committed (or … → failed)
recovery:   … → restored | purged
```

Durability ordering for a quarantine action: create the quarantine directory and marker
(synced); append+fsync `prepared`; rename without replacement; fsync both directories;
append+fsync `applied`; append+fsync `committed`.

Corruption handling: a final line without `\n` is a torn record and is truncated away by
the next `restore`/`purge`; any other unparsable line, or a record with an unknown schema,
is corruption and every command refuses the journal.

## Fingerprint cache: `rdedupe-cache-v2`

Header `{schema, layout, authority}` where `layout` is the fingerprint layout
(`blake3-sample-layout-v1`); then one entry per path: `path`, `device`, `inode`, `size`,
`modified_ns`, `changed_ns`, `digest`. Written to a temporary sibling, synced, and
published only into an absent path (hard link, never replacing) or over a single-link
file that still carries an rdedupe cache header and the identity seen before the scan. A file with another schema or layout, or any corrupt entry, is ignored as a
whole. Entries are used only with `--trust-cache`, and only when every validator matches.
