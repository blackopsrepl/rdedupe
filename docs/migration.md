# Migrating from rdedupe 2.x to 3.0

3.0 replaces the engine, the library API, the CSV schema and the deletion workflow.

## Command changes

| 2.x | 3.0 |
| --- | --- |
| `rdedupe DIR` | unchanged (human report); `rdedupe scan DIR` is the new form |
| `rdedupe DIR --pattern P` | unchanged; `--pattern` still matches a substring of the full path |
| `rdedupe DIR --csv FILE` | unchanged, but the CSV schema changed (below) and existing files are never overwritten |
| `rdedupe DIR --delete --dry-run` | still reports only |
| `rdedupe DIR --delete [--force]` | refuses (exit 4). Use `rdedupe plan DIR --output PLAN` then `rdedupe apply --plan PLAN --journal JOURNAL`, which quarantines recoverably; `rdedupe purge --journal JOURNAL` removes permanently |
| hard-coded `--version` 2.0 | version from the package (`3.0.0`) |
| `env_logger` output (`RUST_LOG`, info level by default) | removed; stdout is results, `--progress` writes stage lines to stderr |

## Behavior changes

- **Equality** is a complete byte comparison. 2.x trusted MD5 of whole files read into
  memory; 3.0 never trusts a hash and never reads a whole file into memory.
- **Default removal** moves copies into a same-directory quarantine instead of unlinking;
  nothing is irreversible until `purge`.
- **Keeper choice** is deterministic and explainable (protected roots, preferred roots,
  optional newest, native path order) instead of traversal order.
- **Hard links** are recognized as one object; 2.x reported them as duplicates and could
  delete one name believing it reclaimed space.
- **Errors** are no longer skipped silently: unreadable files make the scan partial
  (exit 2) and partial scans cannot be planned.
- **Paths** are native bytes end to end; 2.x converted them lossily to UTF-8, so names
  with invalid UTF-8 could not be acted on correctly.

## CSV

2.x columns (`file_path,file_name,extension,size_bytes,size_mb,md5_hash,group_count,is_duplicate`, one row per file, timestamps included) are replaced by
`group,path_encoding,path_hex,path_display,size,allocated,device,inode,links,keeper,protected,reason,alias_of_hex`,
one row per member of a duplicate group, plus one row per hard-link alias of a member. Paths are lossless hex plus an escaped display
column. There is no hash column: groups are defined by comparison, not by a digest.

## Space accounting

2.x reported the size of every duplicate as reclaimed space. 3.0 reports
`logical_redundant_bytes` (logical size of eligible copies) and
`estimated_allocated_bytes` (allocated blocks of single-link copies), and calls both
estimates: quarantine frees nothing, and snapshots, other links, open files, compression
and shared extents can keep blocks allocated after a purge.

## Library API

The Polars DataFrame API (`run_with_dataframe`, `create_dataframe`, `collect_file_info`,
the `dataframe` module) and the `walk`/`find`/`checksum`/`find_duplicates` helpers are
removed. Use `rdedupe::scan(ScanConfig, Policy, &Cancellation)`, which returns a
`ScanResult` whose `store` streams groups and members, and the `plan`, `delete`,
`recovery` and `report` modules. Polars and MD5 are no longer dependencies.
