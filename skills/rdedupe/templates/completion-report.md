# rdedupe completion report

- **Request and authorization level:** <report | quarantine | purge | extent>, roots: <...>
- **Versions:** rdedupe <version from `rdedupe --version`>, skill 3.0.0
- **Commands run (exit codes):**
  - `rdedupe scan ...` → <code>
  - <plan / apply / restore / purge / recover, if any>
- **Scan:** completeness <complete|partial|...>, exactness <exact|heuristic>,
  <N> paths, <N> objects, <N> hard-link aliases.
- **Unresolved paths:** <none | list with reasons>
- **Duplicates:** <N> groups, <N> eligible copies; <bytes> logical redundant bytes,
  <bytes> estimated allocated bytes (estimates of what a purge could free, not promises).
- **Keeper policy:** <default native path order | --protect ... | --prefer ...>
- **Plan / journal:** <paths, or "not created">
- **Outcomes:** quarantined <N>, shared <N>, skipped <N>, failed <N>,
  restored <N>, purged <N>, conflicts <N>.
- **Recovery (if any):** <journal path; last state per action; truncated tail dropped
  yes/no; restored/missing/conflict paths>
- **Read-back:** <how you verified keepers and targets on disk>
- **Space:** logical <bytes>; measured free-space change <bytes | not measured>.
- **Not done / remaining risks:** <e.g. purge not authorized; 2 unresolved paths;
  quarantine still holds N copies under .rdedupe-quarantine/>
