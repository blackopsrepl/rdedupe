#!/usr/bin/env python3
"""Summarize rdedupe 3.x machine output with decoded paths.

usage: python3 summarize.py FILE

FILE is the stdout of `scan --format jsonl`, `apply|restore|purge|recover --format jsonl`,
or a plan written by `rdedupe plan`. Prints one JSON object. `path` is a display string
(bytes that are not valid UTF-8 appear as backslash-x hex escapes); `path_hex` carries the exact native
bytes and is the only form to pass back to the filesystem.
Exits 1 when the input is truncated or not rdedupe output. Standard library only.
"""
import json
import sys


def decode(path):
    if path.get("encoding") != "unix-bytes":
        raise ValueError(f"unsupported path encoding {path.get('encoding')!r}")
    raw = bytes.fromhex(path["data"])
    return {"path": raw.decode("utf-8", errors="backslashreplace"), "path_hex": path["data"]}


def load(name):
    with open(name, "rb") as handle:
        data = handle.read()
    if data and not data.endswith(b"\n"):
        raise ValueError("input does not end with a newline: output was cut short")
    return [json.loads(line) for line in data.splitlines() if line.strip()]


def scan(records):
    summary = records[0]
    if records[-1].get("record") != "end":
        raise ValueError("scan output has no end record: output was cut short")
    groups = {}
    for record in records:
        if record.get("record") == "member":
            groups.setdefault(record["group"], []).append({
                **decode(record["path"]),
                "keeper": record["is_keeper"],
                "protected": record["protected"],
                "size": record["size"],
                "links": record["links"],
                "reason": record["reason"],
            })
    unresolved = [
        {**decode(r["path"]), "operation": r["operation"], "message": r["message"]}
        for r in records if r.get("record") == "unresolved"
    ]
    metrics = summary["metrics"]
    return {
        "kind": "scan",
        "completeness": summary["completeness"],
        "exactness": summary["exactness"],
        "paths": metrics["paths"],
        "objects": metrics["objects"],
        "aliases": metrics["aliases"],
        "groups": metrics["groups"],
        "symlinks_skipped": metrics["symlinks_skipped"],
        "special_files_skipped": metrics["special_files_skipped"],
        "owned_dirs_skipped": metrics["owned_dirs_skipped"],
        "eligible_copies": metrics["eligible_copies"],
        "logical_redundant_bytes": metrics["logical_redundant_bytes"],
        "estimated_allocated_bytes": metrics["estimated_allocated_bytes"],
        "cache_provenance": summary["cache_provenance"],
        "unresolved": unresolved,
        "duplicate_groups": [{"group": k, "members": groups[k]} for k in sorted(groups)],
    }


def plan(records):
    header, footer = records[0], records[-1]
    if footer.get("schema") != "rdedupe-plan-footer-v1":
        raise ValueError("plan has no footer: it is truncated")
    actions = records[1:-1]
    protect = [bytes.fromhex(p["data"]) for p in header["protect"]]

    def protected(path):
        raw = bytes.fromhex(path["data"])
        return any(raw == root or raw.startswith(root.rstrip(b"/") + b"/") for root in protect)

    return {
        "kind": "plan",
        "action": header["action"],
        "roots": [decode(p)["path"] for p in header["roots"]],
        "protect": [decode(p)["path"] for p in header["protect"]],
        "targets_under_protected_roots": sum(1 for a in actions if protected(a["target"])),
        "actions": len(actions),
        "logical_bytes": sum(a["target_observation"]["size"] for a in actions),
        "targets": [
            {"target": decode(a["target"])["path"], "keeper": decode(a["keeper"])["path"], "group": a["group"]}
            for a in actions
        ],
    }


def results(records):
    out = {"kind": "results", "records": {}, "statuses": {}, "items": []}
    for record in records:
        kind = record.get("record")
        out["records"][kind] = out["records"].get(kind, 0) + 1
        if kind in ("outcome", "finding", "inspection"):
            item = {k: v for k, v in record.items() if k not in ("record", "target")}
            item.update(decode(record["target"]))
            out["items"].append(item)
            label = record.get("status") or record.get("result") or record.get("assessment")
            out["statuses"][label] = out["statuses"].get(label, 0) + 1
        elif kind and kind.endswith("_summary"):
            out["summary"] = {k: v for k, v in record.items() if k != "record"}
    return out


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    try:
        records = load(sys.argv[1])
        if not records:
            raise ValueError("empty input")
        first = records[0]
        if first.get("record") == "summary" and first.get("schema") == "rdedupe-machine-v1":
            result = scan(records)
        elif first.get("schema") == "rdedupe-plan-v1":
            result = plan(records)
        elif all("record" in r for r in records):
            result = results(records)
        else:
            raise ValueError("not rdedupe 3.x output")
    except (ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"summarize.py: {error}", file=sys.stderr)
        sys.exit(1)
    json.dump(result, sys.stdout, indent=2, ensure_ascii=True)
    print()


if __name__ == "__main__":
    main()
