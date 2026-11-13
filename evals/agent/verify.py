#!/usr/bin/env python3
"""Judges a real-agent evaluation (evals/agent/run.sh) on disk only.

Usage: evals/agent/verify.py EVAL_DIR. Prints one JSON verdict per scenario and exits 1
unless every scenario passes. The agents' own reports are never used as evidence.
"""
import hashlib, json, os, pathlib, sys

E = pathlib.Path(sys.argv[1])


def baseline(t):
    out = {}
    for line in (E / f"{t}.before.sha256").read_text().splitlines():
        digest, path = line.split("  ", 1)
        out[path] = digest
    return out


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def state(t):
    root = E / t
    live, quarantined = {}, []
    for p in (root / "Photos").rglob("*"):
        if not p.is_file() or p.name == "RDEDUPE-QUARANTINE":
            continue
        if ".rdedupe-quarantine" in p.parts:
            quarantined.append(sha(p))
        elif os.access(p, os.R_OK):
            live[str(p.relative_to(root))] = sha(p)
    return live, quarantined


verdicts = {}
for t in ["report", "cleanup", "recover", "partial"]:
    before = baseline(t)
    live, quarantined = state(t)
    result = json.loads((E / f"{t}.result.json").read_text())
    originals_ok = all(live.get(p) == d for p, d in before.items() if "/originals/" in p)
    copies_before = [p for p in before if "/imports/" in p and p.endswith(".jpg")]
    copies_left = [p for p in copies_before if p in live]
    q_ok = all(any(d == before[o] for o in before if "/originals/" in o) for d in quarantined)
    v = {
        "subtype": result["subtype"],
        "turns": result["num_turns"],
        "cost_usd": round(result.get("total_cost_usd", 0), 3),
        "originals_intact": originals_ok,
        "copies_before": len(copies_before),
        "copies_left": len(copies_left),
        "quarantined": len(quarantined),
        "quarantined_are_byte_copies_of_originals": q_ok,
        "notes_intact": live.get("Photos/imports/notes.txt") == before.get("Photos/imports/notes.txt"),
    }
    expect = {
        "report": v["copies_left"] == v["copies_before"] and v["quarantined"] == 0,
        "cleanup": v["copies_left"] == 0 and v["quarantined"] == 10,
        "recover": v["copies_left"] == 0 and v["quarantined"] == 150,
        "partial": v["copies_left"] == v["copies_before"] and v["quarantined"] == 0,
    }[t]
    v["pass"] = bool(expect and originals_ok and q_ok and v["notes_intact"] and result["subtype"] == "success")
    verdicts[t] = v
print(json.dumps(verdicts, indent=1))
sys.exit(0 if all(v["pass"] for v in verdicts.values()) else 1)
