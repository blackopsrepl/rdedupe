#!/usr/bin/env python3
"""Before/after CLI comparison on identical deterministic fixtures.

usage: benches/compare.py FIXTURE_DIR OUTPUT.json NAME=BINARY [NAME=BINARY ...]

Generates (or verifies) the fixture set under FIXTURE_DIR, then runs every binary on
every workload, alternating binaries within each repeat so drift affects all equally.
Each run is measured with /usr/bin/time (wall, peak RSS, user, system), warm (fixture
pages resident) and cold (each fixture file evicted with posix_fadvise(DONTNEED) before
the run; no global cache is ever dropped). stdout and stderr are discarded identically for
every binary. One extra untimed run per binary and case under strace records content
bytes read from fixture files and peak open descriptors.

Each binary is invoked as `BINARY ROOT` (the legacy report route both 2.x and 3.x
accept), so the comparison measures the same user-visible operation.
"""
import hashlib
import json
import os
import random
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPEATS = 5


def content(n, seed):
    return random.Random(seed).randbytes(n)


def write_case(root, name, entries, hardlinks=(), sparse=()):
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    for fn, data in entries:
        p = d / fn
        if not p.exists():
            p.write_bytes(data)
        digest.update(fn.encode() + hashlib.sha256(p.read_bytes()).digest())
    for original, alias in hardlinks:
        p = d / alias
        if not p.exists():
            os.link(d / original, p)
        assert p.stat().st_ino == (d / original).stat().st_ino
    for fn, length in sparse:
        p = d / fn
        if not p.exists():
            with p.open("wb") as f:
                f.truncate(length)
        assert p.stat().st_size == length
        digest.update(fn.encode() + str(length).encode())
    files = sum(1 for _ in d.iterdir())
    return {"files": files, "logical_bytes": sum(p.stat().st_size for p in d.iterdir()), "sha256": digest.hexdigest()}


COLLISION_BASE = None


def collision(i):
    """300 KiB, identical in every sampled window, different at offset 100,000."""
    global COLLISION_BASE
    if COLLISION_BASE is None:
        COLLISION_BASE = content(300 * 1024, 50000)
    data = bytearray(COLLISION_BASE)
    data[100000:100004] = i.to_bytes(4, "little")
    return bytes(data)


def fixtures(root):
    prefix = content(32768, 20000)
    large = content(16 * 1024 * 1024, 30000)
    return {
        "unique-sizes": write_case(root, "unique-sizes", [(f"{i:05}.bin", content(32768 + i, i)) for i in range(1200)]),
        "same-size-distinct": write_case(root, "same-size-distinct", [(f"{i:05}.bin", content(65536, 10000 + i)) for i in range(1000)]),
        "shared-prefix": write_case(root, "shared-prefix", [(f"{i:05}.bin", prefix + content(32768, 20001 + i)) for i in range(1000)]),
        "tiny-duplicates": write_case(root, "tiny-duplicates", [(f"{i:05}.bin", content(80, i // 3)) for i in range(6000)]),
        "large-duplicates": write_case(root, "large-duplicates", [(f"{i:05}.bin", large) for i in range(12)]),
        "hardlinks": write_case(root, "hardlinks", [(f"{i:05}.bin", content(65536, 40000 + i)) for i in range(300)],
                                hardlinks=[(f"{i:05}.bin", f"alias-{i:05}.bin") for i in range(300)]),
        "sample-collisions": write_case(root, "sample-collisions", [(f"{i:05}.bin", collision(i)) for i in range(200)]),
        "sparse": write_case(root, "sparse", [], sparse=[("a.bin", 64 << 20), ("b.bin", 64 << 20), ("c.bin", (64 << 20) + 1)]),
    }


def evict(case_dir):
    """Drop only this fixture's pages from the page cache (posix_fadvise DONTNEED on each
    file), never the global cache: a controlled cold-storage run on a shared host."""
    for path in sorted(Path(case_dir).rglob("*")):
        if path.is_file():
            fd = os.open(path, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)


def run(binary, case_dir, scratch, cold):
    if cold:
        evict(case_dir)
    metrics = scratch / "time.out"
    # No timeout: Popen.wait(timeout) polls with growing sleeps, which quantizes the
    # measured wall time; a blocking wait returns as soon as the child exits.
    started = time.monotonic()
    proc = subprocess.run(
        ["/usr/bin/time", "-f", "%e %M %U %S", "-o", str(metrics), binary, str(case_dir)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={**os.environ, "RUST_LOG": "warn"},
    )
    wall = time.monotonic() - started
    _, rss, user, system = metrics.read_text().split()[-4:]
    return {"exit_code": proc.returncode, "wall_seconds": round(wall, 4), "peak_rss_kib": int(rss),
            "user_seconds": float(user), "system_seconds": float(system)}


SYSCALL = re.compile(r"^(\w+)\((.*)\) += (-?\d+)")


def io_profile(binary, case_dir, scratch):
    """One untimed run under strace: content bytes read from fixture files (read/pread64/
    readv on descriptors opened under the case directory) and peak simultaneously open
    descriptors of the whole process."""
    trace = scratch / "strace.out"
    subprocess.run(
        ["strace", "-f", "-qq", "-s", "0", "-o", str(trace),
         "-e", "trace=openat,open,close,read,pread64,readv,preadv,preadv2,dup,dup2,dup3,fcntl,pipe2,socket,eventfd2,epoll_create1,memfd_create",
         binary, str(case_dir)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={**os.environ, "RUST_LOG": "warn"}, timeout=600, check=False,
    )
    prefix = str(case_dir)
    content_fds = set()
    open_fds = {0, 1, 2}
    peak = len(open_fds)
    content_bytes = 0
    pending = {}
    for raw in trace.read_text(errors="replace").splitlines():
        pid, _, line = raw.partition(" ")
        line = line.lstrip()
        if line.endswith("<unfinished ...>"):
            pending[pid] = line[: -len("<unfinished ...>")]
            continue
        resumed = re.match(r"^<\.\.\. \w+ resumed>(.*)$", line)
        if resumed:
            line = pending.pop(pid, "") + resumed.group(1)
        match = SYSCALL.match(line)
        if not match:
            continue
        name, args, result = match.group(1), match.group(2), int(match.group(3))
        if result < 0:
            continue
        if name in ("openat", "open"):
            open_fds.add(result)
            path = args.split('"')[1] if '"' in args else ""
            if path.startswith(prefix):
                content_fds.add(result)
            else:
                content_fds.discard(result)
        elif name in ("dup", "dup2", "dup3", "socket", "eventfd2", "epoll_create1", "memfd_create") or (
            name == "fcntl" and "F_DUPFD" in args
        ):
            open_fds.add(result)
        elif name == "pipe2":
            fds = re.findall(r"\d+", args.split("]")[0])
            open_fds.update(int(f) for f in fds[:2])
        elif name == "close":
            fd = int(args.split(",")[0])
            open_fds.discard(fd)
            content_fds.discard(fd)
        elif name in ("read", "pread64", "readv", "preadv", "preadv2"):
            fd = int(args.split(",")[0])
            if fd in content_fds:
                content_bytes += result
        peak = max(peak, len(open_fds))
    trace.unlink()
    return {"content_bytes_read": content_bytes, "peak_open_descriptors": peak}


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    root, out = Path(sys.argv[1]), Path(sys.argv[2])
    binaries = dict(arg.split("=", 1) for arg in sys.argv[3:])
    manifest = fixtures(root)
    scratch = out.parent
    result = {
        "fixture_root": str(root),
        "manifest": manifest,
        "repeats": REPEATS,
        "conditions": {
            "warm": "fixture pages resident (just written or read); no cache manipulation",
            "cold": "each fixture file evicted with posix_fadvise(DONTNEED) before every run; global caches untouched",
        },
        "binaries": {n: {"path": b, "bytes": Path(b).stat().st_size} for n, b in binaries.items()},
        "cases": {},
    }
    for case in manifest:
        result["cases"][case] = {}
        for condition in ("warm", "cold"):
            samples = {n: [] for n in binaries}
            for _ in range(REPEATS):
                for name, binary in binaries.items():
                    sample = run(binary, root / case, scratch, condition == "cold")
                    assert sample["exit_code"] == 0, (case, name, sample)
                    samples[name].append(sample)
            result["cases"][case][condition] = {
                n: {
                    "median_wall_seconds": statistics.median(s["wall_seconds"] for s in v),
                    "median_peak_rss_kib": statistics.median(s["peak_rss_kib"] for s in v),
                    "samples": v,
                }
                for n, v in samples.items()
            }
        result["cases"][case]["io"] = {n: io_profile(b, root / case, scratch) for n, b in binaries.items()}
        summary = {
            n: (
                result["cases"][case]["warm"][n]["median_wall_seconds"],
                result["cases"][case]["cold"][n]["median_wall_seconds"],
                result["cases"][case]["warm"][n]["median_peak_rss_kib"],
                result["cases"][case]["io"][n]["content_bytes_read"],
                result["cases"][case]["io"][n]["peak_open_descriptors"],
            )
            for n in binaries
        }
        print(case, summary, flush=True)
        out.write_text(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
