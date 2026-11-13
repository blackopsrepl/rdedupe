#!/usr/bin/env python3
"""Render the README screenshots from real rdedupe runs.

Usage: python3 scripts/screenshots.py BINARY OUT_DIR

Builds a small photo-library fixture in a temporary directory, runs the real BINARY
against it, and renders each transcript as a terminal window PNG with headless
Chromium (`CHROMIUM`, default `chromium`). The only change made to the output is that
the fixture's temporary root is shown as `~`. Needs python3, Chromium and the Fira
Mono font (any monospace font with a 0.6 em advance renders the same geometry).
"""

from __future__ import annotations

import html
import os
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

COLUMNS = 100  # the terminal width; long lines wrap at a space
FONT_PX = 15
CHAR_PX = FONT_PX * 0.6  # Fira Mono advance width
LINE_PX = 22
PAD_PX = 24
TITLE_PX = 36
SCALE = 2


def build_fixture(root: Path) -> None:
    rng = random.Random(7)
    photos = root / "Photos"
    for sub in ("originals", "imports", "Backup"):
        (photos / sub).mkdir(parents=True)
    for name, size in [("beach", 2_400_000), ("dog", 1_100_000), ("sunset", 3_200_000),
                       ("cake", 900_000), ("hike", 1_700_000)]:
        (photos / "originals" / f"{name}.jpg").write_bytes(rng.randbytes(size))
    copies = [("beach.jpg", "imports/beach (1).jpg"), ("dog.jpg", "imports/IMG_2041.jpg"),
              ("sunset.jpg", "Backup/sunset.jpg"), ("sunset.jpg", "imports/sunset copy.jpg")]
    for original, copy in copies:
        shutil.copyfile(photos / "originals" / original, photos / copy)
    (photos / "imports" / "notes.txt").write_text("trip notes\n")


def run(binary: str, root: Path, args: list[str]) -> list[str]:
    result = subprocess.run([binary, *args], cwd=root, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"rdedupe {' '.join(args)} exited {result.returncode}: {result.stderr}")
    text = (result.stdout + result.stderr).replace(str(root.resolve()), "~")
    return [f"$ rdedupe {' '.join(args)}", *text.rstrip("\n").split("\n")]


def wrap(lines: list[str]) -> list[str]:
    out: list[str] = []
    for line in lines:
        while len(line) > COLUMNS:
            cut = line.rfind(" ", 0, COLUMNS + 1)
            cut = cut if cut > 0 else COLUMNS
            out.append(line[:cut])
            line = line[cut:].lstrip(" ")
        out.append(line)
    return out


def render(lines: list[str], title: str, out: Path, chromium: str) -> None:
    lines = wrap(lines)
    width = round(COLUMNS * CHAR_PX + 2 * PAD_PX)
    height = TITLE_PX + 2 * PAD_PX + LINE_PX * len(lines)
    body = []
    for line in lines:
        if line.startswith("$ "):
            body.append(f'<span class="prompt">$</span> <span class="cmd">{html.escape(line[2:])}</span>')
        elif line == "":
            body.append("")
        else:
            body.append(html.escape(line))
    page = f"""<!doctype html><meta charset="utf-8"><style>
html, body {{ margin: 0; width: {width}px; height: {height}px; overflow: hidden; background: #0d1117; }}
.bar {{ height: {TITLE_PX}px; background: #161b22; border-bottom: 1px solid #30363d; display: flex;
  align-items: center; padding: 0 14px; gap: 8px; box-sizing: border-box; }}
.dot {{ width: 12px; height: 12px; border-radius: 50%; }}
.title {{ flex: 1; text-align: center; color: #8b949e; font: 13px "Fira Sans", sans-serif; margin-right: 52px; }}
pre {{ margin: 0; padding: {PAD_PX}px; font: {FONT_PX}px/{LINE_PX}px "Fira Mono", monospace;
  color: #e6edf3; white-space: pre; }}
.prompt {{ color: #d97a34; }} .cmd {{ color: #7ee2d6; }}
</style>
<div class="bar"><span class="dot" style="background:#ff5f57"></span><span class="dot" style="background:#febc2e"></span><span class="dot" style="background:#28c840"></span><span class="title">{html.escape(title)}</span></div>
<pre>{chr(10).join(body)}</pre>"""
    with tempfile.TemporaryDirectory() as tmp:
        source = Path(tmp) / "shot.html"
        source.write_text(page)
        subprocess.run([chromium, "--headless=new", "--no-sandbox", "--hide-scrollbars",
                        f"--force-device-scale-factor={SCALE}", f"--window-size={width},{height}",
                        f"--screenshot={out}", source.as_uri()],
                       check=True, capture_output=True)
    with out.open("rb") as png:
        header = png.read(24)
    size = (int.from_bytes(header[16:20], "big"), int.from_bytes(header[20:24], "big"))
    if size != (width * SCALE, height * SCALE):
        sys.exit(f"{out}: rendered {size}, expected {(width * SCALE, height * SCALE)}")
    print(f"{out} {size[0]}x{size[1]}")


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    binary = str(Path(sys.argv[1]).resolve())
    out_dir = Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    chromium = os.environ.get("CHROMIUM", "chromium")
    with tempfile.TemporaryDirectory(prefix="rdedupe-shots-") as tmp:
        root = Path(tmp)
        build_fixture(root)
        scan = run(binary, root, ["scan", "Photos", "--protect", "Photos/originals"])
        render(scan, "rdedupe scan: report only, nothing changes", out_dir / "scan.png", chromium)
        cleanup = [
            *run(binary, root, ["plan", "Photos", "--protect", "Photos/originals",
                                "--output", "cleanup.plan"]),
            "",
            *run(binary, root, ["apply", "--plan", "cleanup.plan", "--journal",
                                "cleanup.journal", "--force"]),
            "",
            *run(binary, root, ["restore", "--journal", "cleanup.journal"]),
        ]
        render(cleanup, "plan, quarantine, undo", out_dir / "cleanup.png", chromium)


if __name__ == "__main__":
    main()
