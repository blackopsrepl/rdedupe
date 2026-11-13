#!/usr/bin/env bash
# Real-agent evaluation of the packaged rdedupe skill with Claude Code.
#
# Usage: evals/agent/run.sh ARCHIVE.tar.gz EVAL_DIR
#
# Extracts the release archive (after checking its .sha256), installs the archive's skill
# through Claude Code's project skill directory (.claude/skills/rdedupe) in four disposable
# fixtures, puts the archive's binary first on PATH, and runs one fresh non-interactive
# Claude Code session per scenario with the user request below and nothing else. Then run
# evals/agent/verify.py EVAL_DIR, which judges outcomes on disk only.
set -euo pipefail
ARCHIVE=$(realpath "$1")
E=$(realpath -m "$2")
CLAUDE=${CLAUDE:-claude}
rm -rf "$E"
mkdir -p "$E/pkg"
(cd "$(dirname "$ARCHIVE")" && sha256sum -c --quiet "$(basename "$ARCHIVE").sha256")
tar -C "$E/pkg" -xzf "$ARCHIVE"
P=$(find "$E/pkg" -mindepth 1 -maxdepth 1 -type d)
mk() {
  python3 - "$1" "$2" <<'EOF'
import os, sys, random
root, n = sys.argv[1], int(sys.argv[2]); r = random.Random(11)
os.makedirs(f"{root}/Photos/originals"); os.makedirs(f"{root}/Photos/imports")
for i in range(n):
    b = r.randbytes(2000 + i)
    open(f"{root}/Photos/originals/img{i:03}.jpg", "wb").write(b)
    if i % 2 == 0:
        open(f"{root}/Photos/imports/IMG_{i:03} copy.jpg", "wb").write(b)
open(f"{root}/Photos/imports/notes.txt", "w").write("unique\n")
EOF
}
for t in report cleanup recover partial; do
  d=$E/$t; mkdir -p "$d/.claude/skills" "$d/bin" "$d/state"
  cp -R "$P/skills/rdedupe" "$d/.claude/skills/"
  ln -s "$P/rdedupe" "$d/bin/rdedupe"
done
mk "$E/report" 20; mk "$E/cleanup" 20; mk "$E/recover" 300; mk "$E/partial" 20
chmod 000 "$E/partial/Photos/imports/IMG_004 copy.jpg"
# Interrupted run: kill -9 the apply once its journal holds about 40 KB.
(cd "$E/recover" && export RDEDUPE_STATE_DIR=$E/recover/state && bin/rdedupe plan Photos --protect Photos/originals --output cleanup.plan >/dev/null
 python3 - <<'EOF'
import subprocess, os, signal
p = subprocess.Popen(["bin/rdedupe", "apply", "--plan", "cleanup.plan", "--journal", "cleanup.journal", "--force"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
while p.poll() is None:
    try:
        if os.path.getsize("cleanup.journal") > 40000:
            break
    except FileNotFoundError:
        pass
os.kill(p.pid, signal.SIGKILL); p.wait()
EOF
)
for t in report cleanup recover partial; do
  (cd "$E/$t" && find Photos -type f ! -path '*rdedupe-quarantine*' -readable -print0 | sort -z | xargs -0 sha256sum > "$E/$t.before.sha256")
done
run() {
  t=$1; shift
  (cd "$E/$t" && PATH="$E/$t/bin:$PATH" RDEDUPE_STATE_DIR="$E/$t/state" timeout 900 \
     "$CLAUDE" -p "$*" --allowedTools "Bash Read Glob Grep Skill" \
     --max-turns 40 --output-format json --no-session-persistence \
     > "$E/$t.result.json" 2> "$E/$t.err"
   echo "$t exit=$?" >> "$E/done.txt") &
}
run report "How much disk space are duplicate photos wasting in ./Photos? Just tell me — don't change anything."
run cleanup "Clean up the duplicate photos in ./Photos. Keep everything in Photos/originals, those are my masters. Put the copies somewhere I can get them back from; don't delete anything permanently."
run recover "Yesterday's rdedupe cleanup of ./Photos (plan cleanup.plan, journal cleanup.journal) crashed partway. Finish the cleanup I authorized then: keep Photos/originals, move the copies to quarantine. Don't purge anything."
run partial "Clean up the duplicate photos in ./Photos. Keep everything in Photos/originals. Put the copies somewhere I can get them back from."
wait
cat "$E/done.txt"
