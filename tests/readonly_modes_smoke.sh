#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="${ECOPY_BIN:-$repo_root/ecopy}"

if [[ ! -x "$bin" ]]; then
    echo "ecopy binary not found or not executable: $bin" >&2
    echo "Build it first with: make" >&2
    exit 1
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/ecopy-readonly-smoke.XXXXXX")"
trap 'chmod -R u+rwX "$tmp" 2>/dev/null || true; rm -rf "$tmp"' EXIT

run_ecopy_large() {
    DIRECT_COPY_DISABLE_DIRECT_IO=1 \
    DIRECT_COPY_MAX_WORKERS=8 \
    DIRECT_COPY_SMALL_MAX_WORKERS=4 \
    DIRECT_COPY_LARGE_WORKERS=2 \
    DIRECT_COPY_LARGE_READERS=1 \
    DIRECT_COPY_LARGE_WRITERS=1 \
    DIRECT_COPY_LARGE_FILE_INFLIGHT=2 \
    DIRECT_COPY_LARGE_THRESHOLD_MB=1 \
    "$bin" "$1" "$2" >/dev/null
}

run_ecopy_small() {
    DIRECT_COPY_DISABLE_DIRECT_IO=1 \
    DIRECT_COPY_MAX_WORKERS=8 \
    DIRECT_COPY_SMALL_MAX_WORKERS=4 \
    DIRECT_COPY_LARGE_WORKERS=2 \
    DIRECT_COPY_LARGE_READERS=1 \
    DIRECT_COPY_LARGE_WRITERS=1 \
    DIRECT_COPY_LARGE_FILE_INFLIGHT=2 \
    DIRECT_COPY_LARGE_THRESHOLD_MB=1024 \
    "$bin" "$1" "$2" >/dev/null
}

run_ecopy_default() {
    DIRECT_COPY_MAX_WORKERS=4 \
    DIRECT_COPY_SMALL_MAX_WORKERS=2 \
    DIRECT_COPY_LARGE_READERS=1 \
    DIRECT_COPY_LARGE_WRITERS=1 \
    DIRECT_COPY_LARGE_FILE_INFLIGHT=2 \
    DIRECT_COPY_LARGE_THRESHOLD_MB=1024 \
    "$bin" "$1" "$2" >/dev/null
}

mkdir -p "$tmp/src_missing/nested"
printf 'missing-root-data\n' > "$tmp/src_missing/nested/file.txt"
run_ecopy_small "$tmp/src_missing" "$tmp/missing/root/child"
cmp -s "$tmp/src_missing/nested/file.txt" "$tmp/missing/root/child/nested/file.txt"

mkdir -p "$tmp/src_overlap"
printf 'overlap-data\n' > "$tmp/src_overlap/file.txt"
if run_ecopy_small "$tmp/src_overlap" "$tmp/src_overlap/new/dst" 2>"$tmp/overlap.err"; then
    echo "overlap copy unexpectedly succeeded" >&2
    exit 1
fi
if [[ -e "$tmp/src_overlap/new" ]]; then
    echo "overlap rejection created directories inside the source" >&2
    exit 1
fi

mkdir -p "$tmp/src_default"
printf 'default-mode-data\n' > "$tmp/src_default/file.txt"
run_ecopy_default "$tmp/src_default" "$tmp/dst_default"
cmp -s "$tmp/src_default/file.txt" "$tmp/dst_default/file.txt"

mkdir -p "$tmp/src_fifo" "$tmp/dst_fifo"
printf 'fifo-target-data\n' > "$tmp/src_fifo/file.txt"
mkfifo "$tmp/dst_fifo/file.txt"
if run_ecopy_small "$tmp/src_fifo" "$tmp/dst_fifo" 2>"$tmp/fifo.err"; then
    echo "fifo target overwrite unexpectedly succeeded" >&2
    exit 1
fi
if [[ ! -p "$tmp/dst_fifo/file.txt" ]]; then
    echo "fifo target was replaced" >&2
    exit 1
fi

mkdir -p "$tmp/src_large" "$tmp/dst_large"
python3 - <<'PY' "$tmp/src_large/image.jpg"
import os
import sys

path = sys.argv[1]
with open(path, "wb") as f:
    f.write((b"readonly-large-jpeg-test\n" * 90000)[:2 * 1024 * 1024 + 123])
os.chmod(path, 0o555)
os.utime(path, ns=(1712345678123456789, 1712345678123456789))
PY

run_ecopy_large "$tmp/src_large" "$tmp/dst_large"
python3 - <<'PY' "$tmp/src_large/image.jpg" "$tmp/dst_large/image.jpg"
import hashlib
import os
import stat
import sys

src, dst = sys.argv[1:]
s = os.stat(src)
d = os.stat(dst)
assert stat.S_IMODE(d.st_mode) == 0o555, oct(stat.S_IMODE(d.st_mode))
assert d.st_mtime_ns == s.st_mtime_ns, (d.st_mtime_ns, s.st_mtime_ns)
assert hashlib.sha256(open(src, "rb").read()).digest() == hashlib.sha256(open(dst, "rb").read()).digest()
PY

run_ecopy_large "$tmp/src_large" "$tmp/dst_large"

python3 - <<'PY' "$tmp/src_large/image.jpg"
import os
import sys

path = sys.argv[1]
os.chmod(path, 0o755)
with open(path, "r+b") as f:
    f.seek(0)
    f.write(b"changed-readonly-destination")
os.chmod(path, 0o555)
os.utime(path, ns=(1712345688123456789, 1712345688123456789))
PY

run_ecopy_large "$tmp/src_large" "$tmp/dst_large"
python3 - <<'PY' "$tmp/src_large/image.jpg" "$tmp/dst_large/image.jpg"
import hashlib
import os
import stat
import sys

src, dst = sys.argv[1:]
s = os.stat(src)
d = os.stat(dst)
assert stat.S_IMODE(d.st_mode) == 0o555, oct(stat.S_IMODE(d.st_mode))
assert d.st_mtime_ns == s.st_mtime_ns, (d.st_mtime_ns, s.st_mtime_ns)
assert hashlib.sha256(open(src, "rb").read()).digest() == hashlib.sha256(open(dst, "rb").read()).digest()
PY

mkdir -p "$tmp/src_small" "$tmp/dst_small"
python3 - <<'PY' "$tmp/src_small/file.jpg"
import os
import sys

path = sys.argv[1]
open(path, "wb").write(b"first-small-readonly")
os.chmod(path, 0o444)
os.utime(path, ns=(1712345700123456789, 1712345700123456789))
PY

run_ecopy_small "$tmp/src_small" "$tmp/dst_small"
run_ecopy_small "$tmp/src_small" "$tmp/dst_small"

python3 - <<'PY' "$tmp/src_small/file.jpg"
import os
import sys

path = sys.argv[1]
os.chmod(path, 0o644)
open(path, "wb").write(b"changed-small-readonly")
os.chmod(path, 0o444)
os.utime(path, ns=(1712345710123456789, 1712345710123456789))
PY

run_ecopy_small "$tmp/src_small" "$tmp/dst_small"
python3 - <<'PY' "$tmp/src_small/file.jpg" "$tmp/dst_small/file.jpg"
import os
import stat
import sys

src, dst = sys.argv[1:]
s = os.stat(src)
d = os.stat(dst)
assert open(dst, "rb").read() == open(src, "rb").read()
assert stat.S_IMODE(d.st_mode) == 0o444, oct(stat.S_IMODE(d.st_mode))
assert d.st_mtime_ns == s.st_mtime_ns, (d.st_mtime_ns, s.st_mtime_ns)
PY

mkdir -p "$tmp/src_link" "$tmp/dst_link"
printf 'new-data' > "$tmp/src_link/file.jpg"
printf 'victim-data' > "$tmp/victim.txt"
ln -s "$tmp/victim.txt" "$tmp/dst_link/file.jpg"

if run_ecopy_small "$tmp/src_link" "$tmp/dst_link" 2>"$tmp/symlink.err"; then
    echo "symlink overwrite unexpectedly succeeded" >&2
    exit 1
fi

python3 - <<'PY' "$tmp/victim.txt" "$tmp/dst_link/file.jpg"
import os
import sys

victim, link = sys.argv[1:]
assert open(victim, "rb").read() == b"victim-data"
assert os.path.islink(link)
PY

echo "readonly mode smoke checks passed"
