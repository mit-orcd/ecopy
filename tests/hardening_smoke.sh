#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="${ECOPY_BIN:-$repo_root/ecopy}"

if [[ ! -x "$bin" ]]; then
    echo "ecopy binary not found or not executable: $bin" >&2
    echo "Build it first with: make" >&2
    exit 1
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/ecopy-hardening-smoke.XXXXXX")"
trap 'chmod -R u+rwX "$tmp" 2>/dev/null || true; rm -rf "$tmp"' EXIT

run_ecopy() {
    DIRECT_COPY_DISABLE_DIRECT_IO="${DIRECT_COPY_DISABLE_DIRECT_IO:-1}" \
    DIRECT_COPY_MAX_WORKERS="${DIRECT_COPY_MAX_WORKERS:-8}" \
    DIRECT_COPY_SMALL_MAX_WORKERS="${DIRECT_COPY_SMALL_MAX_WORKERS:-4}" \
    DIRECT_COPY_LARGE_READERS="${DIRECT_COPY_LARGE_READERS:-1}" \
    DIRECT_COPY_LARGE_WRITERS="${DIRECT_COPY_LARGE_WRITERS:-1}" \
    DIRECT_COPY_LARGE_FILE_INFLIGHT="${DIRECT_COPY_LARGE_FILE_INFLIGHT:-2}" \
    DIRECT_COPY_LARGE_THRESHOLD_MB="${DIRECT_COPY_LARGE_THRESHOLD_MB:-1}" \
    "$bin" "$@"
}

mkdir -p "$tmp/src_symlink" "$tmp/dst_symlink"
printf 'outside\n' > "$tmp/outside.txt"
ln -s "$tmp/outside.txt" "$tmp/src_symlink/link.txt"
run_ecopy "$tmp/src_symlink" "$tmp/dst_symlink" >/dev/null
if [[ -e "$tmp/dst_symlink/link.txt" ]]; then
    echo "source symlink was copied" >&2
    exit 1
fi

mkdir -p "$tmp/src_mid/sub" "$tmp/dst_mid" "$tmp/victim_dir"
printf 'safe\n' > "$tmp/src_mid/sub/file.txt"
printf 'victim\n' > "$tmp/victim_dir/file.txt"
ln -s "$tmp/victim_dir" "$tmp/dst_mid/sub"
if run_ecopy "$tmp/src_mid" "$tmp/dst_mid" >/dev/null 2>"$tmp/mid.err"; then
    echo "target intermediate symlink unexpectedly succeeded" >&2
    exit 1
fi
if [[ "$(cat "$tmp/victim_dir/file.txt")" != "victim" ]]; then
    echo "target intermediate symlink was followed" >&2
    exit 1
fi

mkdir -p "$tmp/src_atomic" "$tmp/dst_atomic"
printf 'old-destination\n' > "$tmp/dst_atomic/file.txt"
chmod 0444 "$tmp/dst_atomic/file.txt"
printf 'new-source-data\n' > "$tmp/src_atomic/file.txt"
run_ecopy "$tmp/src_atomic" "$tmp/dst_atomic" >/dev/null
if [[ "$(cat "$tmp/dst_atomic/file.txt")" != "new-source-data" ]]; then
    echo "readonly overwrite did not replace through temp file" >&2
    exit 1
fi
if find "$tmp/dst_atomic" -name '.ecopy.tmp.*' | read -r _; then
    echo "temporary file leaked after successful copy" >&2
    exit 1
fi

mkdir -p "$tmp/src_meta/dir" "$tmp/dst_meta"
printf 'metadata\n' > "$tmp/src_meta/dir/file.txt"
chmod 0555 "$tmp/src_meta/dir"
python3 - <<'PY' "$tmp/src_meta/dir"
import os
import sys

path = sys.argv[1]
os.utime(path, ns=(1712345800123456789, 1712345800123456789))
PY
run_ecopy "$tmp/src_meta" "$tmp/dst_meta" >/dev/null
python3 - <<'PY' "$tmp/src_meta/dir" "$tmp/dst_meta/dir"
import os
import stat
import sys

src, dst = sys.argv[1:]
s = os.stat(src)
d = os.stat(dst)
assert stat.S_IMODE(d.st_mode) == stat.S_IMODE(s.st_mode), (oct(stat.S_IMODE(d.st_mode)), oct(stat.S_IMODE(s.st_mode)))
assert d.st_mtime_ns == s.st_mtime_ns, (d.st_mtime_ns, s.st_mtime_ns)
PY

mkdir -p "$tmp/src_env" "$tmp/dst_env"
printf 'env\n' > "$tmp/src_env/file.txt"
DIRECT_COPY_MAX_WORKERS=1 DIRECT_COPY_CHUNK_MB=abc run_ecopy "$tmp/src_env" "$tmp/dst_env" >/dev/null 2>"$tmp/env.err"
if ! grep -q 'DIRECT_COPY_MAX_WORKERS=1 is below minimum 2' "$tmp/env.err"; then
    echo "missing max-workers clamp warning" >&2
    exit 1
fi
if ! grep -q 'DIRECT_COPY_CHUNK_MB=abc is invalid' "$tmp/env.err"; then
    echo "missing invalid chunk warning" >&2
    exit 1
fi

mkdir -p "$tmp/src_buffered" "$tmp/dst_buffered" "$tmp/src_default" "$tmp/dst_default"
python3 - <<'PY' "$tmp/src_buffered/big.bin" "$tmp/src_default/big.bin"
import sys

payload = (b"buffered-direct-mode-check\n" * 90000)[:2 * 1024 * 1024 + 17]
for path in sys.argv[1:]:
    with open(path, "wb") as f:
        f.write(payload)
PY
DIRECT_COPY_DISABLE_DIRECT_IO=1 run_ecopy "$tmp/src_buffered" "$tmp/dst_buffered" >/dev/null
DIRECT_COPY_DISABLE_DIRECT_IO=0 run_ecopy "$tmp/src_default" "$tmp/dst_default" >/dev/null
cmp -s "$tmp/src_buffered/big.bin" "$tmp/dst_buffered/big.bin"
cmp -s "$tmp/src_default/big.bin" "$tmp/dst_default/big.bin"

mkdir -p "$tmp/src_fd_pressure/mega" "$tmp/dst_fd_pressure"
python3 - <<'PY' "$tmp/src_fd_pressure/mega"
import os
import sys

root = sys.argv[1]
for i in range(600):
    with open(os.path.join(root, f"f{i:06d}.txt"), "wb") as f:
        f.write(b"fd-pressure\n")
PY
(
    ulimit -n 64
    DIRECT_COPY_DISABLE_DIRECT_IO=1 \
    DIRECT_COPY_MAX_WORKERS=8 \
    DIRECT_COPY_SMALL_MAX_WORKERS=4 \
    DIRECT_COPY_LARGE_READERS=1 \
    DIRECT_COPY_LARGE_WRITERS=1 \
    DIRECT_COPY_LARGE_FILE_INFLIGHT=2 \
    DIRECT_COPY_LARGE_THRESHOLD_MB=1024 \
    DIRECT_COPY_MAX_QUEUED_FILES=1000 \
    "$bin" "$tmp/src_fd_pressure" "$tmp/dst_fd_pressure" >/dev/null
)
if [[ "$(find "$tmp/dst_fd_pressure/mega" -type f | wc -l)" -ne 600 ]]; then
    echo "fd pressure copy missed files" >&2
    exit 1
fi

echo "hardening smoke checks passed"
