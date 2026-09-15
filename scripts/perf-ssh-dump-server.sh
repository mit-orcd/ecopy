#!/bin/sh
# perf-ssh-dump-server.sh - turn destination perf.data files into text reports.
#
# Run on the DESTINATION host after the profiled copy finishes.
#
#   scripts/perf-ssh-dump-server.sh
#
# Inputs (override with env):
#   ECOPY_SERVER_DATA_GLOB  /tmp/ecopy-server.*.data
#   ECOPY_SSHD_DATA         /tmp/sshd-sys.data
#   ECOPY_PERF_REPORT_DIR   ./perf-ecopy
#
# Outputs in ECOPY_PERF_REPORT_DIR:
#   ecopy-server.<pid>-symbols.txt
#   ecopy-server.<pid>-comm-dso.txt
#   ecopy-server.<pid>-callgraph.txt
#   sshd-symbols.txt / sshd-comm-dso.txt   (if the sshd capture exists)

set -eu

GLOB=${ECOPY_SERVER_DATA_GLOB:-/tmp/ecopy-server.*.data}
SSHD=${ECOPY_SSHD_DATA:-/tmp/sshd-sys.data}
OUT=${ECOPY_PERF_REPORT_DIR:-./perf-ecopy}

if ! command -v perf >/dev/null 2>&1; then
    echo "perf-ssh-dump-server: perf not found in PATH" >&2
    exit 1
fi

mkdir -p "$OUT"

found=0
# glob must be unquoted so the shell expands it
for f in $GLOB; do
    if [ ! -f "$f" ]; then
        continue
    fi
    found=1
    base=$(basename "$f" .data)
    echo "reporting $f"
    if ! perf report -i "$f" --stdio --no-children \
            > "$OUT/${base}-symbols.txt"; then
        echo "perf-ssh-dump-server: skipping unreadable $f" >&2
        rm -f "$OUT/${base}-symbols.txt"
        continue
    fi
    perf report -i "$f" --stdio --sort comm,dso \
        > "$OUT/${base}-comm-dso.txt"
    perf report -i "$f" --stdio --no-children -n --percent-limit 0.3 \
        > "$OUT/${base}-callgraph.txt"
done

if [ "$found" -eq 0 ]; then
    echo "perf-ssh-dump-server: no files matched $GLOB" >&2
    exit 1
fi

if [ -f "$SSHD" ]; then
    echo "reporting $SSHD"
    perf report -i "$SSHD" --stdio --sort comm,dso \
        > "$OUT/sshd-comm-dso.txt"
    perf report -i "$SSHD" --stdio --no-children --percent-limit 0.5 \
        > "$OUT/sshd-symbols.txt"
else
    echo "perf-ssh-dump-server: no sshd capture at $SSHD (skipping)" >&2
fi

echo "wrote reports in $OUT"
ls -lh "$OUT"
