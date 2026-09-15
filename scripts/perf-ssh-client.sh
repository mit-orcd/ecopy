#!/bin/sh
# perf-ssh-client.sh - profile the CLIENT side of an ecopy ssh:// transfer.
#
# Records ecopy *and* its ssh child processes (perf --inherit), so both
# ecopy's own hot paths and the client-side crypto done by the ssh binary
# show up in one profile.
#
# Usage:
#   scripts/perf-ssh-client.sh [ecopy options] <source> ssh://[user@]host/path
#
# Example:
#   scripts/perf-ssh-client.sh --verify /data1/archive/001/ \
#       ssh://erbmi1@remote.example.org/data1/archive/001/
#
# Output:
#   ./ecopy-client.data   (override with ECOPY_PERF_OUT=/path/file.data)
#
# Report:
#   perf report -i ecopy-client.data --stdio
#   perf report -i ecopy-client.data            # TUI, drill into ssh/ecopy
#   perf script -i ecopy-client.data | stackcollapse-perf.pl | flamegraph.pl
#
# Notes:
#   - Needs perf_event_paranoid <= 1 on the client (or run as root):
#       sysctl kernel.perf_event_paranoid=1
#   - The ssh child does the encryption; expect crypto hot paths under the
#     ssh process. Compare ciphers with DIRECT_COPY_SSH_CIPHER, e.g.:
#       DIRECT_COPY_SSH_CIPHER=aes128-gcm@openssh.com scripts/perf-ssh-client.sh ...
#   - Parallel streams change the picture: DIRECT_COPY_SSH_CONNECTIONS=8 ...
#   - For the server side see scripts/perf-ssh-server.sh.

set -eu

ECOPY_BIN=${ECOPY_BIN:-"$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/ecopy"}
OUT=${ECOPY_PERF_OUT:-ecopy-client.data}
FREQ=${ECOPY_PERF_FREQ:-997}
EVENT=${ECOPY_PERF_EVENT:-cycles}

if [ ! -x "$ECOPY_BIN" ]; then
    echo "ecopy binary not found at $ECOPY_BIN (set ECOPY_BIN)" >&2
    exit 1
fi
if ! command -v perf >/dev/null 2>&1; then
    echo "perf not found in PATH" >&2
    exit 1
fi
if [ $# -lt 2 ]; then
    sed -n '2,30p' "$0" >&2
    exit 1
fi

echo "profiling client: $ECOPY_BIN $*"
echo "perf data -> $OUT (event $EVENT)"
exec perf record -e "$EVENT" -F "$FREQ" -g --inherit -o "$OUT" -- "$ECOPY_BIN" "$@"
