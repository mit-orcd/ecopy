#!/bin/sh
# perf-ssh-server.sh - profile the SERVER side of an ecopy ssh:// transfer.
#
# ecopy spawns one server process per ssh connection on the destination host
# via "<ECOPY_REMOTE_CMD> --server '<path>'". This wrapper runs that server
# under perf record. Each connection writes its own data file.
#
# Setup (on the DESTINATION host):
#   1. install the ecopy binary there (must match the client version)
#   2. install this wrapper, e.g.:
#        scp scripts/perf-ssh-server.sh remote:/tmp/ecopy-perf-server.sh
#        ssh remote chmod +x /tmp/ecopy-perf-server.sh
#
# Run (on the SOURCE host):
#   ECOPY_REMOTE_CMD=/tmp/ecopy-perf-server.sh \
#       ecopy /data1/archive/001/ ssh://remote/data1/archive/001/
#
# Output (on the destination host):
#   /tmp/ecopy-server.<pid>.data   one file per connection
#   (override directory with ECOPY_PERF_OUT_DIR, exported in the wrapper's
#    environment - set it inside this script if the default /tmp is unwanted)
#
# Report (on the destination host):
#   perf report -i /tmp/ecopy-server.<pid>.data --stdio
#   or merge a view across connections:
#   cat /tmp/ecopy-server.*.data ... (or just report on the busiest pid)
#
# Notes:
#   - Needs perf_event_paranoid <= 1 on the destination (or root):
#       sysctl kernel.perf_event_paranoid=1
#   - This profiles the ecopy server only. The per-connection sshd does the
#     decryption and lives *above* this process, so profile it separately:
#       sudo perf record -a -g -o /tmp/sshd-sys.data         # system-wide, or
#       sudo perf record -g -p "$(pgrep -f 'sshd:.*@notty' | head -1)" ...
#     System-wide (-a) also attributes kernel TCP/socket stacks, which is
#     usually where WAN time goes after crypto.
#   - ssh's non-interactive PATH is usually /usr/bin:/bin, so a repo-local
#     ecopy is invisible. This wrapper defaults ECOPY_BIN to ../ecopy next
#     to the script (override if needed).
#   - If handshake fails the client bootstraps an uploaded binary and
#     bypasses this wrapper - abort if you see "bootstrapping remote binary".
#   - If hardware cycles are unavailable (some VMs): ECOPY_PERF_EVENT=cpu-clock

set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ECOPY_BIN=${ECOPY_BIN:-"$HERE/../ecopy"}
OUT_DIR=${ECOPY_PERF_OUT_DIR:-/tmp}
FREQ=${ECOPY_PERF_FREQ:-997}
EVENT=${ECOPY_PERF_EVENT:-cycles}
OUT="$OUT_DIR/ecopy-server.$$.data"

if ! command -v perf >/dev/null 2>&1; then
    echo "perf-ssh-server: perf not found" >&2
    exit 1
fi
if [ ! -x "$ECOPY_BIN" ]; then
    echo "perf-ssh-server: ecopy not executable at $ECOPY_BIN (set ECOPY_BIN)" >&2
    exit 1
fi

echo "perf-ssh-server: recording $ECOPY_BIN -> $OUT (event $EVENT)" >&2
exec perf record -e "$EVENT" -F "$FREQ" -g -o "$OUT" -- "$ECOPY_BIN" "$@"
