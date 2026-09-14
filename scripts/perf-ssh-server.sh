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
#       sudo perf record -a -g -o sshd.data -- sleep 60     # system-wide, or
#       sudo perf record -g -p "$(pgrep -f 'sshd:.*@notty' | head -1)" ...
#     System-wide (-a) also attributes kernel TCP/socket stacks, which is
#     usually where WAN time goes after crypto.
#   - If the client cannot find ecopy on the remote it bootstraps an uploaded
#     binary and bypasses ECOPY_REMOTE_CMD - make sure the wrapper and
#     ECOPY_BIN below are in place so the first handshake succeeds.

set -eu

ECOPY_BIN=${ECOPY_BIN:-ecopy}          # resolved via remote PATH by default
OUT_DIR=${ECOPY_PERF_OUT_DIR:-/tmp}
FREQ=${ECOPY_PERF_FREQ:-997}
OUT="$OUT_DIR/ecopy-server.$$.data"

if ! command -v perf >/dev/null 2>&1; then
    echo "perf-ssh-server: perf not found" >&2
    exit 1
fi

echo "perf-ssh-server: recording to $OUT" >&2
exec perf record -F "$FREQ" -g -o "$OUT" -- "$ECOPY_BIN" "$@"
