#!/usr/bin/env bash
#
# rsync-audit-checksum.sh - independent content audit of an ecopy run.
#
# Like rsync-audit-meta.sh but adds --checksum, forcing a full-content (MD5)
# comparison instead of the size+mtime quick check. This catches same-size,
# same-mtime content corruption that a metadata audit (or ecopy's own
# size+mtime incremental skip) cannot.
#
# --max-size caps which files are content-checked. A full checksum reads the
# entire LOGICAL extent on both ends, so the giant sparse images (e.g. hundreds
# of TiB logical) and the largest dense files are skipped here - reading them
# over the network is infeasible, and ecopy's own sampled BLAKE3 verify already
# covers them. This pass content-verifies the long tail of small/medium real
# files; raise MAX_SIZE if you can afford more reads.
#
# Flags are matched to ecopy semantics (see rsync-audit-meta.sh header):
#   -rlHpt, and NOT -o/-g/-A/-X.
#
# A clean run should produce essentially no itemized lines beyond --stats.
# A content mismatch shows as: >f..c...... path  (or >fcst...... if size/time
# also drifted). See rsync-audit-meta.sh for the full itemize-code legend.
#
# Usage:
#   scripts/rsync-audit-checksum.sh [SRC DST]
# Environment overrides:
#   SRC        source path (trailing slash copies contents)   [default below]
#   DST        rsync destination (user@host:/path/)           [default below]
#   MAX_SIZE   skip files larger than this            [default: 2G]
#   RSYNC_SSH  remote-shell command, e.g. a ControlMaster ssh  [default: unset]
#   VERBOSE    1 = show live per-file progress (see note below)     [default: 0]
#   LOG        log file path                   [default: logs/rsync-audit-checksum.log]
#
# Why the progress line looks frozen: this is a dry run, so nothing is
# transferred and the byte/percent/rate fields of --info=progress2 stay at 0 for
# the whole run - they only count transferred data. The field that actually
# moves is the ir-chk/to-chk file counter, and each decrement is one full-content
# MD5 read on BOTH ends (slow over remote scratch), so it advances slowly but is
# not stuck. With incremental recursion (default) its denominator also keeps
# growing, so it can look stuck; set VERBOSE=1 to build the full file list up
# front (--no-inc-recursive) so it becomes a stable, monotonically decreasing
# to-chk=N/M. Watch that counter, not the byte/rate fields.
#
# Exit code is rsync's own.

set -u

SRC="${1:-${SRC:-/var/home/ereport/files/}}"
DST="${2:-${DST:-erbmi1@orcd-login.mit.edu:/home/erbmi1/orcd/scratch/ecopy-ssh-test/files/}}"
MAX_SIZE="${MAX_SIZE:-2G}"
LOG="${LOG:-logs/rsync-audit-checksum.log}"

mkdir -p "$(dirname "$LOG")"

rsync_args=(
  -rlHpt
  -n
  --checksum
  --max-size="$MAX_SIZE"
  --itemize-changes
  --stats
  --human-readable
  --info=progress2
  --out-format='%i %M %-15l %n%L'
)

# Optional: reuse an authenticated SSH connection (avoids repeated MFA/Duo).
if [ -n "${RSYNC_SSH:-}" ]; then
  rsync_args+=(-e "$RSYNC_SSH")
fi

# VERBOSE=1: build the whole file list first so to-chk=N/M counts down against a
# stable total, and show file-list build progress. (Per-file "is uptodate" lines
# would need -vv and flood the log for large trees, so the counter is the signal.)
if [ "${VERBOSE:-0}" = "1" ]; then
  rsync_args+=(--no-inc-recursive --info=flist2,progress2)
fi

echo "rsync checksum audit (max-size=$MAX_SIZE): $SRC -> $DST" >&2
rsync "${rsync_args[@]}" "$SRC" "$DST" 2>&1 | tee "$LOG"
rc="${PIPESTATUS[0]}"
echo "exit=$rc (log: $LOG)" >&2
exit "$rc"
