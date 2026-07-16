#!/usr/bin/env bash
#
# rsync-audit-meta.sh - independent structural + metadata audit of an ecopy run.
#
# Runs a read-only rsync dry-run that reports what it would still change on the
# target, with flags matched to ecopy's preservation semantics so it does not
# flag things ecopy deliberately does not do. Reads metadata only (size, mtime,
# perms, symlink target, hardlink grouping) - it does NOT read file contents, so
# it finishes in traversal time even for huge/sparse trees.
#
# Matched to ecopy semantics:
#   -r recursive, -l symlinks-as-symlinks, -H hard links, -p perms, -t mtime.
#   NOT -o/-g (ecopy runs unprivileged and cannot chown; comparing owner/group
#   would flag every object, like ecopy's "Ownership not preserved" line).
#   NOT -A/-X (ecopy does not copy ACLs/xattrs).
#
# A clean ecopy run should produce essentially no itemized lines - only the
# --stats summary. Any of these are actionable:
#   cd+++++++++ path/   directory missing on target
#   >f+++++++++ path    file missing on target
#   >f.st...... path    content/size/time differ
#   .f....p.... path    perms differ
#   cL / .L..t.. path   symlink missing / target differs
#   *deleting path      extraneous file on target (judge vs. a fresh target)
#
# Itemize code YXcstpoguax: Y >=xfer c=create .=attrs *=msg; X f/d/L/h;
# then c=checksum s=size t=time p=perms o=owner g=group u=atime a=acl x=xattr.
#
# Usage:
#   scripts/rsync-audit-meta.sh [SRC DST]
# Environment overrides:
#   SRC        source path (trailing slash copies contents)   [default below]
#   DST        rsync destination (user@host:/path/)           [default below]
#   RSYNC_SSH  remote-shell command, e.g. a ControlMaster ssh  [default: unset]
#   LOG        log file path                       [default: logs/rsync-audit-meta.log]
#
# Exit code is rsync's own (0 = clean/no differences reportable errors).

set -u

SRC="${1:-${SRC:-/var/home/ereport/files/}}"
DST="${2:-${DST:-erbmi1@orcd-login.mit.edu:/home/erbmi1/orcd/scratch/ecopy-ssh-test/files/}}"
LOG="${LOG:-logs/rsync-audit-meta.log}"

mkdir -p "$(dirname "$LOG")"

rsync_args=(
  -rlHpt
  -n
  --delete
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

echo "rsync meta audit: $SRC -> $DST" >&2
rsync "${rsync_args[@]}" "$SRC" "$DST" 2>&1 | tee "$LOG"
rc="${PIPESTATUS[0]}"
echo "exit=$rc (log: $LOG)" >&2
exit "$rc"
