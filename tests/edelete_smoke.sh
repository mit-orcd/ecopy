#!/usr/bin/env bash
#
# edelete_smoke.sh
#
# Functional tests for edelete, ported from ereport's scripts/test/test.sh
# (edelete moved to this repo). Covers:
#   - dry-run smoke (default mode deletes nothing)
#   - "." / ".." as argv path components, cwd ".", dot-prefixed directory names
#   - --uid / --gid ownership filters (skipped when chown is not permitted)
#   - safety negatives: symlink targets survive, age filter keeps fresh files,
#     deletion never ascends above the start path
#
# Usage:
#   tests/edelete_smoke.sh            # run all cases
#   EDELETE=/path/to/edelete tests/edelete_smoke.sh
#
# Exit status is non-zero on the first failed invariant.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EDELETE="${EDELETE:-$repo_root/edelete}"

if [[ ! -x "$EDELETE" ]]; then
    echo "edelete binary not found or not executable: $EDELETE" >&2
    echo "Build it first with: make edelete" >&2
    exit 1
fi

td="$(mktemp -d "${TMPDIR:-/tmp}/edelete-smoke.XXXXXX")"
trap 'rm -rf "$td"' EXIT

log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die()  { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'OK: %s\n' "$*"; }

# Last line wins (tools may print stats blocks more than once in verbose modes).
kv_last() {
    local key=$1 file=$2
    grep "^${key}=" "$file" 2>/dev/null | tail -n1 | cut -d= -f2-
}

expect_eq() {
    local label=$1 want=$2 got=$3
    [[ "$got" == "$want" ]] || die "${label}: want '${want}' got '${got}'"
}

# --- dry-run smoke -----------------------------------------------------------

mkdir -p "${td}/walk/sub"
echo hello >"${td}/walk/a.txt"
echo world >"${td}/walk/sub/b.txt"
root_abs=$(cd "${td}/walk" && pwd)

log "edelete dry-run (synthetic walk tree)"
"$EDELETE" "$root_abs" >"${td}/edelete.stdout" 2>"${td}/edelete.stderr" || {
    tail -n 40 "${td}/edelete.stderr" >&2 || true
    die "edelete dry-run failed"
}
[[ -f "${td}/walk/a.txt" ]] || die "dry-run removed a.txt"
pass "edelete --dry-run"

# --- synthetic . / .. path probes (--delete) ----------------------------------
# Ensures we never rely on deleting the literal dot-directory entries from
# readdir (tested in edelete.c) and that path normalization / containment
# behave as expected.

base="${td}/edelete_dot_probe"
mkdir -p "$base"

log "edelete synthetic: probe tree under ${base}"

# t1: start path is "." resolved from inside the deepest directory (realpath -> absolute).
mkdir -p "${base}/t1_rel_dot/deep"
echo x >"${base}/t1_rel_dot/deep/f.txt"
out="${base}/t1.stdout"
err="${base}/t1.stderr"
(cd "${base}/t1_rel_dot/deep" && "$EDELETE" --delete --force . >"$out" 2>"$err") || {
    cat "$err" >&2 || true
    die "edelete dots: t1 (cwd .) failed"
}
[[ ! -f "${base}/t1_rel_dot/deep/f.txt" ]] || die "edelete dots: t1 file should be removed"
[[ ! -d "${base}/t1_rel_dot/deep" ]] || die "edelete dots: t1 leaf dir should be removed"
ef=$(kv_last errors "$out")
expect_eq "edelete dots: t1 errors" "0" "${ef:-missing_errors_line}"

# t2: redundant "./" segments in the start path argument.
mkdir -p "${base}/t2_dotslash/walk/inner"
echo y >"${base}/t2_dotslash/walk/inner/g.txt"
out="${base}/t2.stdout"
"$EDELETE" --delete --force "${base}/t2_dotslash/walk/./inner/./" >"$out" 2>"${base}/t2.stderr" || die "edelete dots: t2 (./ segments) failed"
[[ ! -e "${base}/t2_dotslash/walk/inner/g.txt" ]] || die "edelete dots: t2 file should be removed"
expect_eq "edelete dots: t2 errors" "0" "$(kv_last errors "$out")"

# t3: ".." in argv collapsing to the intended directory (bash resolves before edelete runs).
mkdir -p "${base}/t3_dotdot/norm/a/b"
echo z >"${base}/t3_dotdot/norm/a/b/h.txt"
out="${base}/t3.stdout"
"$EDELETE" --delete --force "${base}/t3_dotdot/norm/a/b/../.." >"$out" 2>"${base}/t3.stderr" || die "edelete dots: t3 (.. collapse) failed"
[[ ! -d "${base}/t3_dotdot/norm" ]] || die "edelete dots: t3 norm/ should be removed"
expect_eq "edelete dots: t3 errors" "0" "$(kv_last errors "$out")"

# t4: deleting one subtree must not remove a sibling (exercises containment vs .. semantics).
mkdir -p "${base}/t4_sibling/keep" "${base}/t4_sibling/delete_me/sub"
echo keep >"${base}/t4_sibling/keep/preserved.txt"
echo x >"${base}/t4_sibling/delete_me/sub/x.txt"
out="${base}/t4.stdout"
"$EDELETE" --delete --force "${base}/t4_sibling/delete_me" >"$out" 2>"${base}/t4.stderr" || die "edelete dots: t4 sibling containment failed"
[[ -f "${base}/t4_sibling/keep/preserved.txt" ]] || die "edelete dots: t4 sibling file must survive"
[[ ! -e "${base}/t4_sibling/delete_me" ]] || die "edelete dots: t4 delete_me subtree should be gone"
expect_eq "edelete dots: t4 errors" "0" "$(kv_last errors "$out")"

# t5: dot-prefixed directory names (not the special "." / ".." entries) are visited and removed.
mkdir -p "${base}/t5_dotnames/.hidden/deep"
echo h >"${base}/t5_dotnames/.hidden/deep/h.txt"
out="${base}/t5.stdout"
"$EDELETE" --delete --force "${base}/t5_dotnames" >"$out" 2>"${base}/t5.stderr" || die "edelete dots: t5 .hidden dir failed"
[[ ! -d "${base}/t5_dotnames/.hidden" ]] || die "edelete dots: t5 .hidden should be removed"
expect_eq "edelete dots: t5 errors" "0" "$(kv_last errors "$out")"

# t6: dry-run with "./" and ".." only in argv path — tree must remain untouched.
mkdir -p "${base}/t6_dry/sub"
echo d >"${base}/t6_dry/sub/file.txt"
out="${base}/t6.stdout"
"$EDELETE" "${base}/t6_dry/./sub/../" >"$out" 2>"${base}/t6.stderr" || die "edelete dots: t6 dry-run failed"
[[ -f "${base}/t6_dry/sub/file.txt" ]] || die "edelete dots: t6 dry-run must keep files"
expect_eq "edelete dots: t6 mode dry-run" "dry-run" "$(kv_last mode "$out")"
expect_eq "edelete dots: t6 deleted_files stays 0" "0" "$(kv_last deleted_files "$out")"

# t7: legal names that start with "." but are not "." or ".." (e.g. .local).
mkdir -p "${base}/t7_dotprefix/.local/bin"
echo p >"${base}/t7_dotprefix/.local/bin/p.txt"
out="${base}/t7.stdout"
"$EDELETE" --delete --force "${base}/t7_dotprefix" >"$out" 2>"${base}/t7.stderr" || die "edelete dots: t7 .local failed"
[[ ! -f "${base}/t7_dotprefix/.local/bin/p.txt" ]] || die "edelete dots: t7 dot-prefix path should be deleted"
expect_eq "edelete dots: t7 errors" "0" "$(kv_last errors "$out")"

pass "edelete synthetic (. .. argv paths, cwd ., dot-named dirs)"

# --- uid/gid ownership filters ------------------------------------------------

base="${td}/edelete_uid_gid"
mkdir -p "$base/mixed"

my_uid=$(id -u)
my_gid=$(id -g)
other_uid=$((my_uid + 1))
other_gid=$((my_gid + 1))

echo mine >"${base}/mixed/mine.txt"
echo other >"${base}/mixed/other.txt"
chown "${my_uid}:${my_gid}" "${base}/mixed/mine.txt"
if chown "${other_uid}:${other_gid}" "${base}/mixed/other.txt" 2>/dev/null; then
    out="${base}/uid.stdout"
    "$EDELETE" --delete --force --uid "$my_uid" "${base}/mixed" >"$out" 2>"${base}/uid.stderr" || die "edelete uid filter failed"
    [[ -f "${base}/mixed/other.txt" ]] || die "edelete uid filter: other.txt should remain"
    [[ ! -f "${base}/mixed/mine.txt" ]] || die "edelete uid filter: mine.txt should be removed"
    expect_eq "edelete uid filter errors" "0" "$(kv_last errors "$out")"

    echo mine2 >"${base}/mixed/mine2.txt"
    echo other2 >"${base}/mixed/other2.txt"
    chown "${my_uid}:${my_gid}" "${base}/mixed/mine2.txt"
    chown "${other_uid}:${other_gid}" "${base}/mixed/other2.txt"

    out="${base}/gid.stdout"
    "$EDELETE" --delete --force --gid "$other_gid" "${base}/mixed" >"$out" 2>"${base}/gid.stderr" || die "edelete gid filter failed"
    [[ -f "${base}/mixed/mine2.txt" ]] || die "edelete gid filter: mine2.txt should remain"
    [[ ! -f "${base}/mixed/other2.txt" ]] || die "edelete gid filter: other2.txt should be removed"
    expect_eq "edelete gid filter errors" "0" "$(kv_last errors "$out")"

    echo both >"${base}/mixed/both.txt"
    chown "${my_uid}:${other_gid}" "${base}/mixed/both.txt"

    out="${base}/both.stdout"
    "$EDELETE" --delete --force --uid "$my_uid" --gid "$other_gid" "${base}/mixed" >"$out" 2>"${base}/both.stderr" || die "edelete uid+gid filter failed"
    [[ ! -f "${base}/mixed/both.txt" ]] || die "edelete uid+gid filter: both.txt should be removed"
    expect_eq "edelete uid+gid filter errors" "0" "$(kv_last errors "$out")"

    pass "edelete --uid / --gid ownership filters"
else
    log "edelete uid/gid: skip (cannot chown to ${other_uid}:${other_gid})"
fi

# --- safety negatives (must-not-delete invariants) ----------------------------
#   - symlinks are unlinked, but their targets outside the start tree are never
#     followed/removed;
#   - the age filter never deletes entries newer than the threshold;
#   - --delete never ascends above the start path (siblings / parents survive).

base="${td}/edelete_safety"
mkdir -p "$base"

# n1: symlinks under the start path point outside it; only the links may go, never the targets.
mkdir -p "${base}/n1/tree" "${base}/n1/outside/dir_keep"
echo keep >"${base}/n1/outside/file_keep.txt"
echo inner >"${base}/n1/outside/dir_keep/inner.txt"
ln -s ../outside/file_keep.txt "${base}/n1/tree/flink"
ln -s ../outside/dir_keep "${base}/n1/tree/dlink"
out="${base}/n1.stdout"
"$EDELETE" --delete --force "${base}/n1/tree" >"$out" 2>"${base}/n1.stderr" || die "edelete safety: n1 symlink walk failed"
[[ -f "${base}/n1/outside/file_keep.txt" ]] || die "edelete safety: n1 symlink target file must survive"
[[ -d "${base}/n1/outside/dir_keep" ]] || die "edelete safety: n1 symlinked dir must survive (no follow)"
[[ -f "${base}/n1/outside/dir_keep/inner.txt" ]] || die "edelete safety: n1 contents under symlinked dir must survive"
expect_eq "edelete safety: n1 errors" "0" "$(kv_last errors "$out")"

# n2: age filter must keep entries newer than the threshold (only the backdated file is eligible).
mkdir -p "${base}/n2/agetree"
echo old >"${base}/n2/agetree/old.txt"
echo fresh >"${base}/n2/agetree/fresh.txt"
if touch -d "100 days ago" "${base}/n2/agetree/old.txt" 2>/dev/null; then
    out="${base}/n2.stdout"
    "$EDELETE" --delete --force mtime 30 "${base}/n2/agetree" >"$out" 2>"${base}/n2.stderr" || die "edelete safety: n2 age filter failed"
    [[ ! -f "${base}/n2/agetree/old.txt" ]] || die "edelete safety: n2 old.txt (>=30d) should be removed"
    [[ -f "${base}/n2/agetree/fresh.txt" ]] || die "edelete safety: n2 fresh.txt (<30d) must survive"
    expect_eq "edelete safety: n2 deleted_files" "1" "$(kv_last deleted_files "$out")"
    expect_eq "edelete safety: n2 errors" "0" "$(kv_last errors "$out")"
else
    log "edelete safety: skip n2 (touch -d not supported)"
fi

# n3: deleting a leaf subtree must never touch a parent file or a sibling subtree.
mkdir -p "${base}/n3/keep_sibling" "${base}/n3/delete_me/sub"
echo parent >"${base}/n3/parent_file.txt"
echo sib >"${base}/n3/keep_sibling/s.txt"
echo gone >"${base}/n3/delete_me/sub/g.txt"
out="${base}/n3.stdout"
"$EDELETE" --delete --force "${base}/n3/delete_me" >"$out" 2>"${base}/n3.stderr" || die "edelete safety: n3 containment failed"
[[ ! -e "${base}/n3/delete_me" ]] || die "edelete safety: n3 target subtree should be gone"
[[ -f "${base}/n3/parent_file.txt" ]] || die "edelete safety: n3 parent file must survive"
[[ -f "${base}/n3/keep_sibling/s.txt" ]] || die "edelete safety: n3 sibling subtree must survive"
expect_eq "edelete safety: n3 errors" "0" "$(kv_last errors "$out")"

pass "edelete safety negatives (symlink targets, age freshness, containment)"

pass "edelete test suite"
