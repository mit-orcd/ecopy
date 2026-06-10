#!/usr/bin/env bash
#
# ecopy_harness.sh
#
# A small, reusable functional test harness for ecopy. It builds a variety of
# source trees (small files, large files, sparse files, nested directories,
# symlinks, read-only files, ...) and verifies that ecopy reproduces them
# correctly. Most data-copy cases are run under several runtime profiles
# (direct I/O, buffered I/O, in-place small writes, and a lowered large-file
# threshold that forces files through the large pipeline) so the same data is
# exercised across ecopy's different code paths.
#
# Usage:
#   tests/ecopy_harness.sh            # run all cases
#   ECOPY_BIN=/path/to/ecopy tests/ecopy_harness.sh
#   ECOPY_HARNESS_VERBOSE=1 tests/ecopy_harness.sh
#
# Exit status is non-zero if any case fails.

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="${ECOPY_BIN:-$repo_root/ecopy}"
verbose="${ECOPY_HARNESS_VERBOSE:-0}"

if [[ ! -x "$bin" ]]; then
    echo "ecopy binary not found or not executable: $bin" >&2
    echo "Build it first with: make" >&2
    exit 1
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/ecopy-harness.XXXXXX")"
trap 'chmod -R u+rwX "$work" 2>/dev/null || true; rm -rf "$work"' EXIT

pass_count=0
fail_count=0
current_case="(none)"

log()  { [[ "$verbose" == "1" ]] && echo "    $*"; return 0; }
note() { echo "    $*"; }

fail() {
    echo "FAIL [$current_case]: $*" >&2
    fail_count=$((fail_count + 1))
}

ok() {
    log "ok: $*"
    pass_count=$((pass_count + 1))
}

# Runtime profiles. Each value is the env prefix applied to an ecopy run.
# Shared modest worker counts keep the harness light on CI machines.
common_env=(
    DIRECT_COPY_MAX_WORKERS=8
    DIRECT_COPY_SMALL_MAX_WORKERS=4
    DIRECT_COPY_LARGE_WORKERS=2
    DIRECT_COPY_LARGE_READERS=1
    DIRECT_COPY_LARGE_WRITERS=1
    DIRECT_COPY_LARGE_FILE_INFLIGHT=2
    DIRECT_COPY_CHUNK_MB=1
)

run_ecopy() {
    # run_ecopy <profile> <src> <dst>
    local profile="$1" src="$2" dst="$3"
    shift 3
    local -a env_extra=()
    case "$profile" in
        direct)
            env_extra=(DIRECT_COPY_DISABLE_DIRECT_IO=0 DIRECT_COPY_LARGE_THRESHOLD_MB=128) ;;
        buffered)
            env_extra=(DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_LARGE_THRESHOLD_MB=128) ;;
        inplace)
            env_extra=(DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_SMALL_INPLACE=1 DIRECT_COPY_LARGE_THRESHOLD_MB=128) ;;
        force_large)
            env_extra=(DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_LARGE_THRESHOLD_MB=1) ;;
        *)
            echo "unknown profile: $profile" >&2; return 2 ;;
    esac
    env "${common_env[@]}" "${env_extra[@]}" "$bin" "$src" "$dst"
}

# allocated_blocks <file> -> number of 512-byte blocks actually allocated.
allocated_blocks() { stat -c %b "$1"; }
logical_size()     { stat -c %s "$1"; }
mode_bits()        { stat -c %a "$1"; }

# Does the working filesystem actually keep files sparse? If not, sparseness
# assertions are skipped (content is still verified).
sparse_supported() {
    local probe="$work/.sparse_probe"
    truncate -s 8M "$probe" 2>/dev/null || { rm -f "$probe"; return 1; }
    local blk
    blk="$(allocated_blocks "$probe")"
    rm -f "$probe"
    # 8 MiB fully dense would be 16384 blocks; a real hole is far smaller.
    [[ "$blk" -lt 1024 ]]
}

# verify_regular_files <src> <dst>: every regular file under src must exist
# under dst with identical content and permission bits.
verify_regular_files() {
    local src="$1" dst="$2" rel f d rc=0
    while IFS= read -r -d '' f; do
        rel="${f#"$src"/}"
        d="$dst/$rel"
        if [[ ! -e "$d" ]]; then
            fail "missing in dst: $rel"; rc=1; continue
        fi
        if ! cmp -s "$f" "$d"; then
            fail "content mismatch: $rel"; rc=1; continue
        fi
        if [[ "$(mode_bits "$f")" != "$(mode_bits "$d")" ]]; then
            fail "mode mismatch on $rel: src=$(mode_bits "$f") dst=$(mode_bits "$d")"; rc=1; continue
        fi
    done < <(find "$src" -type f -print0)
    return $rc
}

case_begin() {
    current_case="$1"
    log "== case: $current_case =="
}

# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

case_empty_tree() {
    case_begin "empty-tree"
    local s="$work/empty_src" d="$work/empty_dst"
    mkdir -p "$s/sub1/sub2"
    if run_ecopy direct "$s" "$d" >/dev/null 2>&1 && [[ -d "$d/sub1/sub2" ]]; then
        ok "empty nested tree recreated"
    else
        fail "empty tree not recreated"
    fi
}

case_basic_trees() {
    # Build one mixed tree and verify it under every profile.
    local s="$work/basic_src"
    mkdir -p "$s/a/b/c" "$s/empty_dir"
    printf 'hello world\n'        > "$s/top.txt"
    printf ''                     > "$s/zero.bin"          # zero-byte file
    head -c 4096  /dev/urandom    > "$s/a/aligned.bin"
    head -c 5000  /dev/urandom    > "$s/a/unaligned.bin"
    head -c 100   /dev/urandom    > "$s/a/b/tiny.bin"
    head -c 200000 /dev/urandom   > "$s/a/b/c/medium.bin"
    chmod 0640 "$s/top.txt"
    chmod 0750 "$s/a/b"
    local i
    for i in $(seq 1 50); do
        printf 'file %d payload\n' "$i" > "$s/many_$i.txt"
    done

    local profile
    for profile in direct buffered inplace force_large; do
        case_begin "basic-tree:$profile"
        local d="$work/basic_dst_$profile"
        if ! run_ecopy "$profile" "$s" "$d" >/dev/null 2>&1; then
            fail "ecopy run failed"; continue
        fi
        if verify_regular_files "$s" "$d"; then
            ok "all regular files match"
        fi
        if [[ -d "$d/empty_dir" ]]; then ok "empty dir preserved"; else fail "empty dir missing"; fi
        if [[ "$(mode_bits "$s/a/b")" == "$(mode_bits "$d/a/b")" ]]; then
            ok "directory mode preserved"
        else
            fail "directory mode mismatch on a/b"
        fi
    done
}

case_large_file() {
    case_begin "large-file-unaligned-tail"
    local s="$work/large_src" d="$work/large_dst"
    mkdir -p "$s"
    # ~5 MiB plus an unaligned tail; force_large pushes it through the large pipeline.
    head -c $((5 * 1024 * 1024 + 777)) /dev/urandom > "$s/big.bin"
    local src_sha; src_sha="$(sha256sum "$s/big.bin" | awk '{print $1}')"
    if ! run_ecopy force_large "$s" "$d" >/dev/null 2>&1; then
        fail "ecopy run failed"; return
    fi
    local dst_sha; dst_sha="$(sha256sum "$d/big.bin" | awk '{print $1}')"
    if [[ "$src_sha" == "$dst_sha" ]]; then
        ok "large-file content identical"
    else
        fail "large-file content mismatch"
    fi
    if [[ "$(logical_size "$s/big.bin")" == "$(logical_size "$d/big.bin")" ]]; then
        ok "large-file size identical"
    else
        fail "large-file size mismatch"
    fi
}

case_sparse_file() {
    case_begin "sparse-file"
    local s="$work/sparse_src" d="$work/sparse_dst"
    mkdir -p "$s"
    # 256 MiB logical: data at the start and middle, holes elsewhere. This is
    # also larger than the default large threshold, so it exercises the sparse
    # routing away from the dense large pipeline.
    truncate -s 256M "$s/sparse.bin"
    dd if=/dev/urandom of="$s/sparse.bin" bs=1M count=4 conv=notrunc status=none
    dd if=/dev/urandom of="$s/sparse.bin" bs=1M count=4 seek=128 conv=notrunc status=none

    local src_sha; src_sha="$(sha256sum "$s/sparse.bin" | awk '{print $1}')"
    if ! run_ecopy direct "$s" "$d" >/dev/null 2>&1; then
        fail "ecopy run failed"; return
    fi
    local dst_sha; dst_sha="$(sha256sum "$d/sparse.bin" | awk '{print $1}')"
    if [[ "$src_sha" == "$dst_sha" ]]; then
        ok "sparse content identical"
    else
        fail "sparse content mismatch"
    fi
    if [[ "$(logical_size "$s/sparse.bin")" == "$(logical_size "$d/sparse.bin")" ]]; then
        ok "sparse logical size identical"
    else
        fail "sparse logical size mismatch"
    fi

    if sparse_supported; then
        local dense_blocks=$((256 * 1024 * 1024 / 512))
        local dst_blocks; dst_blocks="$(allocated_blocks "$d/sparse.bin")"
        # Allow generous headroom; a non-sparse copy would be ~524288 blocks.
        if [[ "$dst_blocks" -lt $((dense_blocks / 4)) ]]; then
            ok "sparseness preserved (dst=$dst_blocks blocks << dense=$dense_blocks)"
        else
            fail "sparseness NOT preserved (dst=$dst_blocks blocks, dense would be $dense_blocks)"
        fi
    else
        note "skip sparseness assertion (filesystem does not keep files sparse)"
    fi
}

case_all_hole_file() {
    case_begin "all-hole-file"
    local s="$work/hole_src" d="$work/hole_dst"
    mkdir -p "$s"
    truncate -s 64M "$s/hole.bin"   # pure hole, no data anywhere
    local src_sha; src_sha="$(sha256sum "$s/hole.bin" | awk '{print $1}')"
    if ! run_ecopy direct "$s" "$d" >/dev/null 2>&1; then
        fail "ecopy run failed"; return
    fi
    local dst_sha; dst_sha="$(sha256sum "$d/hole.bin" | awk '{print $1}')"
    if [[ "$src_sha" == "$dst_sha" ]]; then
        ok "all-zero content identical"
    else
        fail "all-zero content mismatch"
    fi
    if sparse_supported; then
        local dst_blocks; dst_blocks="$(allocated_blocks "$d/hole.bin")"
        if [[ "$dst_blocks" -lt 256 ]]; then
            ok "pure-hole file stays unallocated (dst=$dst_blocks blocks)"
        else
            fail "pure-hole file allocated storage (dst=$dst_blocks blocks)"
        fi
    else
        note "skip sparseness assertion (filesystem does not keep files sparse)"
    fi
}

case_skip_rerun() {
    case_begin "skip-on-rerun"
    local s="$work/skip_src" d="$work/skip_dst"
    mkdir -p "$s"
    printf 'stable payload\n' > "$s/keep.txt"
    head -c 200000 /dev/urandom > "$s/data.bin"
    run_ecopy buffered "$s" "$d" >/dev/null 2>&1
    local out
    out="$(run_ecopy buffered "$s" "$d" 2>&1)"
    if grep -q "Files skipped : 2" <<<"$out"; then
        ok "unchanged files skipped on rerun"
    else
        fail "expected 2 skipped files on rerun; got: $(grep -E 'Files (copied|skipped)' <<<"$out" | tr '\n' ' ')"
    fi
}

case_symlink_ignored() {
    case_begin "symlink-ignored"
    local s="$work/link_src" d="$work/link_dst"
    mkdir -p "$s"
    printf 'real file\n' > "$s/real.txt"
    ln -s real.txt "$s/link.txt"
    if ! run_ecopy buffered "$s" "$d" >/dev/null 2>&1; then
        fail "ecopy run failed"; return
    fi
    if [[ -f "$d/real.txt" ]] && cmp -s "$s/real.txt" "$d/real.txt"; then
        ok "regular file copied"
    else
        fail "regular file not copied correctly"
    fi
    if [[ ! -e "$d/link.txt" ]]; then
        ok "symlink ignored (not copied)"
    else
        fail "symlink unexpectedly present in dst"
    fi
}

case_readonly_mode() {
    case_begin "readonly-mode"
    local s="$work/ro_src" d="$work/ro_dst"
    mkdir -p "$s"
    printf 'read only payload\n' > "$s/ro.txt"
    chmod 0444 "$s/ro.txt"
    if ! run_ecopy buffered "$s" "$d" >/dev/null 2>&1; then
        fail "ecopy run failed"; return
    fi
    if cmp -s "$s/ro.txt" "$d/ro.txt" && [[ "$(mode_bits "$d/ro.txt")" == "444" ]]; then
        ok "read-only file content and 0444 mode preserved"
    else
        fail "read-only file mismatch (mode=$(mode_bits "$d/ro.txt" 2>/dev/null))"
    fi
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

echo "ecopy harness: $bin"
case_empty_tree
case_basic_trees
case_large_file
case_sparse_file
case_all_hole_file
case_skip_rerun
case_symlink_ignored
case_readonly_mode

echo
echo "ecopy harness results: $pass_count passed, $fail_count failed"
if [[ "$fail_count" -ne 0 ]]; then
    exit 1
fi
echo "ecopy harness checks passed"
