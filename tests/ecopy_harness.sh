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

# run_ecopy_ssh <src> <dst_abs>: push to a remote-style ssh:// target.
# By default this uses tests/fake_ssh.sh, a stand-in that runs `ecopy --server`
# locally over a pipe, so the full protocol/transport/server path is exercised
# without needing SSH keys. Set ECOPY_HARNESS_REAL_SSH=1 to use the real ssh to
# localhost instead (requires working key-based auth).
run_ecopy_ssh() {
    local src="$1" dst_abs="$2"
    local -a env_extra=(DIRECT_COPY_LARGE_THRESHOLD_MB=1 ECOPY_REMOTE_CMD="$bin")
    if [[ "${ECOPY_HARNESS_REAL_SSH:-0}" != "1" ]]; then
        env_extra+=(ECOPY_SSH="$repo_root/tests/fake_ssh.sh")
    fi
    env "${common_env[@]}" "${env_extra[@]}" "$bin" "$src" "ssh://localhost${dst_abs}"
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

case_local_fresh_policy() {
    case_begin "local-fresh-policy"
    local s="$work/fresh_src" d="$work/fresh_dst" out
    mkdir -p "$s/sub"
    printf 'fresh payload\n' > "$s/sub/file.txt"

    out="$(env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
               DIRECT_COPY_SMALL_INPLACE=0 \
               "$bin" -v "$s" "$d" 2>&1)"
    if [[ "$?" -ne 0 ]]; then
        fail "fresh local copy failed"
        return
    fi
    if cmp -s "$s/sub/file.txt" "$d/sub/file.txt" &&
       grep -Eq 'small in-place writes[[:space:]]*: yes' <<<"$out"; then
        ok "new local root automatically uses in-place writes"
    else
        fail "fresh local root did not select in-place policy"
    fi

    out="$(env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
               DIRECT_COPY_SMALL_INPLACE=0 \
               "$bin" -v "$s" "$d" 2>&1)"
    if [[ "$?" -eq 0 ]] &&
       grep -Eq 'small in-place writes[[:space:]]*: no' <<<"$out" &&
       grep -q 'Files skipped : 1' <<<"$out"; then
        ok "existing local root retains incremental skip policy"
    else
        fail "existing local root did not use incremental policy"
    fi
}

case_local_batched_mixed_queue() {
    case_begin "local-batched-mixed-queue"
    local s="$work/lbatch_src" d="$work/lbatch_dst" i
    mkdir -p "$s/many"
    for i in $(seq 1 600); do
        printf 'tiny %d\n' "$i" > "$s/many/f_$i"
    done
    head -c 2200000 /dev/urandom > "$s/large.bin"
    truncate -s 16M "$s/sparse.bin"
    dd if=/dev/urandom of="$s/sparse.bin" bs=4096 count=1 seek=1024 \
       conv=notrunc status=none

    if ! env "${common_env[@]}" \
             DIRECT_COPY_DISABLE_DIRECT_IO=1 \
             DIRECT_COPY_LARGE_THRESHOLD_MB=1 \
             DIRECT_COPY_MAX_QUEUED_FILES=7 \
             "$bin" "$s" "$d" >/dev/null 2>&1; then
        fail "mixed local batch copy failed"
        return
    fi
    if verify_regular_files "$s" "$d"; then
        ok "local batch spans 512 entries and preserves mixed routing"
    fi
}

case_local_no_preserve_times() {
    case_begin "local-no-preserve-times"
    local s="$work/lnptime_src" d="$work/lnptime_dst"
    mkdir -p "$s/sub"
    printf 'keep content and mode\n' > "$s/sub/file.txt"
    chmod 0640 "$s/sub/file.txt"
    touch -d '2001-02-03 04:05:06' "$s/sub/file.txt"

    if ! env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
             "$bin" --no-preserve-times "$s" "$d" >/dev/null 2>&1; then
        fail "local --no-preserve-times copy failed"
        return
    fi
    if cmp -s "$s/sub/file.txt" "$d/sub/file.txt" &&
       [[ "$(mode_bits "$d/sub/file.txt")" == "640" ]] &&
       [[ "$(stat -c %Y "$s/sub/file.txt")" != "$(stat -c %Y "$d/sub/file.txt")" ]]; then
        ok "local no-preserve-times keeps content/mode but skips mtime"
    else
        fail "local no-preserve-times behavior mismatch"
    fi
}

case_local_type_collision() {
    case_begin "local-type-collision"
    local s="$work/lcollision_src" d="$work/lcollision_dst"
    mkdir -p "$s" "$d/collide"
    printf 'regular source\n' > "$s/collide"

    if env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
           "$bin" "$s" "$d" >/dev/null 2>&1; then
        fail "local copy accepted a directory where a file belongs"
    else
        ok "local batch rejects wrong-type destination"
    fi
}

case_ssh_loopback() {
    case_begin "ssh-loopback"
    local s="$work/ssh_src" d="$work/ssh_dst"
    mkdir -p "$s/a/b" "$s/empty" "$s/deep/d1/d2/d3"
    printf 'top payload\n'        > "$s/top.txt"
    printf ''                     > "$s/zero.bin"
    head -c 300000 /dev/urandom   > "$s/a/mid.bin"
    printf 'nested\n'             > "$s/a/b/deep.txt"
    chmod 0640 "$s/top.txt"
    chmod 0750 "$s/a/b"
    local i
    for i in $(seq 1 20); do printf 'file %d\n' "$i" > "$s/many_$i.txt"; done
    # Lots of tiny files across nested dirs to exercise the PUTFILE batch path.
    mkdir -p "$s/tiny/one" "$s/tiny/two" "$s/tiny/batch"
    for i in $(seq 1 60); do printf 'x' > "$s/tiny/one/t_$i"; done
    for i in $(seq 1 60); do printf 'yy' > "$s/tiny/two/u_$i"; done
    # More than REMOTE_STAT_BATCH (512) entries exercises multi-task queue
    # appends and the transition to a second batch.
    for i in $(seq 1 600); do printf 'z' > "$s/tiny/batch/b_$i"; done
    # A directory whose ONLY child is a large (streamed) file. The client sends
    # no explicit MKDIR for it, so the server's streamed OPEN must create the
    # parent on its own. run_ecopy_ssh forces the large-file threshold to 1 MiB.
    mkdir -p "$s/bigonly"
    head -c 2200000 /dev/urandom > "$s/bigonly/stream.bin"
    # Final directory metadata is pooled remotely; preserve distinctive modes
    # and mtimes to validate its barrier/order rules.
    chmod 0710 "$s/tiny"
    chmod 0750 "$s/tiny/one"
    touch -d '2002-03-04 05:06:07' "$s/tiny/one"
    touch -d '2001-02-03 04:05:06' "$s/tiny"
    truncate -s 64M "$s/sparse.bin"
    dd if=/dev/urandom of="$s/sparse.bin" bs=1M count=2 conv=notrunc status=none
    dd if=/dev/urandom of="$s/sparse.bin" bs=1M count=2 seek=32 conv=notrunc status=none

    if ! run_ecopy_ssh "$s" "$d" >/dev/null 2>&1; then
        fail "ssh push run failed"; return
    fi
    if verify_regular_files "$s" "$d"; then
        ok "all regular files match over ssh"
    fi
    if [[ -d "$d/empty" ]]; then ok "empty dir preserved over ssh"; else fail "empty dir missing over ssh"; fi
    if [[ -d "$d/deep/d1/d2/d3" ]]; then ok "deep empty dir chain created over ssh"; else fail "deep empty dir chain missing over ssh"; fi
    if [[ -d "$d/bigonly" && -f "$d/bigonly/stream.bin" ]]; then
        ok "dir with only a streamed file created over ssh"
    else
        fail "streamed-only dir not created over ssh"
    fi
    if [[ "$(mode_bits "$s/a/b")" == "$(mode_bits "$d/a/b")" ]]; then
        ok "directory mode preserved over ssh"
    else
        fail "directory mode mismatch over ssh"
    fi
    if [[ "$(mode_bits "$s/tiny")" == "$(mode_bits "$d/tiny")" &&
          "$(mode_bits "$s/tiny/one")" == "$(mode_bits "$d/tiny/one")" ]]; then
        ok "pooled directory modes preserved over ssh"
    else
        fail "pooled directory mode mismatch over ssh"
    fi
    if [[ "$(stat -c %Y "$s/tiny")" == "$(stat -c %Y "$d/tiny")" &&
          "$(stat -c %Y "$s/tiny/one")" == "$(stat -c %Y "$d/tiny/one")" ]]; then
        ok "pooled directory mtimes preserved over ssh"
    else
        fail "pooled directory mtime mismatch over ssh"
    fi
    if [[ "$(logical_size "$s/sparse.bin")" == "$(logical_size "$d/sparse.bin")" ]]; then
        ok "sparse logical size preserved over ssh"
    else
        fail "sparse size mismatch over ssh"
    fi
    if sparse_supported; then
        local dst_blocks; dst_blocks="$(allocated_blocks "$d/sparse.bin")"
        if [[ "$dst_blocks" -lt $((64 * 1024 * 1024 / 512 / 4)) ]]; then
            ok "sparseness preserved over ssh (dst=$dst_blocks blocks)"
        else
            fail "sparseness NOT preserved over ssh (dst=$dst_blocks blocks)"
        fi
    fi

    # Second run must skip everything (bulk-stat skip path).
    local out
    out="$(run_ecopy_ssh "$s" "$d" 2>&1)"
    if grep -q "Files copied  : 0" <<<"$out"; then
        ok "unchanged tree fully skipped over ssh"
    else
        fail "expected 0 copied on rerun; got: $(grep -E 'Files (copied|skipped)' <<<"$out" | tr '\n' ' ')"
    fi
}

# A server-side failure on a fire-and-forget op must surface at the barrier and
# make the run exit nonzero (batch-level error reporting).
case_ssh_batch_failure() {
    case_begin "ssh-batch-failure"
    local s="$work/sshf_src" d="$work/sshf_dst"
    mkdir -p "$s/collide"
    printf 'inner\n' > "$s/collide/inner.txt"
    # Pre-create the destination with 'collide' as a regular FILE, so both the
    # remote mkdir and the child PUTFILE fail on the server.
    mkdir -p "$d"
    printf 'i am a file, not a dir\n' > "$d/collide"

    local out rc
    out="$(run_ecopy_ssh "$s" "$d" 2>&1)"; rc=$?
    if [[ "$rc" -ne 0 ]]; then
        ok "run exits nonzero when a remote op fails"
    else
        fail "expected nonzero exit on remote failure"
    fi
    if grep -q "remote reported" <<<"$out"; then
        ok "barrier surfaced remote error"
    else
        fail "expected 'remote reported' diagnostic; got: $(tr '\n' ' ' <<<"$out")"
    fi
}

# An empty source directory must not silently accept a destination regular file
# as an already-created directory (the shared mkdir cache validates EEXIST).
case_ssh_empty_dir_collision() {
    case_begin "ssh-empty-dir-collision"
    local s="$work/sshe_src" d="$work/sshe_dst"
    mkdir -p "$s/empty" "$d"
    printf 'do not modify\n' > "$d/empty"

    local out rc
    out="$(run_ecopy_ssh "$s" "$d" 2>&1)"; rc=$?
    if [[ "$rc" -ne 0 ]]; then
        ok "empty-dir collision exits nonzero"
    else
        fail "empty-dir collision unexpectedly succeeded"
    fi
    if [[ "$(cat "$d/empty")" == "do not modify" ]]; then
        ok "empty-dir collision leaves destination file untouched"
    else
        fail "empty-dir collision modified destination file"
    fi
}

# Single regular file as the source (not a directory).
case_single_file() {
    case_begin "single-file"
    local s="$work/one_src" d="$work/one_dst"
    mkdir -p "$s"
    printf 'single file payload\n' > "$s/note.txt"
    chmod 0640 "$s/note.txt"
    head -c 250000 /dev/urandom > "$s/blob.bin"
    mkdir -p "$d/existing"

    # Into an existing directory: keeps the source name.
    if env "${common_env[@]}" DIRECT_COPY_LARGE_THRESHOLD_MB=128 "$bin" "$s/note.txt" "$d/existing" >/dev/null 2>&1 \
        && cmp -s "$s/note.txt" "$d/existing/note.txt"; then
        ok "file copied into existing dir"
    else
        fail "file into existing dir"
    fi
    if [[ "$(mode_bits "$s/note.txt")" == "$(mode_bits "$d/existing/note.txt")" ]]; then
        ok "single-file mode preserved"
    else
        fail "single-file mode mismatch"
    fi

    # Trailing slash: create the directory, keep the source name.
    if env "${common_env[@]}" DIRECT_COPY_LARGE_THRESHOLD_MB=128 "$bin" "$s/note.txt" "$d/made/" >/dev/null 2>&1 \
        && cmp -s "$s/note.txt" "$d/made/note.txt"; then
        ok "file copied into new dir (trailing slash)"
    else
        fail "file into new dir"
    fi

    # Full destination path: rename, creating parent dirs.
    if env "${common_env[@]}" DIRECT_COPY_LARGE_THRESHOLD_MB=128 "$bin" "$s/note.txt" "$d/a/b/renamed.txt" >/dev/null 2>&1 \
        && cmp -s "$s/note.txt" "$d/a/b/renamed.txt"; then
        ok "file renamed to full path"
    else
        fail "file rename to full path"
    fi

    # Forced through the large-file pipeline.
    if env "${common_env[@]}" DIRECT_COPY_LARGE_THRESHOLD_MB=1 "$bin" "$s/blob.bin" "$d/big/" >/dev/null 2>&1 \
        && cmp -s "$s/blob.bin" "$d/big/blob.bin"; then
        ok "large single file copied"
    else
        fail "large single file"
    fi

    # Over ssh: the ssh:// path is the destination directory.
    local r="$work/one_remote"
    mkdir -p "$r"
    if run_ecopy_ssh "$s/note.txt" "$r" >/dev/null 2>&1 \
        && cmp -s "$s/note.txt" "$r/note.txt"; then
        ok "single file pushed over ssh"
    else
        fail "single file over ssh"
    fi
}

# --no-preserve-times: content and mode are still reproduced, but the mtime is
# NOT carried over (the server skips the futimens SETATTR).
case_ssh_no_preserve_times() {
    case_begin "ssh-no-preserve-times"
    local s="$work/nptime_src" d="$work/nptime_dst"
    mkdir -p "$s/sub"
    printf 'keep my content\n' > "$s/sub/f.txt"
    chmod 0640 "$s/sub/f.txt"
    # Give the source a distinctly old mtime so "not preserved" is unambiguous.
    touch -d '2001-02-03 04:05:06' "$s/sub/f.txt"

    local -a env_extra=(DIRECT_COPY_LARGE_THRESHOLD_MB=1 ECOPY_REMOTE_CMD="$bin")
    if [[ "${ECOPY_HARNESS_REAL_SSH:-0}" != "1" ]]; then
        env_extra+=(ECOPY_SSH="$repo_root/tests/fake_ssh.sh")
    fi
    if env "${common_env[@]}" "${env_extra[@]}" "$bin" --no-preserve-times \
        "$s" "ssh://localhost${d}" >/dev/null 2>&1 \
        && cmp -s "$s/sub/f.txt" "$d/sub/f.txt"; then
        ok "content copied with --no-preserve-times"
    else
        fail "content mismatch with --no-preserve-times"
    fi
    if [[ "$(mode_bits "$s/sub/f.txt")" == "$(mode_bits "$d/sub/f.txt")" ]]; then
        ok "mode preserved with --no-preserve-times"
    else
        fail "mode mismatch with --no-preserve-times"
    fi
    if [[ "$(stat -c %Y "$s/sub/f.txt")" != "$(stat -c %Y "$d/sub/f.txt")" ]]; then
        ok "mtime not carried over with --no-preserve-times"
    else
        fail "mtime unexpectedly preserved with --no-preserve-times"
    fi
}

case_transfer_verification() {
    case_begin "transfer-verification"
    local s="$work/verify_src" d="$work/verify_dst" out rc
    mkdir -p "$s/sub"
    head -c 20000 /dev/urandom > "$s/sub/data.bin"
    : > "$s/empty"
    chmod 0640 "$s/sub/data.bin"

    out="$(env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
              "$bin" --verify-data=100 --verify-metadata --verify-seed=123 \
              "$s" "$d" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] && grep -q 'Verify failures   : 0' <<<"$out" &&
       grep -q 'Verify seed       : 123' <<<"$out"; then
        ok "local 100% verification succeeds with reproducible seed"
    else
        fail "local verification success path: $(tr '\n' ' ' <<<"$out")"
        return
    fi

    local d0="$work/verify_endpoints_dst"
    out="$(env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
              "$bin" --verify-data=0 --verify-seed=123 "$s" "$d0" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] && grep -Eq 'Verify blocks[[:space:]]*: 2$' <<<"$out"; then
        ok "0% mode still verifies unique first/last blocks"
    else
        fail "endpoint-only verification: $(tr '\n' ' ' <<<"$out")"
    fi

    # Keep size+mtime equal so traversal skips the corrupted destination; the
    # optional skipped-file checker must still catch the middle-block change.
    printf X | dd of="$d/sub/data.bin" bs=1 seek=8192 conv=notrunc status=none
    touch -r "$s/sub/data.bin" "$d/sub/data.bin"
    out="$(env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
              "$bin" --verify-data=100 --verify-skipped --verify-seed=123 \
              "$s" "$d" 2>&1)"; rc=$?
    if [[ "$rc" -ne 0 ]] &&
       grep -q 'verification data mismatch' <<<"$out"; then
        ok "local skipped-file corruption is detected"
    else
        fail "local skipped corruption was not detected: $(tr '\n' ' ' <<<"$out")"
    fi

    local rd="$work/verify_remote"
    local -a remote_env=(DIRECT_COPY_LARGE_THRESHOLD_MB=1 ECOPY_REMOTE_CMD="$bin")
    if [[ "${ECOPY_HARNESS_REAL_SSH:-0}" != "1" ]]; then
        remote_env+=(ECOPY_SSH="$repo_root/tests/fake_ssh.sh")
    fi
    out="$(env "${common_env[@]}" "${remote_env[@]}" "$bin" \
              --verify-data=1 --verify-metadata --verify-seed=456 \
              "$s" "ssh://localhost${rd}" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] && grep -q 'Verify failures   : 0' <<<"$out"; then
        ok "remote sampled verification succeeds"
    else
        fail "remote verification success path: $(tr '\n' ' ' <<<"$out")"
        return
    fi

    printf Y | dd of="$rd/sub/data.bin" bs=1 seek=8192 conv=notrunc status=none
    touch -r "$s/sub/data.bin" "$rd/sub/data.bin"
    out="$(env "${common_env[@]}" "${remote_env[@]}" "$bin" \
              --verify-data=100 --verify-skipped --verify-seed=456 \
              "$s" "ssh://localhost${rd}" 2>&1)"; rc=$?
    if [[ "$rc" -ne 0 ]] && grep -q 'remote reported' <<<"$out"; then
        ok "remote skipped-file corruption is detected"
    else
        fail "remote skipped corruption was not detected: $(tr '\n' ' ' <<<"$out")"
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
case_single_file
case_local_fresh_policy
case_local_batched_mixed_queue
case_local_no_preserve_times
case_local_type_collision
case_ssh_loopback
case_ssh_batch_failure
case_ssh_empty_dir_collision
case_ssh_no_preserve_times
case_transfer_verification

echo
echo "ecopy harness results: $pass_count passed, $fail_count failed"
if [[ "$fail_count" -ne 0 ]]; then
    exit 1
fi
echo "ecopy harness checks passed"
