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

case_symlink_preserved() {
    case_begin "symlink-preserved"
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
    if [[ -L "$d/link.txt" && "$(readlink "$d/link.txt")" == "real.txt" ]]; then
        ok "symlink recreated with same target"
    else
        fail "symlink not recreated correctly"
    fi
}

case_hardlink_preserved() {
    case_begin "hardlink-preserved"
    local s="$work/hl_src" d="$work/hl_dst"
    mkdir -p "$s/sub"
    head -c 4096 /dev/urandom > "$s/a.bin"
    ln "$s/a.bin" "$s/b.bin"       # same-directory hard link
    ln "$s/a.bin" "$s/sub/c.bin"   # cross-directory hard link
    if ! run_ecopy buffered "$s" "$d" >/dev/null 2>&1; then
        fail "ecopy run failed"; return
    fi
    local f content_ok=1
    for f in a.bin b.bin sub/c.bin; do
        cmp -s "$s/a.bin" "$d/$f" || content_ok=0
    done
    if [[ "$content_ok" -eq 1 ]]; then
        ok "hard-linked files present with identical content"
    else
        fail "hard-linked content mismatch"
    fi
    local ia ib ic
    ia="$(stat -c %i "$d/a.bin")"; ib="$(stat -c %i "$d/b.bin")"; ic="$(stat -c %i "$d/sub/c.bin")"
    if [[ "$ia" == "$ib" && "$ia" == "$ic" ]]; then
        ok "hard links share one inode on dst"
    else
        fail "hard links not preserved (distinct inodes: $ia $ib $ic)"
    fi
    local nl
    nl="$(stat -c %h "$d/a.bin")"
    if [[ "$nl" -ge 3 ]]; then
        ok "dst link count preserved ($nl)"
    else
        fail "dst link count wrong ($nl)"
    fi
}

case_ssh_symlink_hardlink() {
    case_begin "ssh-symlink-hardlink"
    local s="$work/ssh_lnk_src" d="$work/ssh_lnk_dst"
    mkdir -p "$s/sub"
    printf 'payload\n' > "$s/real.txt"
    ln -s real.txt "$s/link.txt"
    head -c 4096 /dev/urandom > "$s/a.bin"
    ln "$s/a.bin" "$s/sub/c.bin"
    if ! run_ecopy_ssh "$s" "$d" >/dev/null 2>&1; then
        fail "remote ecopy run failed"; return
    fi
    if [[ -L "$d/link.txt" && "$(readlink "$d/link.txt")" == "real.txt" ]]; then
        ok "remote symlink recreated with same target"
    else
        fail "remote symlink not recreated correctly"
    fi
    local ia ic
    ia="$(stat -c %i "$d/a.bin" 2>/dev/null)"
    ic="$(stat -c %i "$d/sub/c.bin" 2>/dev/null)"
    if [[ -n "$ia" && "$ia" == "$ic" ]]; then
        ok "remote hard link shares one inode"
    else
        fail "remote hard link not preserved ($ia vs $ic)"
    fi
}

# Copy a mixed tree (regular + symlink + hard link) over the SSH transport with
# multiple parallel connections, and again with a single connection, asserting
# identical results and clean --verify. With ECOPY_SSH set the pool spawns N
# independent `ecopy --server` sessions (no ControlMaster), so this exercises the
# connection-pool refactor end-to-end without SSH auth.
case_ssh_parallel_connections() {
    case_begin "ssh-parallel-connections"
    local s="$work/sshpar_src"
    mkdir -p "$s/sub"
    printf 'payload\n' > "$s/real.txt"
    ln -s real.txt "$s/link.txt"
    head -c 4096 /dev/urandom > "$s/a.bin"
    ln "$s/a.bin" "$s/sub/c.bin"
    local i
    for i in $(seq 1 40); do head -c 20000 /dev/urandom > "$s/f_$i.bin"; done

    local -a base_env=(DIRECT_COPY_LARGE_THRESHOLD_MB=1 ECOPY_REMOTE_CMD="$bin")
    if [[ "${ECOPY_HARNESS_REAL_SSH:-0}" != "1" ]]; then
        base_env+=(ECOPY_SSH="$repo_root/tests/fake_ssh.sh")
    fi

    local n
    for n in 1 4; do
        local d="$work/sshpar_dst_$n"
        if ! env "${common_env[@]}" "${base_env[@]}" DIRECT_COPY_SSH_CONNECTIONS="$n" \
                 "$bin" --verify "$s" "ssh://localhost${d}" >/dev/null 2>&1; then
            fail "remote ecopy (N=$n) failed"; continue
        fi
        if verify_regular_files "$s" "$d"; then
            ok "N=$n: regular files match with clean --verify"
        fi
        if [[ -L "$d/link.txt" && "$(readlink "$d/link.txt")" == "real.txt" ]]; then
            ok "N=$n: symlink recreated with same target"
        else
            fail "N=$n: symlink not recreated correctly"
        fi
        local ia ic
        ia="$(stat -c %i "$d/a.bin" 2>/dev/null)"
        ic="$(stat -c %i "$d/sub/c.bin" 2>/dev/null)"
        if [[ -n "$ia" && "$ia" == "$ic" ]]; then
            ok "N=$n: hard link shares one inode"
        else
            fail "N=$n: hard link not preserved ($ia vs $ic)"
        fi
    done
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
       grep -q 'Verify seed       : 123' <<<"$out" &&
       grep -q 'Copy data rate' <<<"$out" &&
       grep -q 'Copied files rate' <<<"$out" &&
       grep -q 'Copy complete rate' <<<"$out" &&
       grep -q 'Verify object rate' <<<"$out" &&
       grep -q 'Verify sample rate' <<<"$out" &&
       grep -Eq 'Verify pending peak: [1-9]' <<<"$out" &&
       grep -q 'Verify readahead  : sequential' <<<"$out" &&
       grep -q 'Verify hash backend:' <<<"$out" &&
       ! grep -q 'Avg speed' <<<"$out"; then
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

    # Ownership that an unprivileged copy could not preserve must be reported as a
    # warning category, not a failure, so verification still exits 0.
    local do="$work/verify_ownership_dst"
    out="$(env "${common_env[@]}" DIRECT_COPY_DISABLE_DIRECT_IO=1 \
              ECOPY_TEST_FORCE_OWNERSHIP_MISMATCH=1 \
              "$bin" --verify-data=0 --verify-metadata --verify-seed=123 \
              "$s" "$do" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] &&
       grep -Eq 'Ownership not preserved: [1-9]' <<<"$out" &&
       grep -q 'Verify failures   : 0' <<<"$out"; then
        ok "local unpreservable ownership is a warning, not a failure"
    else
        fail "local ownership-unpreservable classification: $(tr '\n' ' ' <<<"$out")"
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
    if [[ "$rc" -eq 0 ]] && grep -q 'Verify failures   : 0' <<<"$out" &&
       grep -q 'Remote drain rate' <<<"$out"; then
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
    if [[ "$rc" -ne 0 ]] && grep -q 'remote reported' <<<"$out" &&
       grep -Eq 'data mismatches[[:space:]]*: [1-9]' <<<"$out"; then
        ok "remote skipped-file corruption is detected"
    else
        fail "remote skipped corruption was not detected: $(tr '\n' ' ' <<<"$out")"
    fi

    # An unprivileged target cannot chown; that must stay best-effort so the copy
    # still succeeds and verification runs, rather than failing at the barrier.
    local rce="$work/verify_remote_chown_eperm"
    out="$(env "${common_env[@]}" "${remote_env[@]}" \
              ECOPY_TEST_FORCE_CHOWN_EPERM=1 "$bin" \
              --verify --verify-seed=789 \
              "$s" "ssh://localhost${rce}" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] &&
       ! grep -q 'remote reported' <<<"$out" &&
       grep -Eq 'Verify objects[[:space:]]*: [1-9]' <<<"$out"; then
        ok "remote unprivileged chown is tolerated and verification still runs"
    else
        fail "remote chown-eperm tolerance/verify: $(tr '\n' ' ' <<<"$out")"
    fi

    # The server classifies unpreservable ownership as a warning category, so a
    # remote --verify-metadata run does not fail merely on uid/gid differences.
    local roe="$work/verify_remote_ownership"
    out="$(env "${common_env[@]}" "${remote_env[@]}" \
              ECOPY_TEST_FORCE_OWNERSHIP_MISMATCH=1 "$bin" \
              --verify-data=0 --verify-metadata --verify-seed=789 \
              "$s" "ssh://localhost${roe}" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] &&
       ! grep -q 'remote reported' <<<"$out" &&
       grep -Eq 'Ownership not preserved: [1-9]' <<<"$out" &&
       grep -q 'Verify failures   : 0' <<<"$out"; then
        ok "remote unpreservable ownership is a warning, not a failure"
    else
        fail "remote ownership-unpreservable classification: $(tr '\n' ' ' <<<"$out")"
    fi
}

case_verify_only() {
    case_begin "verify-only"
    local s="$work/verify_only_src" d="$work/verify_only_dst"
    local out rc before after content_before content_after
    local missing="$work/verify_only_missing"
    mkdir -p "$s/sub/deep"
    : > "$s/empty"
    printf 'unaligned-data' > "$s/sub/unaligned"
    dd if=/dev/urandom of="$s/data.bin" bs=4096 count=64 status=none
    truncate -s 1M "$s/sparse"
    printf sparse | dd of="$s/sparse" bs=1 seek=524288 conv=notrunc status=none
    chmod 0640 "$s/data.bin"
    cp -a "$s" "$d"
    local -a verify_paths=()
    mapfile -t verify_paths < <(cd "$s" && find . -print | sort)
    content_before="$(for rel in "${verify_paths[@]}"; do
        [[ -f "$d/$rel" ]] && sha256sum "$d/$rel"
    done)"
    for rel in "${verify_paths[@]}"; do
        touch -a -m -d @1700000000 "$s/$rel" "$d/$rel"
    done
    before="$(for rel in "${verify_paths[@]}"; do
        stat -c '%n|%f|%u|%g|%s|%X|%Y' "$d/$rel"
    done)"
    out="$("$bin" --verify-only --verify-workers=4 --verify-seed=77 \
              "$s" "$d" 2>&1)"; rc=$?
    after="$(for rel in "${verify_paths[@]}"; do
        stat -c '%n|%f|%u|%g|%s|%X|%Y' "$d/$rel"
    done)"
    content_after="$(for rel in "${verify_paths[@]}"; do
        [[ -f "$d/$rel" ]] && sha256sum "$d/$rel"
    done)"
    if [[ "$rc" -eq 0 ]] &&
       grep -q 'Verify failures   : 0' <<<"$out" &&
       grep -q 'Verify seed       : 77' <<<"$out" &&
       grep -Eq 'Verify hole blocks: [1-9]' <<<"$out" &&
       ! grep -q 'Files copied' <<<"$out" &&
       [[ "$before" == "$after" ]] &&
       [[ "$content_before" == "$content_after" ]]; then
        ok "default verify-only succeeds and leaves target unchanged"
    else
        fail "default verify-only/read-only guarantee: $(tr '\n' ' ' <<<"$out")"
    fi

    out="$("$bin" --verify-only --no-preserve-times --verify-data=0 \
              "$s" "$d" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] && grep -Eq 'Verify blocks[[:space:]]*: [1-9]' <<<"$out"; then
        ok "verify-only 0% keeps endpoint checks"
    else
        fail "verify-only endpoint coverage: $(tr '\n' ' ' <<<"$out")"
    fi
    for pct in 1 100; do
        if "$bin" --verify-only --no-preserve-times --verify-data="$pct" \
             --verify-seed=88 "$s" "$d" >/dev/null 2>&1; then
            ok "verify-only ${pct}% coverage succeeds"
        else
            fail "verify-only ${pct}% coverage failed"
        fi
    done

    if "$bin" --verify-only "$s" "$missing" >/dev/null 2>&1 ||
       [[ -e "$missing" ]]; then
        fail "verify-only created a missing local target"
    else
        ok "verify-only rejects and does not create a missing local target"
    fi

    cp -a "$d" "$work/verify_only_mode"
    chmod 0600 "$work/verify_only_mode/data.bin"
    if "$bin" --verify-only --no-preserve-times --verify-metadata \
         "$s" "$work/verify_only_mode" >/dev/null 2>&1; then
        fail "verify-only missed a mode mismatch"
    else
        ok "verify-only detects mode mismatch"
    fi

    cp -a "$d" "$work/verify_only_content"
    printf X | dd of="$work/verify_only_content/data.bin" bs=1 seek=4096 \
                  conv=notrunc status=none
    if "$bin" --verify-only --no-preserve-times --verify-data=100 \
         "$s" "$work/verify_only_content" >/dev/null 2>&1; then
        fail "verify-only missed data corruption"
    else
        ok "verify-only detects data mismatch"
    fi

    cp -a "$d" "$work/verify_only_wrong"
    rm "$work/verify_only_wrong/sub/unaligned"
    mkdir "$work/verify_only_wrong/sub/unaligned"
    if "$bin" --verify-only --no-preserve-times --verify-metadata \
         "$s" "$work/verify_only_wrong" >/dev/null 2>&1; then
        fail "verify-only missed wrong target type"
    else
        ok "verify-only detects wrong-type objects"
    fi
    rm -rf "$work/verify_only_wrong/sub/unaligned"
    if "$bin" --verify-only --no-preserve-times --verify-metadata \
         "$s" "$work/verify_only_wrong" >/dev/null 2>&1; then
        fail "verify-only missed a missing target object"
    else
        ok "verify-only detects missing target objects"
    fi

    printf extra > "$d/target-only-extra"
    if "$bin" --verify-only --no-preserve-times "$s" "$d" >/dev/null 2>&1; then
        ok "verify-only ignores target-only extras"
    else
        fail "verify-only rejected a target-only extra"
    fi

    local sf="$work/verify_one" tf="$work/verify_one_target"
    printf single-file > "$sf"
    cp -a "$sf" "$tf"
    touch -a -m -d @1700000000 "$sf" "$tf"
    if "$bin" --verify-only "$sf" "$tf" >/dev/null 2>&1; then
        ok "local single-file verify-only succeeds"
    else
        fail "local single-file verify-only failed"
    fi

    local many_s="$work/verify_many_src" many_d="$work/verify_many_dst"
    mkdir "$many_s"
    for i in $(seq 1 64); do
        dd if=/dev/urandom of="$many_s/f$i" bs=4096 count=32 status=none
    done
    cp -a "$many_s" "$many_d"
    out="$("$bin" --verify-only --no-preserve-times --verify-data=100 \
              --verify-workers=8 "$many_s" "$many_d" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] &&
       grep -Eq 'Verify workers[[:space:]]*: 8 configured, ([2-9]|[1-9][0-9]+) peak active' <<<"$out"; then
        ok "multiple local verifier workers become active"
    else
        fail "local verifier concurrency not observed: $(tr '\n' ' ' <<<"$out")"
    fi

    local rd="$work/verify_only_remote"
    local rmissing="$work/verify_only_remote_missing"
    local -a remote_env=(ECOPY_REMOTE_CMD="$bin")
    if [[ "${ECOPY_HARNESS_REAL_SSH:-0}" != "1" ]]; then
        remote_env+=(ECOPY_SSH="$repo_root/tests/fake_ssh.sh")
    fi
    cp -a "$s" "$rd"
    find "$s" "$rd" -exec touch -a -m -d @1700000000 {} +
    out="$(env "${remote_env[@]}" "$bin" --verify-only --verify-workers=4 \
              "$s" "ssh://localhost${rd}" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] &&
       grep -Eq 'Verify workers[[:space:]]*: 4 configured, ([2-9]|[1-9][0-9]+) peak active' <<<"$out"; then
        ok "remote verify-only uses parallel client/server verification"
    else
        fail "remote verify-only/concurrency: $(tr '\n' ' ' <<<"$out")"
    fi

    chmod 0700 "$rd/sub"
    out="$(env "${remote_env[@]}" "$bin" --verify-only --no-preserve-times \
              --verify-metadata "$s/sub" "ssh://localhost${rd}/sub" 2>&1)"; rc=$?
    if [[ "$rc" -ne 0 ]] &&
       grep -Eq 'metadata mismatches[[:space:]]*: 1$' <<<"$out" &&
       grep -q '(mode)' <<<"$out"; then
        ok "remote metadata-only directory verification detects mode mismatch"
    else
        fail "remote directory metadata mismatch was not isolated: $(tr '\n' ' ' <<<"$out")"
    fi

    # A 1 MiB file at 100% coverage fills a complete 256-digest batch.
    # Data-only verification must ignore metadata, while an enabled metadata
    # mismatch must be counted once for the object rather than once per batch.
    chmod 0600 "$rd/sparse"
    out="$(env "${remote_env[@]}" "$bin" --verify-only --no-preserve-times \
              --verify-data=100 "$s/sparse" "ssh://localhost${rd}" 2>&1)"; rc=$?
    if [[ "$rc" -eq 0 ]] && grep -q 'Verify failures   : 0' <<<"$out"; then
        ok "remote data-only verification ignores metadata mismatches"
    else
        fail "remote data-only verification checked metadata: $(tr '\n' ' ' <<<"$out")"
    fi
    out="$(env "${remote_env[@]}" "$bin" --verify-only --no-preserve-times \
              --verify-data=100 --verify-metadata "$s/sparse" \
              "ssh://localhost${rd}" 2>&1)"; rc=$?
    if [[ "$rc" -ne 0 ]] &&
       grep -Eq 'metadata mismatches[[:space:]]*: 1$' <<<"$out"; then
        ok "remote metadata mismatch is counted once per object"
    else
        fail "remote metadata mismatch count was not object-scoped: $(tr '\n' ' ' <<<"$out")"
    fi

    if env "${remote_env[@]}" "$bin" --verify-only "$s" \
         "ssh://localhost${rmissing}" >/dev/null 2>&1 ||
       [[ -e "$rmissing" ]]; then
        fail "remote verify-only created a missing target root"
    else
        ok "remote verify-only does not create missing target roots"
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
case_symlink_preserved
case_hardlink_preserved
case_ssh_symlink_hardlink
case_ssh_parallel_connections
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
case_verify_only

echo
echo "ecopy harness results: $pass_count passed, $fail_count failed"
if [[ "$fail_count" -ne 0 ]]; then
    exit 1
fi
echo "ecopy harness checks passed"
