# ecopy

Parallel copy for large file trees over fast storage (local, NFS, or `ssh://`). It keeps source, network, and
target busy at once, skips unchanged files, preserves sparse files and hard links, and can verify what it copied.
Linux is the primary platform; macOS is supported with fallbacks (see [Platforms](#platforms)).

## Quick start

```bash
make
./ecopy /src/tree /dst/tree                     # copy a directory tree
./ecopy /src/data.bin /dst/                     # copy one file into a directory
./ecopy /src/tree ssh://user@host/data/tree     # push to another host
./ecopy --verify /src/tree /dst/tree            # copy, then check metadata + a 1% data sample
./ecopy --verify-only /src/tree /dst/tree       # check an existing copy; changes nothing
```

- `source` is a directory or a single regular file; `target` is a local path or `ssh://[user@]host[:port]/path`.
- A single-file target is the destination directory (file keeps its name) or, locally, a full path to rename it.
- For a directory source, source and target must not overlap. Missing target directories are created.
- Re-running is incremental: files with matching `size + mtime` are skipped.
- `-v` shows queue depths, verify worker counts, and the current file on the progress line.

Buffered I/O is often faster for many small files:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 ./ecopy /src /dst
```

## What is preserved

Regular files, directories, symlinks (verbatim, never followed), and hard links (copied once, then linked; a
cross-filesystem link falls back to a copy). Sparse files copy only their data extents. Permission bits, `atime`,
`mtime`, and `uid`/`gid` are preserved; ownership is best effort when unprivileged (see below).

Not preserved: xattrs, ACLs, SELinux labels, capabilities. Devices, FIFOs, and sockets are ignored on the source
and rejected on the target. A top-level source that is itself a symlink is rejected.

**Atomicity.** Into an *existing* target tree, each file is written to a same-directory temp file and renamed into
place after data and metadata are complete. Into a *new* target root, small files are written directly to their
final name for speed, so an interrupted first copy can leave partial files there. Do not let another writer
populate a root while its first copy runs.

## Options

| Flag | Effect |
| --- | --- |
| `-v`, `--verbose` | Detailed progress line and full diagnostic counters in the report |
| `--no-preserve-times` | Do not copy or verify `atime`/`mtime` |
| `--uid N`, `--gid N` | Force target ownership of every object to `N` (the other id stays from source) |
| `--verify` | After copy: metadata check plus a 1% sampled data check |
| `--verify-metadata` | Check type, size, mode, uid/gid, and timestamps |
| `--verify-data[=PCT]` | Check `PCT`% of aligned 4 KiB blocks (default 1); first and last block always |
| `--verify-skipped` | Also verify files skipped as unchanged |
| `--verify-seed N` | Reproducible block selection (default random; the seed used is printed) |
| `--verify-only` | Compare source and target without copying, creating, or repairing anything |
| `--verify-workers N` | Checker threads, 1–128 (default `min(cpus,16)`, `min(cpus,8)` over ssh) |

## Verification

- Verification is opt-in; plain copies pay nothing for it. For directory trees it runs *pipelined* with the copy,
  so `Verify wall sec` overlaps the copy timings — `Total Elapsed` is the ground truth.
- Data sampling is relative to *allocated* data: holes in sparse files are never read or hashed, so a 1% sample of a
  mostly-empty image reads 1% of its real data. Sampled holes on the target must read as zeros.
- Ownership that could not be preserved is not a failure. An unprivileged process cannot `chown` to a foreign uid;
  such objects are counted on a separate `Ownership not preserved` line and the run still exits 0. A group you belong
  to *is* applied even when the owner uid cannot be. Mode, size, and time mismatches are always failures.
- `--uid`/`--gid` are applied where each object's metadata is captured, so the copy and the verification agree on
  the forced ids. They do not reduce crawling and cannot bypass the privilege rules above.
- `--verify-only` requires every source file and directory to exist on the target, ignores target-only extras, and
  over ssh starts a read-only peer that refuses to create a missing root.
- Over ssh, 32-byte BLAKE3 digests are sent to the peer, which hashes its own blocks; file data never comes back.
- When timestamps are verified, reads use `O_NOATIME`. If the caller lacks permission for it, verification fails
  rather than invalidate the atime it is checking; use `--no-preserve-times` or metadata-only checks instead.

## SSH targets

```bash
./ecopy /local/src ssh://user@host:22/data/dst
```

- **Push only.** The source is always local. The peer runs `ecopy --server <path>` and performs all destination
  syscalls confined under `<path>`. If `ecopy` is missing or mismatched on the remote, the local binary is streamed
  over and run (same `uname -sm` required); install it in the remote `PATH` to skip this.
- **Parallel connections, one authentication.** `DIRECT_COPY_SSH_CONNECTIONS` (default 4) sessions share one
  `ControlMaster`, so an interactive prompt (MFA/Duo) fires once. Pipelined verify adds one more session
  automatically. `DIRECT_COPY_SSH_MULTIPLEX=0` gives each session its own TCP connection — and its own prompt.
- **Fast cipher, fallback safe.** `aes128-gcm@openssh.com` is *prepended* to the client's cipher list (OpenSSH ≥ 7.8),
  so the defaults remain as fallback and key exchange cannot fail because of it. `DIRECT_COPY_SSH_CIPHER=0` disables.
- **Latency aware.** Small files ship as a single frame with their metadata; directories are created by the first
  file written into them; metadata updates are fire-and-forget. A periodic and a final barrier drain the peer,
  flush to stable storage, and report any errors as a batch. There is no per-file `fsync`.
- The peer writes dense streamed files with `O_DIRECT` and runs `DIRECT_COPY_SSH_SERVER_THREADS` (default 16) apply
  threads so NFS metadata RPCs overlap. On autofs destinations it caches open directory handles to avoid re-walking
  paths. Mounting the destination with `nconnect=N` raises the RPC ceiling further.
- `ECOPY_SSH` overrides the ssh command (e.g. `ssh -i key`); `ECOPY_REMOTE_CMD` points at a specific remote binary.

## How it copies

- **Small files** (≤ `DIRECT_COPY_LARGE_THRESHOLD_MB`, default 10) use a many-way worker pool.
- **Large files** run a bounded reader → queue → writer pipeline with aligned chunk buffers, preallocating the
  destination with `fallocate()` so block allocation stays off the write path.
- **Sparse files** are found with `SEEK_DATA`/`SEEK_HOLE` and copied hole-skipping regardless of size.
- **Dispatch** is biggest-allocated-data first, so large files start filling the link as soon as they are found.
  `DIRECT_COPY_SIZE_PRIORITY=0` restores discovery order.
- Directories are read with raw `getdents64` and every entry is opened relative to its parent handle, so symlink
  swaps during traversal are rejected.

## Tuning

Defaults suit large streaming copies. Out-of-range values are clamped with a warning.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DIRECT_COPY_MAX_WORKERS` | 256 | Total worker-slot budget (2–512) |
| `DIRECT_COPY_SMALL_MAX_WORKERS` | 32 | Concurrent small-file workers (also the thread cap for ssh targets) |
| `DIRECT_COPY_LARGE_WORKERS` | 6 | Concurrent large files |
| `DIRECT_COPY_LARGE_READERS` / `_WRITERS` | 4 / 2 | Threads per active large file |
| `DIRECT_COPY_LARGE_FILE_INFLIGHT` | 16 | Chunk buffers in flight per large file |
| `DIRECT_COPY_CHUNK_MB` | 1 | Chunk size (1–4096) |
| `DIRECT_COPY_LARGE_THRESHOLD_MB` | 10 | Small/large boundary; keep it above your typical medium file |
| `DIRECT_COPY_DIRECT_IO_MIN_SIZE_MB` | 10 | Use direct I/O only at or above this size (`0` = always) |
| `DIRECT_COPY_DISABLE_DIRECT_IO` | 0 | Buffered I/O everywhere; `_READ_` / `_WRITE_` variants split it |
| `DIRECT_COPY_DISABLE_COPY_FILE_RANGE` | 0 | Never use `copy_file_range()` |
| `DIRECT_COPY_TRAVERSAL_WORKERS` | 8 | Parallel directory walkers |
| `DIRECT_COPY_GETDENTS_BUF` | 262144 | Per-walker `getdents64` buffer; `0` uses libc `readdir` |
| `DIRECT_COPY_MAX_QUEUED_FILES` | 262144 | Backpressure cap on queued file tasks |
| `DIRECT_COPY_SIZE_PRIORITY` | 1 | Biggest-first dispatch; `0` = discovery order |
| `DIRECT_COPY_SMALL_INPLACE` | 0 | Final-name writes even into existing trees (not crash-atomic) |
| `DIRECT_COPY_NO_PRESERVE_TIMES` | 0 | Same as `--no-preserve-times` |
| `DIRECT_COPY_VERIFY_WORKERS` | see `--verify-workers` | Checker threads |
| `DIRECT_COPY_VERIFY_QUEUE_MAX` | 262144 | Verify intake queue bound |
| `DIRECT_COPY_VERIFY_PIPELINE_OPS` | 4096 | Remote small files released per verify generation |
| `ECOPY_SSH` | `ssh` | Command used to reach an `ssh://` target |
| `ECOPY_REMOTE_CMD` | `ecopy` | Remote `ecopy` command |
| `DIRECT_COPY_SSH_CONNECTIONS` | 4 | Copy sessions per run (1–16) |
| `DIRECT_COPY_SSH_VERIFY_CONNECTIONS` | auto (1) | Extra sessions for pipelined verify; `0` shares the copy pool |
| `DIRECT_COPY_SSH_MULTIPLEX` | 1 | One `ControlMaster` auth for all sessions; `0` = one prompt per session |
| `DIRECT_COPY_SSH_CIPHER` | fast AEAD list | Ciphers prepended to the ssh defaults; `0` disables |
| `DIRECT_COPY_SSH_PUTFILE_MAX` | 1024 | Max KiB for a file to ship as one frame |
| `DIRECT_COPY_SSH_BARRIER_OPS` | 8192 | Fire-and-forget ops between drain/flush barriers (min 256) |
| `DIRECT_COPY_SSH_SERVER_THREADS` | 16 | Apply threads on the peer (max 256) |
| `DIRECT_COPY_SSH_SERVER_DIRECT_IO` | 1 | Peer writes streamed files with `O_DIRECT`; `0` = buffered |
| `DIRECT_COPY_SSH_SERVER_DIRFD_CACHE` | auto | Directory handles the peer caches; `0` favors fewer fds |

## Reading the report

- **Copy data rate** covers the time until all file workers drain; **Copy complete** adds directory finalization and
  the remote flush. **Payload** is bytes moved; **logical** is source file size; the difference is sparse savings.
- **Remote drain rate/busy** (ssh) is how fast the peer's `write`/`fsync` consumed the stream. Busy time close to
  the copy time, or a rate far below the link, means the peer's storage is the bottleneck.
- **Transfer distribution** rates are summed worker service time, not wall-clock throughput. Percentiles come from
  logarithmic histograms (approximate); min/max are exact but outlier-sensitive.
- **Verify** rates are computed from summed checker time (`Verify busy sec`) for the same reason.

## Safety

Test on your storage stack first. This is not `rsync` and not a replication tool; for independent full-tree
assurance, audit a run with a trusted tool as well. Prefer an empty or trusted target: existing regular files may be
skipped or atomically replaced, and existing non-regular target entries are rejected.

## Build and test

```bash
make            # ecopy
make test       # protocol and telemetry unit tests, the harness, and the smoke tests
```

`tests/ecopy_harness.sh` builds varied trees (small, large, sparse, hole-only, symlinks, hard links, read-only) and
checks that ecopy reproduces them across runtime profiles, including an `ssh://localhost` loopback that runs
`ecopy --server` over a local pipe (`tests/fake_ssh.sh`) so no keys are needed. `ECOPY_HARNESS_REAL_SSH=1` uses real
ssh; `ECOPY_HARNESS_VERBOSE=1` prints every passing check.

Warning-clean build: `make clean && make CFLAGS='-O2 -g -Wall -Wextra -Wpedantic -pthread'`.

## Platforms

Linux is the primary target. macOS builds and passes the same tests using fallbacks in `compat.h`:

| Linux | macOS |
| --- | --- |
| `O_DIRECT` | `fcntl(F_NOCACHE)` after open |
| `copy_file_range()` | `pread`/`pwrite` |
| `getdents64()` | `readdir()` |
| `posix_fadvise()` | `fcntl(F_RDAHEAD)`; `DONTNEED` is a no-op |
| `fallocate()` | `fcntl(F_PREALLOCATE)` + `ftruncate()` |
| `O_NOATIME` | none — **atime is copied but not verified** (reading a file moves it), announced once per run |

## License

MIT. See [LICENSE](LICENSE). Copyright Michel Erb (2026).
