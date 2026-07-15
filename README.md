# ecopy

`ecopy` is a parallel directory copy tool for moving large regular-file datasets quickly over high-throughput
storage paths (tested on NFS v4.2 exports and local filesystems). It keeps source, network, and target busy at the
same time using a shared pool of read/write/traversal workers.

## Quick Start

```bash
make
./ecopy <source> <target>
```

- `source` may be an existing directory or a single regular file.
- For a **directory** source, source and target must not overlap; the target root and the matching tree beneath it
  are created as needed.
- For a **single file** source, `target` is the destination directory (the file keeps its name), or — locally — a
  full destination path to also rename it. `ssh://` targets are always treated as the destination directory.
- Add `-v`/`--verbose` to print the resolved config and full diagnostic counters.

Common variations:

```bash
# Copy one file into a directory (keeps its name)
./ecopy /src/data.bin /dst/

# Copy + rename one file (local only)
./ecopy /src/data.bin /dst/data-2026.bin

# Push one file to a remote directory
./ecopy /src/data.bin ssh://user@host/data/incoming/

# Many small files / metadata-heavy trees: buffered I/O is often faster
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_MAX_WORKERS=16 ./ecopy /src /dst

# Large aligned streaming copies (defaults are already tuned for this)
./ecopy /src /dst

# Mixed mode: buffered reads, direct writes
DIRECT_COPY_DISABLE_READ_DIRECT_IO=1 ./ecopy /src /dst
```

## Remote (SSH) Targets

The target may be an `ssh://` URL to push a local tree to another host:

```bash
./ecopy /local/src ssh://user@host:22/data/dst
```

- **Push only.** The source is always local; an `ssh://` source is rejected.
- `ecopy` opens one SSH connection and runs `ecopy --server <path>` on the remote host, then streams
  data and metadata over a single pipelined binary channel. Reads stay local (O_DIRECT, sparse detection);
  the remote peer performs the destination syscalls confined under `<path>`.
- **Latency-aware by design:** small files, directory creation, and metadata updates are *fire-and-forget* (no
  per-item round-trip). A small file at or below `DIRECT_COPY_SSH_PUTFILE_MAX` ships as a single frame (path +
  metadata + data); larger files stream in pipelined chunks. Many files stream concurrently. Sparse files send
  only their data extents so holes are recreated remotely.
- **Directories are created lazily by their contents:** the first file written into a directory creates it (and any
  missing parents), so no separate `MKDIR` round-trip is spent on directories that hold files. A shared
  sharded single-flight cache guarantees that only one server worker creates a path; the others reuse the result instead of
  duplicating NFS LOOKUP/MKDIR RPCs. Missing parents are walked only after an optimistic leaf `mkdir` returns
  `ENOENT`. An explicit `MKDIR` is sent only for directories that end up with no files.
- **Batched durability:** each file is written and (incrementally) atomically renamed into place, but there is no
  per-file `fsync`. A periodic and a final *barrier* drain the server, flush to stable storage, and report any
  errors as a batch (with the first offending path); a nonzero count fails the run. Tune the barrier cadence with
  `DIRECT_COPY_SSH_BARRIER_OPS`.
- **Parallel remote apply:** over NFS each remote metadata op (mkdir, create, chown, chmod, times, rename) is a
  separate synchronous RPC, so a single-threaded peer sits idle waiting on latency. The remote runs an apply pool
  (`DIRECT_COPY_SSH_SERVER_THREADS`, default 16) so file writes and final directory metadata run concurrently.
  Barriers before finalization and between directory-depth groups preserve ordering for restrictive parent modes.
- **Batched client scheduling:** traversal appends each stat group to the file queues under one lock instead of
  locking and signaling once per file. The same scheduler path is used for local/NFS and SSH destinations.
- **Fewer RPCs per file:** the peer creates files with their final mode (no extra `fchmod`) and skips `chown` when
  the owner already matches. File-bearing directories are also created with their source mode when it remains
  owner-writable, allowing finalization to skip a redundant `fchmod`. A fresh small file costs about a create +
  write + one time-stamp RPC. Add
  `--no-preserve-times` to drop the time-stamp SETATTR too (atime/mtime are not carried over), leaving roughly a
  create + write per file.
- **Fresh vs. incremental:** on the first copy into a non-existent destination root, per-directory bulk stats are
  skipped entirely and files are written straight to their final name (no temp + rename). Re-running into an
  existing destination keeps one bulk stat per directory (so unchanged files by `size + mtime` are skipped) and
  writes via temp + atomic rename.
- **Self-bootstrapping:** if the remote `ecopy` is missing or an incompatible version, the local binary is
  streamed over and executed (guarded by a matching `uname -sm`). Install `ecopy` in the remote `PATH` to skip this.
- Requires working SSH access (key-based auth recommended). Set `ECOPY_SSH` to override the ssh command and
  `ECOPY_REMOTE_CMD` to point at a specific remote `ecopy` binary.
- **Tip:** when the remote peer writes to an NFS-mounted destination, mounting it with `nconnect=N` (multiple TCP
  connections) is a complementary, zero-code way to raise the concurrent-RPC ceiling that the apply pool feeds.

## What It Does

- Walks the source tree and recreates it on the target: regular files, directories, symlinks, and hard links.
- Recreates symlinks verbatim without ever following them (`cp -d` semantics): the link's target string is
  preserved unchanged, and the link's `uid`/`gid`/times are applied to the link itself.
- Preserves hard links: files sharing a source inode are copied once (the first occurrence), and additional links
  become real hard links on the target instead of duplicating the data. A cross-filesystem link (`EXDEV`) falls back
  to a full copy locally so no file is lost.
- Skips unchanged files based on `size + mtime`.
- Detects sparse files and copies only their data, preserving holes instead of moving zeros.
- On a newly created destination tree, writes small files directly to their final names to avoid destination stats
  and rename RPCs. Interrupted runs can therefore leave partial files in that new tree.
- On an existing destination, copies through same-directory temp files and renames into place after data + metadata
  are complete (crash-atomic).
- Opens entries relative to open directory handles, so symlink swaps are rejected during traversal and copy.
- Preserves `uid`/`gid` (when permitted), permission bits, `atime`, and `mtime`.

The final report shows `Symlinks : <created> of <seen>` and `Hard links : <created> linked, <N> not duplicated`
whenever the source tree contains them. Symlinks and hard-link secondaries are not enrolled in `--verify` (the
primary file's data is verified, and a hard link shares its content by construction).

Not preserved: xattrs, ACLs, SELinux labels, file capabilities. Other non-regular entries (devices, FIFOs, sockets)
are ignored on the source and rejected on the target rather than reconciled. A top-level source that is itself a
symlink is rejected.

## Transfer Verification

Verification is opt-in, so ordinary copies pay no extra opens, reads, hashing, allocations, or protocol traffic:

```bash
# Exact preserved-metadata checks plus the default 1% data sample
./ecopy --verify /src /dst

# Check every logical 4 KiB block
./ecopy --verify-data=100 --verify-metadata /src /dst

# Also verify files skipped by the size+mtime incremental test
./ecopy --verify --verify-skipped /src /dst

# Verify an existing tree without copying, creating, or repairing anything
./ecopy --verify-only /src /dst

# Read-only verification over SSH with explicit checker parallelism
./ecopy --verify-only --verify-workers=8 /src ssh://host/existing/dst
```

- `--verify-metadata` checks object type, regular-file size, permission and special bits, numeric UID/GID, and
  atime/mtime when time preservation is enabled. It does not check metadata ecopy does not copy (ctime/birth time,
  ACLs, xattrs, labels, or capabilities).
- Ownership that could not be preserved is not a failure. An unprivileged process cannot change a file's owner
  uid, so when the only metadata difference is UID/GID and the copy ran unprivileged, ecopy skips the doomed
  `chown` during copy, warns once, and reports the count on a separate `Ownership not preserved` line rather than
  counting it under `Verify failures` (so such a run still exits 0). Genuine mode/size/time mismatches, and any
  UID/GID mismatch when running privileged, remain hard failures.
- `--verify-data[=PERCENT]` samples aligned logical 4 KiB blocks. The first and final block are always checked,
  including a short final block; empty files receive size/metadata checks only. The percentage sets the total target
  block count including those endpoints, so small files can have higher achieved coverage than requested.
- Sampling uses one random per-run seed. `--verify-seed=N` reproduces the same offsets, and the effective seed plus
  requested/achieved logical-byte coverage are printed in the final report.
- `--verify-skipped` includes files that were not copied because size+mtime matched. Without it, checks apply only
  to files copied in this invocation.
- `--verify-only` never starts copy workers and never creates, truncates, renames, or repairs target objects. It
  walks the source once, requires each source regular file and directory to exist at the corresponding target path,
  and ignores target-only extras. It defaults to metadata plus 1% sampled data unless explicit
  `--verify-metadata` or `--verify-data[=PERCENT]` selectors are supplied. `--verify-skipped` is redundant in this
  mode. Local single-file target mapping is the same as copy mode; an SSH target is always a directory.
- Verification uses a bounded post-copy/read-only worker pool. `--verify-workers=N` or
  `DIRECT_COPY_VERIFY_WORKERS=N` selects 1–128 workers. Defaults are the online CPU count capped at 16 locally and
  8 for SSH. Copy and verification remain separate phases, so the checker pool does not compete with bulk copy
  workers.
- Local targets compare source and destination bytes directly. SSH targets send full 32-byte BLAKE3 digests in
  bounded batches and hash target blocks in the remote server pool; sampled file data is not sent back over SSH and
  there is one phase barrier rather than a per-file round trip. SSH verify-only starts a read-only peer and refuses
  to create a missing target root.
- Sparse files are checked as logical content: sampled holes read as zero. This verifies bytes and size, not the
  target's physical extent layout. A source extent map lets sampled source holes skip the source read and hash while
  still reading the target and requiring zero bytes. Unsupported extent discovery falls back to ordinary reads.
- Partial samples retain the same seed-derived block set but read each bounded batch in offset order with random-I/O
  readahead advice. A 100% check is a sequential scan with sequential readahead. BLAKE3 1.8.2's official C
  implementation selects AVX-512, AVX2, SSE4.1, SSE2, or the portable backend at runtime; the selected backend is
  printed in the final verification report.

Enabled verification necessarily adds I/O. At 1%, large files add approximately 1% source reads and 1% target
reads; mandatory endpoint reads dominate tiny-file workloads. `--no-preserve-times` excludes timestamps from the
metadata comparison. When timestamps are checked, data reads use `O_NOATIME`; if the caller lacks permission for
that flag, verification fails rather than invalidating the atime it is checking. Use metadata-only verification or
`--no-preserve-times` when `O_NOATIME` is unavailable.

### Final report timing and percentiles

The final report separates phases so verification time never depresses the reported copy rate:

- **Copy data elapsed/rate** runs from process statistics initialization until all file workers drain. **Copy
  complete elapsed/rate** also includes directory finalization and the remote flush immediately before verification.
  **Payload bytes** are bytes actually moved; **logical bytes** are selected source file sizes. Their difference is
  reported as sparse savings.
- For SSH targets, **Remote drain rate / busy** report how many payload bytes the remote peer wrote and how long it
  spent in the `write`/`fsync` syscalls that consume the stream (summed server-side service time, reported at each
  barrier). A drain busy time close to the copy elapsed time, or a drain rate far below your link speed, means the
  bottleneck is the remote peer's storage or a loaded remote host rather than the client or the network.
- **Transfer distribution** values are worker-service metrics measured from task claim through successful
  finalization. Small dense, large dense (the configured large threshold), and sparse files have separate
  populations. Latency includes zero-byte files; per-file effective throughput excludes them. A remote PUTFILE
  sample ends after client send/backpressure completion, while streamed remote files include COMMIT acknowledgement.
  The **summed-service rate** divides class payload by service time summed across workers; it is a worker-equivalent
  efficiency measure, not wall-clock transfer throughput. The report also prints copied files/s.
- One-second payload-rate windows begin with the first payload and end when file work drains. They include zero-rate
  stall windows and are collected even when stdout is redirected; only the live 10-second rolling display requires a
  terminal.
- **Verification wall time/rate** measures the checker phase and sampled bytes. Scope, achieved coverage, verified
  objects/s, the compact pre-phase pending peak, bounded worker-queue peak, hole reads avoided, hash backend, and
  categorized failures are reported independently.

Percentiles use bounded integer logarithmic histograms: counts, sums, and min/max are exact, while percentile values
are bucket approximations. Min and max are intentionally shown but are highly sensitive to single-file and
single-window outliers.

## Safety

Use at your own risk and test on your storage stack first. This is not `rsync` and not a full replication tool;
for independent full-tree assurance, checking a run with `rsync` or another trusted tool is still good practice. Prefer an empty or
trusted target tree: existing regular files may be skipped or atomically replaced, and existing non-regular target
entries are rejected. Do not let another writer populate a destination root while its first copy is running: a root
created by this invocation uses the faster non-atomic final-name path.

## How It Copies

- **Small files** use a simple copy path. With buffered I/O they can also use streaming `posix_fadvise()` hints and
  opportunistic `copy_file_range()`. Local/NFS and SSH traversal share 512-entry stat/queue batches; a fresh target
  skips destination existence stats, while an incremental target keeps `size + mtime` skip checks.
- **Large files** (size > `DIRECT_COPY_LARGE_THRESHOLD_MB`, default 10) run a bounded reader→queue→writer pipeline
  with preallocated aligned chunk buffers. The destination is preallocated up front with `fallocate()` (falling back
  to `ftruncate()`), which keeps block allocation off the write path and avoids late-run throughput collapse. Keep
  this well below your typical medium-file size: files under the threshold use the many-way small-file pool, where a
  large number of concurrent medium files thrash a bandwidth-limited target.
- **Sparse files** are routed to a hole-skipping path regardless of logical size: data regions are found with
  `lseek(SEEK_DATA/SEEK_HOLE)`, only data is copied, and the destination is `ftruncate()`d to the exact source size.

## Tuning

Best settings are workload- and environment-dependent. Key knobs (defaults in parentheses):

| Variable | Default | Purpose |
| --- | --- | --- |
| `DIRECT_COPY_MAX_WORKERS` | 256 | Total shared worker-slot budget (clamped 2–512) |
| `DIRECT_COPY_SMALL_MAX_WORKERS` | 32 | Cap on concurrent small-file workers; also caps threads created for SSH targets |
| `DIRECT_COPY_LARGE_READERS` / `DIRECT_COPY_LARGE_WRITERS` | 4 / 2 | Threads per active large file |
| `DIRECT_COPY_LARGE_FILE_INFLIGHT` | 16 | Chunk buffers in flight per large file |
| `DIRECT_COPY_CHUNK_MB` | 1 | Aligned bulk-transfer chunk size (1–4096) |
| `DIRECT_COPY_LARGE_THRESHOLD_MB` | 10 | Size at which a file enters the large-file pipeline |
| `DIRECT_COPY_TRAVERSAL_WORKERS` | 8 | Parallel directory-walker threads |
| `DIRECT_COPY_MAX_QUEUED_FILES` | 262144 | Backpressure cap on queued file tasks |
| `DIRECT_COPY_SMALL_INPLACE` | 0 | Force final-name writes even on existing targets (fresh trees do this automatically; not crash-atomic) |
| `DIRECT_COPY_DISABLE_DIRECT_IO` | 0 | Disable `O_DIRECT` on both sides (buffered I/O) |
| `DIRECT_COPY_DISABLE_READ_DIRECT_IO` / `DIRECT_COPY_DISABLE_WRITE_DIRECT_IO` | 0 / 0 | Per-side direct-I/O switches |
| `DIRECT_COPY_DISABLE_COPY_FILE_RANGE` | 0 | Skip `copy_file_range()` on buffered paths |
| `ECOPY_SSH` | `ssh` | Command used to reach an `ssh://` target (e.g. `ssh -i key`) |
| `ECOPY_REMOTE_CMD` | `ecopy` | Remote `ecopy` binary/command for `ssh://` targets |
| `DIRECT_COPY_SSH_PUTFILE_MAX` | 1024 | Max size (KiB) a file may be to ship as a single `ssh://` PUTFILE frame |
| `DIRECT_COPY_SSH_BARRIER_OPS` | 8192 | Fire-and-forget remote ops between drain/flush barriers (min 256) |
| `DIRECT_COPY_SSH_SERVER_THREADS` | 16 | Apply threads on the `ssh://` peer; higher hides more per-op RPC latency (max 256) |
| `DIRECT_COPY_VERIFY_WORKERS` | local: min(CPUs, 16); SSH: min(CPUs, 8) | Bounded post-copy/verify-only checker threads (1–128) |
| `DIRECT_COPY_NO_PRESERVE_TIMES` | 0 | Skip atime/mtime on local/NFS and `ssh://` targets (same as `--no-preserve-times`) |

Out-of-range numeric values are clamped with a warning. The final report prints one suggested next-run experiment;
`-v` adds the full diagnostic counters (per-phase seconds, read/write opens, queue/buffer waits, etc.) to help you
see whether time is going to traversal, copy, metadata, or teardown.

## Build & Test

```bash
make          # builds ecopy (and ecopy-jemalloc if jemalloc is available)
make test     # runs protocol_test, the smoke tests, and tests/ecopy_harness.sh
```

`tests/ecopy_harness.sh` builds varied source trees (small/large/sparse/pure-hole files, nested dirs, symlinks,
read-only files) and verifies ecopy reproduces them across several runtime profiles, including an `ssh://localhost`
loopback profile. The loopback profile uses `tests/fake_ssh.sh` (a stand-in that runs `ecopy --server` over a local
pipe) so the full protocol path is exercised without SSH keys; set `ECOPY_HARNESS_REAL_SSH=1` to use real ssh.
Run it directly with `tests/ecopy_harness.sh` (or `ECOPY_HARNESS_VERBOSE=1 tests/ecopy_harness.sh`).
`make protocol_test` builds the wire-protocol unit tests.

Warning-clean verification build:

```bash
make clean && make CFLAGS='-O2 -g -Wall -Wextra -Wpedantic -pthread'
```

## License

MIT License. See [LICENSE](LICENSE). Copyright held by **Michel Erb** (2026).
