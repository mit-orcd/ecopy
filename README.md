# direct_copy

`direct_copy` is a parallel directory copy tool aimed at moving large regular-file datasets quickly and predictably.
It is designed for high-throughput storage paths where the goal is to keep source, network, and target busy at the
same time.

## What It Does

- Walks a source directory tree and creates the matching target tree.
- Copies regular files only.
- Skips unchanged files based on `size + mtime`.
- Preserves important metadata from the source.
- Uses a shared worker-slot scheduler so small and large file workloads can share the same concurrency budget.
- Supports either direct I/O or buffered I/O, with runtime controls for both.
- Applies bounded queue backpressure so traversal does not run arbitrarily far ahead of copy workers.

The tool intentionally ignores non-regular payloads such as symlinks, devices, FIFOs, and sockets. It is tuned for
dataset movement, not full filesystem replication.

## Usage

```bash
/tmp/direct_copy [-v] <source_dir> <target_dir>
```

Examples:

```bash
/tmp/direct_copy /orcd/datasets/001/GLM51/ /scratch/GLM51/
```

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /orcd/datasets/001/GLM51/ /scratch/GLM51/
```

```bash
DIRECT_COPY_DISABLE_READ_DIRECT_IO=1 DIRECT_COPY_DISABLE_WRITE_DIRECT_IO=0 /tmp/direct_copy /src /dst
```

```bash
DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 /tmp/direct_copy /src /dst
```

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=128 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
```

## Scheduler Model

The current design uses a shared slot budget.

- Small files consume `1` worker slot.
- Large files are activated in parallel based on `DIRECT_COPY_MAX_WORKERS / (DIRECT_COPY_LARGE_READERS + DIRECT_COPY_LARGE_WRITERS)` when explicit reader/writer counts are in use.
- Each active large file gets a bounded large-file pipeline with preallocated aligned chunk buffers.
- Readers fill buffers, writers drain a ready queue, and the per-file in-flight depth is capped by `DIRECT_COPY_LARGE_FILE_INFLIGHT`.
- The total budget is capped by `DIRECT_COPY_MAX_WORKERS`.

This avoids the older problem where fixed small-file and large-file pools wasted capacity when one side of the workload
was idle.

Default behavior:

- `DIRECT_COPY_MAX_WORKERS = 32`
- `DIRECT_COPY_LARGE_READERS = 4`
- `DIRECT_COPY_LARGE_WRITERS = 2`
- `DIRECT_COPY_LARGE_FILE_INFLIGHT = 16`
- `DIRECT_COPY_CHUNK_MB = 1`
- `DIRECT_COPY_LARGE_THRESHOLD_MB = 128`
- `DIRECT_COPY_TRAVERSAL_WORKERS = 8`
- `DIRECT_COPY_MAX_QUEUED_FILES = 262144`

So by default:

- all-small workloads can run up to `32` files in parallel
- all-large workloads can run up to about `5` large files in parallel by default
- each active large file uses `4` readers and `2` writers by default
- each active large file can keep up to `16` chunk buffers in flight by default
- mixed workloads share the same slot pool instead of reserving idle capacity for one class of work
- traversal is backpressured once queued file work reaches the configured cap

## Copy Strategy

### Small Files

Small files use a simple copy path. When direct I/O is disabled, the buffered path can also use streaming hints and opportunistic `copy_file_range()` acceleration.

### Large Files

A file is treated as large when its size exceeds `DIRECT_COPY_LARGE_THRESHOLD_MB`. This threshold is independent of chunk size.

Default:

```text
128 MiB
```

This keeps large-file classification stable even when `DIRECT_COPY_CHUNK_MB` is tuned down for other reasons.

Large-file copy works like this:

1. The target file is created and pre-sized.
2. The large file is activated into its own bounded pipeline.
3. A pool of aligned chunk buffers is preallocated up front.
4. Reader threads use `pread()` to fill free buffers and push them into a ready-to-write queue.
5. Writer threads drain that ready queue with `pwrite()`.
6. `DIRECT_COPY_LARGE_FILE_INFLIGHT` limits how many chunk buffers each active large file may keep in flight at once.
7. Any remaining unaligned tail bytes are copied in a buffered tail pass.
8. Final metadata is restored after data copy completes.

This gives a real read-buffer-write pipeline for large files, with bounded queue depth and instrumentation that tells us whether readers are waiting for buffers or writers are waiting for data.

## Buffered I/O Streaming Optimizations

When direct I/O is disabled with `DIRECT_COPY_DISABLE_DIRECT_IO=1`, the copy path enables a few extra optimizations for
large sequential transfers:

- `posix_fadvise(..., POSIX_FADV_SEQUENTIAL)` to tell the kernel the source stream is sequential
- `posix_fadvise(..., POSIX_FADV_WILLNEED)` to encourage readahead on the source stream
- `posix_fadvise(..., POSIX_FADV_DONTNEED)` after source ranges are consumed, to reduce page-cache pollution
- opportunistic `copy_file_range()` for buffered copies, with automatic fallback to normal read/write if unsupported
- optional runtime disable switch for controlled A/B testing on filesystems where `copy_file_range()` is inconsistent or unhelpful

These hints are most relevant on buffered I/O paths over fast storage or network filesystems where the best-performing
behavior depends on the kernel, client, and server stack.

## Metadata Preservation

The tool preserves the following source metadata:

- `uid`
- `gid`
- permission bits
- `atime`
- `mtime`

This applies to:

- copied regular files
- skipped regular files
- directories after a final metadata pass

Newly created target directories stay owner-writable during the copy so child entries can still be created under source trees that contain read-only directories; the final directory pass restores the true source mode afterward.

Not preserved yet:

- xattrs
- ACLs
- SELinux labels
- file capabilities

## Runtime Controls

### `DIRECT_COPY_MAX_WORKERS`

Total shared slot budget across all work.

Default:

```text
32
```

### `DIRECT_COPY_LARGE_WORKERS`

Concurrency divisor used to determine how many large files may be active at once. The default active-large-file limit is roughly `DIRECT_COPY_MAX_WORKERS / DIRECT_COPY_LARGE_WORKERS`. 

Default:

```text
6
```

### `DIRECT_COPY_LARGE_FILE_INFLIGHT`

Maximum number of chunk tasks that each active large file may keep queued or in flight at once.

Default:

```text
16
```

This is the closest runtime knob to per-file queue depth for large-file NFS/RDMA tuning. Higher values can create more in-flight I/O, but can also increase contention.

In the final runtime summary, `large file readers/file` and `large file writers/file` show the actual split used for each active large-file pipeline.

### `DIRECT_COPY_LARGE_READERS`

Optional explicit override for large-file reader threads per active large file.

Default:

```text
4
```

### `DIRECT_COPY_LARGE_WRITERS`

Optional explicit override for large-file writer threads per active large file.

Default:

```text
2
```

Set these two together when you want a deterministic split such as `3 readers / 1 writer` or `6 readers / 2 writers`.
When both are set, their sum becomes the effective per-file large-worker total and is used to compute the active-large-file limit.

### `DIRECT_COPY_CHUNK_MB`

Chunk size in MiB used for aligned bulk transfer.

Default:

```text
1
```

Larger values may help high-bandwidth sequential workloads, but the best setting is environment-dependent.

### `DIRECT_COPY_LARGE_THRESHOLD_MB`

File-size threshold in MiB used to decide when a file enters the large-file pipeline. This is intentionally separate from `DIRECT_COPY_CHUNK_MB`.

Default:

```text
128
```

Use this when you want to keep medium-sized files on the simpler small-file path while still experimenting with small chunk sizes for true large-file transfers.

### `DIRECT_COPY_TRAVERSAL_WORKERS`

Number of parallel directory-walker threads used to discover directories and files.

Default:

```text
8
```

Higher values may reduce tree-discovery time on large namespace-heavy workloads, but can also increase metadata-server or filesystem contention. This knob is most relevant for trees with many small files and directories.

### `DIRECT_COPY_MAX_QUEUED_FILES`

Maximum number of queued regular-file tasks across the small-file and large-file queues before traversal blocks and waits for workers to drain backlog.

Default:

```text
262144
```

This limits memory growth on very large trees while still allowing traversal to stay comfortably ahead of the copy workers.

### `DIRECT_COPY_DISABLE_COPY_FILE_RANGE`

When unset or set to `0`, the tool may use `copy_file_range()` on buffered copy paths when the kernel and filesystem support it.

When set to any non-empty value other than `0`, the tool skips `copy_file_range()` entirely and uses the normal buffered read/write path.

Example:

```bash
DIRECT_COPY_DISABLE_COPY_FILE_RANGE=1 DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /src /dst
```

### `DIRECT_COPY_DISABLE_DIRECT_IO`

Legacy global switch. When unset or set to `0`, the tool allows direct I/O on both reads and writes.

When set to any non-empty value other than `0`, the tool disables direct I/O on both sides and uses buffered I/O paths instead.

Example:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /src /dst
```

When to try buffered I/O instead of direct I/O:

- small-file trees such as home directories, software installs, or source trees
- workloads with many files that are smaller than the large-file threshold
- cases where direct I/O causes frequent buffered tail fallbacks or extra reopen overhead
- environments where page cache and `copy_file_range()` improve small-file throughput

In practice, buffered I/O is often the better starting point for metadata-heavy trees, while direct I/O is usually more useful for large aligned streaming copies.

### `DIRECT_COPY_DISABLE_READ_DIRECT_IO`

When set to any non-empty value other than `0`, the tool disables direct I/O for source reads while leaving the write side unchanged.

Example:

```bash
DIRECT_COPY_DISABLE_READ_DIRECT_IO=1 DIRECT_COPY_DISABLE_WRITE_DIRECT_IO=0 /tmp/direct_copy /src /dst
```

### `DIRECT_COPY_DISABLE_WRITE_DIRECT_IO`

When set to any non-empty value other than `0`, the tool disables direct I/O for destination writes while leaving the read side unchanged.

The per-side switches override the old all-or-nothing behavior and make mixed mode possible, such as buffered reads from NFS and direct writes to local flash.

## Performance Tuning

For NFS/RDMA or other high-throughput flash-backed paths, the best settings are workload- and environment-dependent.
The current built-in defaults are already tuned toward a high-concurrency large-file profile:

```bash
DIRECT_COPY_DISABLE_READ_DIRECT_IO=0 DIRECT_COPY_DISABLE_WRITE_DIRECT_IO=0 DIRECT_COPY_MAX_WORKERS=256 DIRECT_COPY_LARGE_READERS=4 DIRECT_COPY_LARGE_WRITERS=2 DIRECT_COPY_LARGE_FILE_INFLIGHT=16 DIRECT_COPY_CHUNK_MB=1 DIRECT_COPY_LARGE_THRESHOLD_MB=128 DIRECT_COPY_TRAVERSAL_WORKERS=8 /tmp/direct_copy /src /dst
```

For small-file or metadata-heavy trees, try buffered I/O first. A practical starting point is:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_MAX_WORKERS=16 /tmp/direct_copy /src /dst
```

That profile is often better for trees with many tiny files because it reduces direct-I/O alignment overhead and avoids overdriving metadata operations with too many workers.

A useful benchmark matrix is:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=64  DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=128 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=256 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
```

Things to watch while tuning:

- completed copy throughput reported by `direct_copy`
- `Traversal seen seconds`, `File work drained sec`, `Finalize dir seconds`, and `Shutdown tail seconds` to identify whether the remaining time is in tree discovery, copy completion, final directory metadata, or teardown
- `Read opens` and `Write opens` counters to confirm actual direct-vs-buffered behavior
- `Queue wait seconds`, `Read time seconds`, `Write time seconds`, and `cfr time seconds` to identify where workers are stalling
- `Reader buf waits`, `Reader buf wait seconds`, `Writer data waits`, `Writer data wait seconds`, and `Ready queue peak/avg depth` to see whether the large-file pipeline is backpressured by reads, writes, or buffer availability
- `Large chunk buffer allocs` to confirm the large-file path preallocated and reused its bounded buffer pool
- final `copy_file_range` counters in the summary, to confirm whether the fast path was actually used
- target-side sustained write throughput
- source-side sustained read throughput
- client CPU usage and run queue pressure
- whether more concurrency improves throughput or just adds contention

## Design Decisions

### Why Shared Worker Slots?

A fixed split between small-file and large-file workers wastes concurrency when one workload class is missing. Shared
slots let the tool adapt naturally to all-small, all-large, and mixed workloads.

### Why Skip Based On Size And mtime?

It is a fast practical heuristic for bulk copy jobs. If a file matches on both size and `mtime`, the tool treats the
payload as already synchronized and then refreshes metadata so the target still matches the source.

### Why Offer Direct I/O As A Runtime Switch?

On some flash and NFS/RDMA stacks, `O_DIRECT` helps. On others, buffered I/O with kernel readahead and writeback is
faster. The switch exists because the best choice is environment-dependent.

### Why Add Buffered I/O Hints?

When direct I/O is disabled, the kernel can often help a lot with sequential streaming if it knows the access pattern.
The `posix_fadvise()` hints are a low-risk way to encourage readahead and avoid keeping already-consumed data hot in
page cache.

### Why Use `copy_file_range()` Opportunistically?

Some kernels and filesystems handle buffered range copies more efficiently than a userspace read/write loop. Others do
not. The tool treats `copy_file_range()` as an optimization, not a dependency, and falls back automatically when it is
unsupported or unsuitable.

### Why A Final Directory Metadata Pass?

Creating children changes parent directory metadata. Restoring directory ownership and timestamps only after the full
tree is materialized gives correct final metadata. The final pass is parallelized by directory depth so siblings can be finalized concurrently while still preserving child-before-parent ordering.

## Build

```bash
make
```

Warning-clean verification build:

```bash
make clean
make CFLAGS='-O2 -g -Wall -Wextra -Wpedantic -pthread'
```
