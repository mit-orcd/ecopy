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

The tool intentionally ignores non-regular payloads such as symlinks, devices, FIFOs, and sockets. It is tuned for
dataset movement, not full filesystem replication.

## Usage

```bash
/tmp/direct_copy <source_dir> <target_dir>
```

Examples:

```bash
/tmp/direct_copy /orcd/datasets/001/GLM51/ /scratch/GLM51/
```

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /orcd/datasets/001/GLM51/ /scratch/GLM51/
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
- Large files are activated in parallel based on `DIRECT_COPY_MAX_WORKERS / DIRECT_COPY_LARGE_WORKERS`.
- Each active large file can keep up to `DIRECT_COPY_LARGE_FILE_INFLIGHT` chunk tasks queued or in flight.
- Large chunk workers reuse a worker-local aligned buffer instead of allocating one buffer per chunk task.
- The total budget is capped by `DIRECT_COPY_MAX_WORKERS`.

This avoids the older problem where fixed small-file and large-file pools wasted capacity when one side of the workload
was idle.

Default behavior:

- `DIRECT_COPY_MAX_WORKERS = 16`
- `DIRECT_COPY_LARGE_WORKERS = 4`
- `DIRECT_COPY_LARGE_FILE_INFLIGHT = 4`

So by default:

- all-small workloads can run up to `16` files in parallel
- all-large workloads can run up to `4` large files in parallel by default
- each active large file can keep up to `4` chunk tasks in flight by default
- mixed workloads share the same slot pool instead of reserving idle capacity for one class of work

## Copy Strategy

### Small Files

Small files use a simple copy path. When direct I/O is disabled, the buffered path can also use streaming hints and opportunistic `copy_file_range()` acceleration.

### Large Files

A file is treated as large when its size exceeds:

```text
10 * CHUNK_SIZE
```

With the current defaults, that is `640 MiB`.

Large-file copy works like this:

1. The target file is created and pre-sized.
2. The large file is activated into the global chunk scheduler.
3. Aligned ranges are emitted as chunk tasks instead of assigning one fixed quarter-file slice to each worker.
4. Each active large file can keep up to `DIRECT_COPY_LARGE_FILE_INFLIGHT` chunk tasks queued or in flight.
5. Worker threads pull chunk tasks from the shared global queue, which improves load balancing and keeps more I/O in flight.
6. Any remaining unaligned tail bytes are copied in a buffered tail pass.
7. Final metadata is restored after data copy completes.

This gives wide sequential I/O on large files without forcing all workloads into a chunk scheduler.

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
16
```

### `DIRECT_COPY_LARGE_WORKERS`

Concurrency divisor used to determine how many large files may be active at once. The default active-large-file limit is roughly `DIRECT_COPY_MAX_WORKERS / DIRECT_COPY_LARGE_WORKERS`. 

Default:

```text
4
```

### `DIRECT_COPY_LARGE_FILE_INFLIGHT`

Maximum number of chunk tasks that each active large file may keep queued or in flight at once.

Default:

```text
4
```

This is the closest runtime knob to per-file queue depth for large-file NFS/RDMA tuning. Higher values can create more in-flight I/O, but can also increase contention.

### `DIRECT_COPY_CHUNK_MB`

Chunk size in MiB used for aligned bulk transfer.

Default:

```text
64
```

Larger values may help high-bandwidth sequential workloads, but the best setting is environment-dependent.

### `DIRECT_COPY_DISABLE_COPY_FILE_RANGE`

When unset or set to `0`, the tool may use `copy_file_range()` on buffered copy paths when the kernel and filesystem support it.

When set to any non-empty value other than `0`, the tool skips `copy_file_range()` entirely and uses the normal buffered read/write path.

Example:

```bash
DIRECT_COPY_DISABLE_COPY_FILE_RANGE=1 DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /src /dst
```

### `DIRECT_COPY_DISABLE_DIRECT_IO`

When unset or set to `0`, the tool tries direct I/O first.

When set to any non-empty value other than `0`, the tool disables direct I/O and uses buffered I/O paths instead.

Example:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /src /dst
```

## Performance Tuning

For NFS/RDMA or other high-throughput flash-backed paths, the best settings are workload- and environment-dependent.
A good starting point for large-file tests is:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 DIRECT_COPY_CHUNK_MB=128 /tmp/direct_copy /src /dst
```

A useful benchmark matrix is:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=64  DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=128 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=256 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_WORKERS=4 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 /tmp/direct_copy /src /dst
```

Things to watch while tuning:

- completed copy throughput reported by `direct_copy`
- `Read opens` and `Write opens` counters to confirm actual direct-vs-buffered behavior
- `Queue wait seconds`, `Read time seconds`, `Write time seconds`, and `cfr time seconds` to identify where workers are stalling
- `Large chunk buffer allocs` to confirm the large-file path is reusing worker-local buffers instead of churning allocations
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
tree is materialized gives correct final metadata.

## Build

```bash
make
```

Warning-clean verification build:

```bash
make clean
make CFLAGS='-O2 -g -Wall -Wextra -Wpedantic -pthread'
```
