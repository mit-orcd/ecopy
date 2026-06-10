# ecopy

`ecopy` is a parallel directory copy tool aimed at moving large regular-file datasets quickly and predictably.
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
- Opens source and target entries relative to already-opened directory handles so symlink swaps are rejected during traversal and copy.
- Copies through same-directory temporary files and renames them into place only after data and metadata are complete.

## Safety And Limitations

Use this tool at your own risk. It is intended for controlled bulk dataset movement, and you should test it on your
storage stack and workload before relying on it for important data.

It is good practice to verify the result of an `ecopy` run with `rsync` or another trusted comparison tool before
depending on the copied tree.

Use an empty or otherwise trusted target tree when possible. Existing regular files may be skipped or atomically replaced
based on the rules below. Existing target-side non-regular entries, including symlinks, devices, FIFOs, and sockets, are
rejected rather than followed or reconciled like they would be by a full synchronization tool. Missing target directories
are created with symlink-following disabled for each new path component.

So far, `ecopy` has only been tested on NFS v4.2 exports and local filesystems. Other filesystems, network
storage stacks, and mount options may have different behavior, especially around direct I/O, metadata preservation,
and `copy_file_range()`.

This is not `rsync` and it is not a full filesystem replication tool. It intentionally ignores non-regular payloads
such as symlinks, devices, FIFOs, and sockets. It currently does not preserve xattrs, ACLs, SELinux labels, or file
capabilities.

## Usage

```bash
./ecopy [-v|--verbose] <source_dir> <target_dir>
```

Use `-v` or `--verbose` to print the resolved startup configuration and the full diagnostic block in the final report.
The final report always prints one suggested next-run tuning experiment based on the same workload; verbose mode adds the full diagnostic counters behind that suggestion.
The live progress line reports payload bytes, payload throughput, and rolling files-per-second. Metadata-heavy or zero-byte-file workloads can show high files-per-second while payload bytes remain low.

The source argument must name an existing directory. The source and target directories must not overlap; `ecopy`
validates that relationship before creating a missing target root. After validation, it creates the target root when it
is missing, then creates the matching tree beneath it.

Examples (paths are placeholders; use your own source and target directories):

```bash
./ecopy /path/to/source_tree /path/to/destination_tree
```

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 ./ecopy /path/to/source_tree /path/to/destination_tree
```

```bash
DIRECT_COPY_DISABLE_READ_DIRECT_IO=1 DIRECT_COPY_DISABLE_WRITE_DIRECT_IO=0 ./ecopy /src /dst
```

```bash
DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_READERS=3 DIRECT_COPY_LARGE_WRITERS=1 ./ecopy /src /dst
```

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=128 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_READERS=3 DIRECT_COPY_LARGE_WRITERS=1 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 ./ecopy /src /dst
```

## Scheduler Model

The current design uses a shared slot budget.

- Small files consume `1` worker slot.
- Large files consume one active-file slot worth of reader/writer threads. By default that is `4` readers plus `2`
  writers, so the active large-file limit is `DIRECT_COPY_MAX_WORKERS / 6`.
- Each active large file gets a bounded large-file pipeline with preallocated aligned chunk buffers.
- Readers fill buffers, writers drain a ready queue, and the per-file in-flight depth is capped by `DIRECT_COPY_LARGE_FILE_INFLIGHT`.
- The total budget is capped by `DIRECT_COPY_MAX_WORKERS`.

This avoids the older problem where fixed small-file and large-file pools wasted capacity when one side of the workload
was idle.

Default behavior:

- `DIRECT_COPY_MAX_WORKERS = 256`
- `DIRECT_COPY_SMALL_MAX_WORKERS = 32`
- `DIRECT_COPY_LARGE_WORKERS = 6` when both reader/writer split knobs are set to `0`
- `DIRECT_COPY_LARGE_READERS = 4`
- `DIRECT_COPY_LARGE_WRITERS = 2`
- `DIRECT_COPY_LARGE_FILE_INFLIGHT = 16`
- `DIRECT_COPY_CHUNK_MB = 1`
- `DIRECT_COPY_LARGE_THRESHOLD_MB = 128`
- `DIRECT_COPY_TRAVERSAL_WORKERS = 8`
- `DIRECT_COPY_MAX_QUEUED_FILES = 262144`
- `DIRECT_COPY_SMALL_INPLACE = 0`
- `DIRECT_COPY_DISABLE_DIRECT_IO = 0`
- `DIRECT_COPY_DISABLE_READ_DIRECT_IO = 0`
- `DIRECT_COPY_DISABLE_WRITE_DIRECT_IO = 0`
- `DIRECT_COPY_DISABLE_COPY_FILE_RANGE = 0`
- effective large-file chunk-buffer budget capped at `8192 MiB`

So by default:

- all-small workloads can run up to `32` files in parallel by default
- all-large workloads can run up to about `42` large files in parallel by default when small-file work is absent
- each active large file uses `4` readers and `2` writers by default
- each active large file can keep up to `16` chunk buffers in flight by default
- mixed workloads use a shared total slot pool, but small-file work is capped separately so it cannot consume the entire machine by default
- traversal is backpressured once queued file work reaches the configured cap
- directory descriptors are opened lazily, only while a directory is actively being walked, so the number of open directory file descriptors is bounded by `DIRECT_COPY_TRAVERSAL_WORKERS` rather than by the number of pending directories; a single parent with millions of immediate children no longer exhausts the process file-descriptor table
- at startup the tool raises its open-file soft limit (`RLIMIT_NOFILE`) to the hard limit; run with `-v` to print the effective limit
- numeric environment values outside their accepted range are clamped with a warning; invalid values fall back to the documented default

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

1. The source file is opened relative to its source directory and verified against traversal metadata.
2. A hidden temporary target is created in the destination directory and its blocks are preallocated up front with `fallocate()`, falling back to `ftruncate()` sizing on filesystems that do not support it.
3. The large file is activated into its own bounded pipeline.
4. A pool of aligned chunk buffers is preallocated up front.
5. Reader threads use `pread()` to fill free buffers and push them into a ready-to-write queue.
6. Writer threads drain that ready queue with `pwrite()`.
7. `DIRECT_COPY_LARGE_FILE_INFLIGHT` limits how many chunk buffers each active large file may keep in flight at once.
8. Any remaining unaligned tail bytes are copied in a buffered tail pass.
9. Final metadata is restored on the temporary file before it is renamed into place.

This gives a real read-buffer-write pipeline for large files, with bounded queue depth and instrumentation that tells us whether readers are waiting for buffers or writers are waiting for data.

Preallocating the whole file with `fallocate()` keeps block allocation off the write path: each `pwrite()` then only converts an already-reserved extent instead of allocating blocks on the fly. On a filling, fragmenting filesystem this avoids the late-run throughput collapse that occurs when every direct write has to walk the free-space allocator.

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

- `uid`, when permitted
- `gid`, when permitted
- permission bits
- `atime`
- `mtime`

This applies to:

- copied regular files
- skipped regular files
- directories after a final metadata pass

Newly created target directories stay owner-writable during the copy so child entries can still be created under source trees that contain read-only directories; the final directory pass restores the true source mode afterward.
Copied regular files are written to hidden temporary files that stay owner-writable while data is being written, then restored to the source mode before the final rename. Existing read-only destination regular files do not need to be chmodded for overwrite; the containing directory must allow the final rename.
If `chown`/`fchown` is not permitted, `ecopy` prints one warning, counts each degraded uid/gid update in the final report, and continues without preserving uid/gid ownership for those entries. Fatal metadata update failures, such as mode or timestamp updates that cannot be applied, are counted as metadata errors before the run fails.

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
256
```

If a large-file reader/writer configuration would cost more slots than this budget, `ecopy` clamps the per-file split and warns so at least one large file can still make progress.
Values below `2` are clamped to `2` because a large-file pipeline needs at least one reader and one writer thread.
Values above `512` are clamped to `512`.

### `DIRECT_COPY_SMALL_MAX_WORKERS`

Maximum number of active small-file workers allowed at once. This cap works alongside `DIRECT_COPY_MAX_WORKERS` so large-file pipelines can still use the remaining slot budget.

Default:

```text
32
```

Example: with the defaults, small-file work can use up to `32` slots while large-file work can still fan out over the remaining capacity.
Values below `1` are clamped to `1`; values above `DIRECT_COPY_MAX_WORKERS` are clamped to the resolved max-worker value.

### `DIRECT_COPY_SMALL_INPLACE`

When unset or `0` (the default), small files are copied to a hidden temporary file that is renamed into place once the copy and metadata are complete. This makes each small-file copy crash-atomic: a reader never sees a partial file, and an interrupted run never leaves a truncated destination.

When set to any non-empty value other than `0`, small files are written **directly to the final destination name** instead. This removes the per-file `rename()` (and its directory-rename lock), roughly halving the directory inode-lock operations per file.

Use this when copying a very large number of small files into one (or few) destination directories, where the directory inode lock and rename serialization dominate — exactly the contention shown by `perf` as `lock_rename` / `rwsem`/`osq_lock` time. On a quota-enabled or network filesystem the win can be significant; on a fast local filesystem it is modest.

Trade-offs when enabled:

- Not crash-atomic: an interrupted or failed copy can leave a partially written or truncated destination file. On a clean per-file error `ecopy` removes the partial file, but a hard crash or kill can leave one behind.
- An existing read-only destination is made owner-writable to be truncated, then restored to the source mode at the end; if the run is interrupted the mode may not be restored.
- Symlink, FIFO, and other non-regular destinations are still rejected (the final open uses `O_NOFOLLOW` and destinations are type-checked first), so this flag does not weaken those protections.

Example:

```bash
DIRECT_COPY_SMALL_INPLACE=1 ./ecopy /src /dst
```

This only affects the small-file path; large files always use the temporary-file-plus-rename pipeline.

### `DIRECT_COPY_LARGE_WORKERS`

Fallback total thread count per active large file when both `DIRECT_COPY_LARGE_READERS` and `DIRECT_COPY_LARGE_WRITERS`
are set to `0`. In the normal/default configuration, the reader/writer settings define the total directly.

Default:

```text
6
```

Values below `2` are clamped to `2`; values above `DIRECT_COPY_MAX_WORKERS` are clamped to the resolved max-worker value.

### `DIRECT_COPY_LARGE_FILE_INFLIGHT`

Maximum number of chunk tasks that each active large file may keep queued or in flight at once.

Default:

```text
16
```

This is the closest runtime knob to per-file queue depth for large-file NFS/RDMA tuning. Higher values can create more in-flight I/O, but can also increase contention.
Values below `1` are clamped to `1`; values above `1024` are clamped to `1024`.
`ecopy` also clamps the effective in-flight depth when active large files, chunk size, and in-flight buffers would exceed the built-in `8192 MiB` large-buffer budget.

In the final runtime summary, `large file readers/file` and `large file writers/file` show the actual split used for each active large-file pipeline.

### `DIRECT_COPY_LARGE_READERS`

Large-file reader threads per active large file.

Default:

```text
4
```

Values below `0` are clamped to `0`; values above `DIRECT_COPY_MAX_WORKERS` are clamped to the resolved max-worker value.

### `DIRECT_COPY_LARGE_WRITERS`

Large-file writer threads per active large file.

Default:

```text
2
```

Values below `0` are clamped to `0`; values above `DIRECT_COPY_MAX_WORKERS` are clamped to the resolved max-worker value.

Their sum becomes the effective per-file large-worker total and is used to compute the active-large-file limit. For
example, `DIRECT_COPY_LARGE_READERS=3 DIRECT_COPY_LARGE_WRITERS=1` uses `4` worker slots per active large file.

### `DIRECT_COPY_CHUNK_MB`

Chunk size in MiB used for aligned bulk transfer.

Default:

```text
1
```

Larger values may help high-bandwidth sequential workloads, but the best setting is environment-dependent.
Values below `1` are clamped to `1`; values above `4096` are clamped to `4096`.

### `DIRECT_COPY_LARGE_THRESHOLD_MB`

File-size threshold in MiB used to decide when a file enters the large-file pipeline. This is intentionally separate from `DIRECT_COPY_CHUNK_MB`.

Default:

```text
128
```

Use this when you want to keep medium-sized files on the simpler small-file path while still experimenting with small chunk sizes for true large-file transfers.
Values below `1` are clamped to `1`; values above `1048576` are clamped to `1048576`.

### `DIRECT_COPY_TRAVERSAL_WORKERS`

Number of parallel directory-walker threads used to discover directories and files.

Default:

```text
8
```

Higher values may reduce tree-discovery time on large namespace-heavy workloads, but can also increase metadata-server or filesystem contention. This knob is most relevant for trees with many small files and directories.
Values below `1` are clamped to `1`; values above `128` are clamped to `128`.

### `DIRECT_COPY_MAX_QUEUED_FILES`

Maximum number of queued regular-file tasks across the small-file and large-file queues before traversal blocks and waits for workers to drain backlog.

Default:

```text
262144
```

This limits memory growth on very large trees while still allowing traversal to stay comfortably ahead of the copy workers.
Values below `1` are clamped to `1`; values above `10000000` are clamped to `10000000`.

### `DIRECT_COPY_DISABLE_COPY_FILE_RANGE`

When unset or set to `0`, the tool may use `copy_file_range()` on buffered copy paths when the kernel and filesystem support it.

When set to any non-empty value other than `0`, the tool skips `copy_file_range()` entirely and uses the normal buffered read/write path.

Example:

```bash
DIRECT_COPY_DISABLE_COPY_FILE_RANGE=1 DIRECT_COPY_DISABLE_DIRECT_IO=1 ./ecopy /src /dst
```

### `DIRECT_COPY_DISABLE_DIRECT_IO`

Legacy global switch. When unset or set to `0`, the tool allows direct I/O on both reads and writes.

When set to any non-empty value other than `0`, the tool disables direct I/O on both sides and uses buffered I/O paths instead.

Example:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 ./ecopy /src /dst
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
DIRECT_COPY_DISABLE_READ_DIRECT_IO=1 DIRECT_COPY_DISABLE_WRITE_DIRECT_IO=0 ./ecopy /src /dst
```

### `DIRECT_COPY_DISABLE_WRITE_DIRECT_IO`

When set to any non-empty value other than `0`, the tool disables direct I/O for destination writes while leaving the read side unchanged.

The per-side switches override the old all-or-nothing behavior and make mixed mode possible, such as buffered reads from NFS and direct writes to local flash.

## Performance Tuning

For NFS/RDMA or other high-throughput flash-backed paths, the best settings are workload- and environment-dependent.
The current built-in defaults are already tuned toward a high-concurrency large-file profile:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=0 DIRECT_COPY_DISABLE_READ_DIRECT_IO=0 DIRECT_COPY_DISABLE_WRITE_DIRECT_IO=0 DIRECT_COPY_DISABLE_COPY_FILE_RANGE=0 DIRECT_COPY_MAX_WORKERS=256 DIRECT_COPY_SMALL_MAX_WORKERS=32 DIRECT_COPY_LARGE_WORKERS=6 DIRECT_COPY_LARGE_READERS=4 DIRECT_COPY_LARGE_WRITERS=2 DIRECT_COPY_LARGE_FILE_INFLIGHT=16 DIRECT_COPY_CHUNK_MB=1 DIRECT_COPY_LARGE_THRESHOLD_MB=128 DIRECT_COPY_TRAVERSAL_WORKERS=8 DIRECT_COPY_MAX_QUEUED_FILES=262144 ./ecopy /src /dst
```

For small-file or metadata-heavy trees, try buffered I/O first. A practical starting point is:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_MAX_WORKERS=16 ./ecopy /src /dst
```

That profile is often better for trees with many tiny files because it reduces direct-I/O alignment overhead and avoids overdriving metadata operations with too many workers.

A useful benchmark matrix is:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=64  DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_READERS=3 DIRECT_COPY_LARGE_WRITERS=1 DIRECT_COPY_LARGE_FILE_INFLIGHT=8 ./ecopy /src /dst
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=128 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_READERS=3 DIRECT_COPY_LARGE_WRITERS=1 DIRECT_COPY_LARGE_FILE_INFLIGHT=4 ./ecopy /src /dst
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_CHUNK_MB=256 DIRECT_COPY_MAX_WORKERS=64 DIRECT_COPY_LARGE_READERS=3 DIRECT_COPY_LARGE_WRITERS=1 DIRECT_COPY_LARGE_FILE_INFLIGHT=2 ./ecopy /src /dst
```

Things to watch while tuning:

- live payload throughput, rolling files-per-second, and final copied/skipped byte totals reported by `ecopy`
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
tree is materialized gives correct final metadata.

The final directory pass is parallelized by directory depth, so sibling directories can be finalized concurrently while
still preserving child-before-parent ordering.

## Build

```bash
make
```

This builds the regular `ecopy` binary. If jemalloc is available, the same build also produces `ecopy-jemalloc`.
The Makefile first checks `pkg-config --exists jemalloc`; if that is not available, it falls back to compiling and
linking a small `<jemalloc/jemalloc.h>` test with `-ljemalloc`. Missing jemalloc support does not fail the normal
`ecopy` build.

The optional `ecopy-jemalloc` binary accepts the same command-line arguments and runtime environment variables as
`ecopy`; it only differs by linking against jemalloc.

Warning-clean verification build:

```bash
make clean
make CFLAGS='-O2 -g -Wall -Wextra -Wpedantic -pthread'
```

Run the portable smoke tests:

```bash
make test
```

## License

This project is licensed under the **MIT License**. See **[LICENSE](LICENSE)**. Copyright is held by **Michel Erb** (2026).
