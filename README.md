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

## Scheduler Model

The current design uses a shared slot budget.

- Small files consume `1` worker slot.
- Large files consume `DIRECT_COPY_LARGE_WORKERS` slots because they fan out into that many chunk workers.
- The total budget is capped by `DIRECT_COPY_MAX_WORKERS`.

This avoids the older problem where fixed small-file and large-file pools wasted capacity when one side of the workload
was idle.

Default behavior:

- `DIRECT_COPY_MAX_WORKERS = 16`
- `DIRECT_COPY_LARGE_WORKERS = 4`

So by default:

- all-small workloads can run up to `16` files in parallel
- all-large workloads can run up to `4` large files in parallel, each with `4` chunk workers

## Copy Strategy

### Small Files

Smaller files are copied with a straightforward read/write loop.

### Large Files

A file is treated as large when its size exceeds:

```text
10 * CHUNK_SIZE
```

With the current defaults, that is `640 MiB`.

Large-file copy works like this:

1. The target file is created and pre-sized.
2. The aligned bulk region is split into contiguous ranges.
3. Those ranges are copied in parallel by chunk workers.
4. Any remaining unaligned tail bytes are copied in a buffered tail pass.
5. Final metadata is restored after data copy completes.

This gives wide sequential I/O on large files without forcing all workloads into a chunk scheduler.

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

Chunk workers consumed by each active large file.

Default:

```text
4
```

### `DIRECT_COPY_CHUNK_MB`

Chunk size in MiB used for aligned bulk transfer.

Default:

```text
64
```

### `DIRECT_COPY_DISABLE_DIRECT_IO`

When unset or set to `0`, the tool tries direct I/O first.

When set to any non-empty value other than `0`, the tool disables direct I/O and uses buffered I/O paths instead.

Example:

```bash
DIRECT_COPY_DISABLE_DIRECT_IO=1 /tmp/direct_copy /src /dst
```

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
