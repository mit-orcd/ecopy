# ecopy

`ecopy` is a parallel directory copy tool for moving large regular-file datasets quickly over high-throughput
storage paths (tested on NFS v4.2 exports and local filesystems). It keeps source, network, and target busy at the
same time using a shared pool of read/write/traversal workers.

## Quick Start

```bash
make
./ecopy <source_dir> <target_dir>
```

- `source_dir` must be an existing directory; source and target must not overlap.
- The target root and the matching tree beneath it are created as needed.
- Add `-v`/`--verbose` to print the resolved config and full diagnostic counters.

Common variations:

```bash
# Many small files / metadata-heavy trees: buffered I/O is often faster
DIRECT_COPY_DISABLE_DIRECT_IO=1 DIRECT_COPY_MAX_WORKERS=16 ./ecopy /src /dst

# Large aligned streaming copies (defaults are already tuned for this)
./ecopy /src /dst

# Mixed mode: buffered reads, direct writes
DIRECT_COPY_DISABLE_READ_DIRECT_IO=1 ./ecopy /src /dst
```

## What It Does

- Walks the source tree and recreates it on the target. Copies regular files only.
- Skips unchanged files based on `size + mtime`.
- Detects sparse files and copies only their data, preserving holes instead of moving zeros.
- Copies through same-directory temp files and renames into place after data + metadata are complete (crash-atomic).
- Opens entries relative to open directory handles, so symlink swaps are rejected during traversal and copy.
- Preserves `uid`/`gid` (when permitted), permission bits, `atime`, and `mtime`.

Not preserved: xattrs, ACLs, SELinux labels, file capabilities. Non-regular entries (symlinks, devices, FIFOs,
sockets) are ignored on the source and rejected on the target rather than reconciled.

## Safety

Use at your own risk and test on your storage stack first. This is not `rsync` and not a full replication tool;
verifying a run with `rsync` or another trusted tool before relying on the copy is good practice. Prefer an empty or
trusted target tree: existing regular files may be skipped or atomically replaced, and existing non-regular target
entries are rejected.

## How It Copies

- **Small files** use a simple copy path. With buffered I/O they can also use streaming `posix_fadvise()` hints and
  opportunistic `copy_file_range()`.
- **Large files** (size > `DIRECT_COPY_LARGE_THRESHOLD_MB`, default 128) run a bounded reader→queue→writer pipeline
  with preallocated aligned chunk buffers. The destination is preallocated up front with `fallocate()` (falling back
  to `ftruncate()`), which keeps block allocation off the write path and avoids late-run throughput collapse.
- **Sparse files** are routed to a hole-skipping path regardless of logical size: data regions are found with
  `lseek(SEEK_DATA/SEEK_HOLE)`, only data is copied, and the destination is `ftruncate()`d to the exact source size.

## Tuning

Best settings are workload- and environment-dependent. Key knobs (defaults in parentheses):

| Variable | Default | Purpose |
| --- | --- | --- |
| `DIRECT_COPY_MAX_WORKERS` | 256 | Total shared worker-slot budget (clamped 2–512) |
| `DIRECT_COPY_SMALL_MAX_WORKERS` | 32 | Cap on concurrent small-file workers |
| `DIRECT_COPY_LARGE_READERS` / `DIRECT_COPY_LARGE_WRITERS` | 4 / 2 | Threads per active large file |
| `DIRECT_COPY_LARGE_FILE_INFLIGHT` | 16 | Chunk buffers in flight per large file |
| `DIRECT_COPY_CHUNK_MB` | 1 | Aligned bulk-transfer chunk size (1–4096) |
| `DIRECT_COPY_LARGE_THRESHOLD_MB` | 128 | Size at which a file enters the large-file pipeline |
| `DIRECT_COPY_TRAVERSAL_WORKERS` | 8 | Parallel directory-walker threads |
| `DIRECT_COPY_MAX_QUEUED_FILES` | 262144 | Backpressure cap on queued file tasks |
| `DIRECT_COPY_SMALL_INPLACE` | 0 | Write small files directly to the final name (faster, not crash-atomic) |
| `DIRECT_COPY_DISABLE_DIRECT_IO` | 0 | Disable `O_DIRECT` on both sides (buffered I/O) |
| `DIRECT_COPY_DISABLE_READ_DIRECT_IO` / `DIRECT_COPY_DISABLE_WRITE_DIRECT_IO` | 0 / 0 | Per-side direct-I/O switches |
| `DIRECT_COPY_DISABLE_COPY_FILE_RANGE` | 0 | Skip `copy_file_range()` on buffered paths |

Out-of-range numeric values are clamped with a warning. The final report prints one suggested next-run experiment;
`-v` adds the full diagnostic counters (per-phase seconds, read/write opens, queue/buffer waits, etc.) to help you
see whether time is going to traversal, copy, metadata, or teardown.

## Build & Test

```bash
make          # builds ecopy (and ecopy-jemalloc if jemalloc is available)
make test     # runs the smoke tests and tests/ecopy_harness.sh
```

`tests/ecopy_harness.sh` builds varied source trees (small/large/sparse/pure-hole files, nested dirs, symlinks,
read-only files) and verifies ecopy reproduces them across several runtime profiles. Run it directly with
`tests/ecopy_harness.sh` (or `ECOPY_HARNESS_VERBOSE=1 tests/ecopy_harness.sh`).

Warning-clean verification build:

```bash
make clean && make CFLAGS='-O2 -g -Wall -Wextra -Wpedantic -pthread'
```

## License

MIT License. See [LICENSE](LICENSE). Copyright held by **Michel Erb** (2026).
