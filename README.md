# ecopy

Fast parallel copy for big file trees — local disk, NFS, or over SSH.
Think `cp -a` that uses all your cores, skips files you already copied, and can prove the copy is correct.

## Install

```bash
make
sudo cp ecopy /usr/local/bin/     # optional
```

Linux is the main platform. macOS works too (Apple Silicon included).

## Use

```bash
ecopy /src/tree /dst/tree                      # copy a directory
ecopy /src/file.bin /dst/                      # copy one file
ecopy /src/tree ssh://user@host/dst/tree       # copy to another machine
ecopy --verify /src/tree /dst/tree             # copy, then check the result
ecopy --verify-only /src/tree /dst/tree        # only check an existing copy
```

Run it again any time: files that already match (same size and mtime) are skipped.

What gets copied: files, directories, symlinks, hard links, sparse files (holes stay holes), permissions,
timestamps, and owner/group when you have permission to set them. Not copied: xattrs, ACLs, devices, FIFOs.

## Options

| Flag | Meaning |
| --- | --- |
| `--verify` | Check metadata and a 1% random sample of the data after copying |
| `--verify-data=50` | Check 50% of the data instead (100 = every block) |
| `--verify-only` | Check only. Never writes to the target |
| `--verify-skipped` | Also check files that were skipped as already copied |
| `--uid N` / `--gid N` | Give everything on the target this owner / group |
| `--no-preserve-times` | Don't copy timestamps (slightly faster over NFS) |
| `-v` | Show more detail while running and in the final report |

`ecopy --help` lists everything.

## Over SSH

```bash
ecopy /src/tree ssh://user@host:22/dst/tree
```

- Works like push-only `scp`: source is local, target is remote.
- If `ecopy` isn't installed on the remote, it copies itself over for the run. Installing it there is faster.
- Opens 4 connections but authenticates once, so an MFA/Duo prompt appears one time.
- Set `ECOPY_SSH="ssh -i ~/.ssh/key"` to change how ssh is invoked.

## Verifying

`--verify` compares type, size, permissions, owner, timestamps, and a random sample of the data. Sparse files
are sampled over their real data, not the holes, so a mostly-empty disk image is cheap to check.

If you're not root, some files can't be given their original owner. That's reported as `Ownership not
preserved` and is not a failure. Real mismatches are.

Verification runs alongside the copy, so it adds little wall time. `Total Elapsed` in the report is the number
that matters.

## Making it faster

The defaults are tuned for large files. The two settings worth trying:

```bash
# Lots of small files? Buffered I/O is usually faster.
DIRECT_COPY_DISABLE_DIRECT_IO=1 ecopy /src /dst

# Slow ssh link? More connections (still one login).
DIRECT_COPY_SSH_CONNECTIONS=8 ecopy /src ssh://host/dst
```

Other common knobs (set as environment variables):

| Variable | Default | What it does |
| --- | --- | --- |
| `DIRECT_COPY_MAX_WORKERS` | 256 | Total worker threads |
| `DIRECT_COPY_SMALL_MAX_WORKERS` | 32 | Small files copied at once |
| `DIRECT_COPY_LARGE_WORKERS` | 6 | Large files copied at once |
| `DIRECT_COPY_LARGE_THRESHOLD_MB` | 10 | What counts as "large" |
| `DIRECT_COPY_TRAVERSAL_WORKERS` | 8 | Threads scanning directories |
| `DIRECT_COPY_VERIFY_WORKERS` | cpus (max 16) | Threads checking files |
| `DIRECT_COPY_SSH_CONNECTIONS` | 4 | Parallel ssh sessions |
| `DIRECT_COPY_SSH_MULTIPLEX` | 1 | `0` = separate login per session (more prompts, separate TCP paths) |
| `DIRECT_COPY_SSH_SERVER_THREADS` | 16 | Worker threads on the remote side |
| `ECOPY_REMOTE_CMD` | `ecopy` | Path to `ecopy` on the remote |

## Safety

Test on your own storage before trusting it with anything important. Into an existing target tree, every file is
written to a temp name and renamed into place, so an interrupted run leaves no half-written files. Into a *brand
new* target directory, small files are written directly for speed — if that run is interrupted, delete the
target and start over.

Existing files on the target may be replaced. Nothing is ever deleted.

## edelete

This repo also builds `edelete`, a parallel tree deleter (moved here from the ereport repo). Dry-run by
default; nothing is removed unless you pass `--delete`.

```bash
make edelete

./edelete /scratch/staging                          # dry run: prints would_delete=
./edelete mtime 90 /scratch/job123                  # dry run, only entries older than 90 days
./edelete --delete mtime 90 /scratch/job123         # asks you to type YES
./edelete --delete --force ctime 14 /cache/tmp      # no prompt (scripting)
./edelete --uid 1234 --delete /scratch/shared       # only that owner's entries
```

Symlinks are never followed, and deletion never ascends above the start path. Thread count:
`EDELETE_THREADS` (default 16); `EDELETE_MAX_UNLINK_INFLIGHT` caps concurrent `unlink` calls
(default 256, `0` = unlimited); `EDELETE_FANOUT_MIN_BYTES` (default 64 MiB, `0` = off) queues
files at least that large for parallel unlink via the work queue instead of inline.

Quota'd XFS: every `unlink` of one owner's files serializes on that owner's dquot mutex, so more
threads make it *slower* (kernel time in `osq_lock` / `mutex_spin_on_owner`). Cap
`EDELETE_MAX_UNLINK_INFLIGHT` (2–4 is often best when deleting one user's tree); traversal
parallelism (`EDELETE_THREADS`) can stay high. Find the knee with:

```bash
for n in 1 2 4 8 16; do
  EDELETE_THREADS=$n EDELETE_MAX_UNLINK_INFLIGHT=$n ./edelete --delete --force <path> 2>/dev/null \
  | awk -F= -v n=$n '/^deleted_files=/{d=$2} /^elapsed_sec=/{e=$2} END{printf "inflight=%s rate=%.0f/s\n", n, e>0?d/e:0}'
done
```

## Testing

```bash
make test
```

## License

MIT — see [LICENSE](LICENSE). Copyright Michel Erb (2026).
