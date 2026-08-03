/*
 * compat.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Platform shims. ecopy is developed against Linux and its fast paths are
 * Linux syscalls; everything here exists so the same sources also build and
 * run on macOS. Every shim is inside #ifdef __APPLE__, and the handful of
 * helpers that exist on both platforms compile to nothing on Linux, so the
 * Linux build is byte-for-byte what it was before this header existed.
 *
 * Include this after any _GNU_SOURCE define and before the first use of the
 * names it provides.
 */

#ifndef ECOPY_COMPAT_H
#define ECOPY_COMPAT_H

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 * Raw getdents64(2) lets a directory scan pull thousands of entries per
 * syscall. Darwin exposes no stable equivalent (getdirentries(2) is deprecated
 * and 32-bit-inode only), so traversal there uses the libc readdir() reader
 * that already exists as the fallback path.
 */
#ifdef __linux__
#define ECOPY_HAVE_GETDENTS64 1
#endif

#ifdef __APPLE__

/*
 * struct stat spells the nanosecond timestamps st_?timespec on Darwin. The
 * fields are otherwise identical (struct timespec), so a rename is enough.
 */
#define st_atim st_atimespec
#define st_mtim st_mtimespec
#define st_ctim st_ctimespec

/*
 * Darwin has no O_DIRECT: uncached I/O is a property of the open file
 * description, set after the fact with fcntl(F_NOCACHE). Defining the flag to
 * 0 keeps the open() call sites identical on both platforms; they then call
 * ecopy_set_direct_io() below, which is where Darwin actually turns caching
 * off. Unlike Linux, an unaligned read/write on such an fd still works (the
 * kernel bounces it), so the aligned-buffer machinery is an optimization here
 * rather than a correctness requirement.
 */
#define O_DIRECT 0

/*
 * posix_fadvise() is absent. The two hints ecopy relies on map onto Darwin's
 * per-fd read-ahead switch; there is no equivalent of range-scoped page cache
 * eviction, so DONTNEED becomes a no-op (a missed optimization, never a
 * correctness problem). Returns an errno value like the POSIX function, not -1.
 */
#define POSIX_FADV_NORMAL     0
#define POSIX_FADV_RANDOM     1
#define POSIX_FADV_SEQUENTIAL 2
#define POSIX_FADV_WILLNEED   3
#define POSIX_FADV_DONTNEED   4
#define POSIX_FADV_NOREUSE    5

static inline int posix_fadvise(int fd, off_t offset, off_t len, int advice)
{
    (void)offset;
    (void)len;
    switch (advice) {
    case POSIX_FADV_SEQUENTIAL:
    case POSIX_FADV_WILLNEED:
        return fcntl(fd, F_RDAHEAD, 1) == -1 ? errno : 0;
    case POSIX_FADV_RANDOM:
        return fcntl(fd, F_RDAHEAD, 0) == -1 ? errno : 0;
    default:
        return 0;
    }
}

/*
 * fallocate(mode 0) reserves blocks *and* grows the file; F_PREALLOCATE only
 * reserves, so the size still has to come from ftruncate(). Allocation is
 * requested relative to the physical end of file, hence the fstat() to ask for
 * just the missing tail. Contiguous allocation is tried first because that is
 * the point of preallocating; F_ALLOCATEALL accepts a fragmented extent list.
 * Only the mode/offset combination ecopy uses is implemented.
 */
static inline int fallocate(int fd, int mode, off_t offset, off_t len)
{
    struct stat sb;
    fstore_t fst;

    if (mode != 0 || offset != 0 || len < 0) {
        errno = EOPNOTSUPP;
        return -1;
    }
    if (fstat(fd, &sb) != 0) {
        return -1;
    }
    if (sb.st_size < len) {
        fst.fst_flags = F_ALLOCATECONTIG;
        fst.fst_posmode = F_PEOFPOSMODE;
        fst.fst_offset = 0;
        fst.fst_length = len - sb.st_size;
        fst.fst_bytesalloc = 0;
        if (fcntl(fd, F_PREALLOCATE, &fst) == -1) {
            fst.fst_flags = F_ALLOCATEALL;
            fst.fst_bytesalloc = 0;
            if (fcntl(fd, F_PREALLOCATE, &fst) == -1) {
                return -1;
            }
        }
    }
    return ftruncate(fd, len);
}

/*
 * No in-kernel ranged file-to-file copy exists on Darwin (copyfile(3) with
 * COPYFILE_CLONE is whole-file only and cannot express the chunked, resumable
 * ranges this code issues). copy_file_range_enabled() reports 0 on Darwin so
 * the pread/pwrite path is taken and this never runs; it exists to keep the
 * call site compiling.
 */
static inline ssize_t copy_file_range(int fd_in, off_t *off_in,
                                      int fd_out, off_t *off_out,
                                      size_t len, unsigned int flags)
{
    (void)fd_in; (void)off_in; (void)fd_out; (void)off_out;
    (void)len; (void)flags;
    errno = ENOSYS;
    return -1;
}

#endif /* __APPLE__ */

/*
 * Whether a file's contents can be read without moving its access time, which
 * is what makes atime verifiable: verification hashes both copies, so without
 * such a read it would invalidate the very timestamp it then compares.
 *
 * Linux has O_NOATIME. Darwin has no equivalent — O_EVTONLY looks like one but
 * does not suppress the update — so atime is still *copied* there, just not
 * *verified*; every other metadata field is checked as usual.
 */
#ifdef O_NOATIME
#define ECOPY_VERIFY_ATIME 1
#else
#define ECOPY_VERIFY_ATIME 0
#endif

/*
 * Enable/disable uncached I/O on an already-open fd. On Linux this is settled
 * at open() time by O_DIRECT and both helpers fold away to nothing; on Darwin
 * they are the fcntl(F_NOCACHE) that O_DIRECT stands in for. Returns 0 on
 * success, -1 (errno set) if the fd cannot be made uncached, in which case the
 * caller keeps using it as an ordinary buffered fd.
 */
static inline int ecopy_set_direct_io(int fd)
{
#ifdef __APPLE__
    return fcntl(fd, F_NOCACHE, 1);
#else
    (void)fd;
    return 0;
#endif
}

static inline int ecopy_clear_direct_io(int fd)
{
#ifdef __APPLE__
    return fcntl(fd, F_NOCACHE, 0);
#else
    (void)fd;
    return 0;
#endif
}

#endif /* ECOPY_COMPAT_H */
