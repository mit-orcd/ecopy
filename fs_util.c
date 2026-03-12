#define _GNU_SOURCE
#include "fs_util.h"
#include "stats.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

static int direct_io_fallback_errno(int err) {
    return err == EINVAL || err == EOPNOTSUPP || err == ENOTSUP || err == ENOSYS;
}

int open_read_maybe_direct(const char *path, int *used_direct) {
    int fd;
    if (used_direct) *used_direct = 0;
    fd = open(path, O_RDONLY | O_DIRECT);
    if (fd >= 0) {
        if (used_direct) *used_direct = 1;
        return fd;
    }
    if (!direct_io_fallback_errno(errno)) {
        perror(path);
        return -1;
    }
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        perror(path);
        return -1;
    }
    return fd;
}

int open_write_maybe_direct(const char *path, mode_t mode, int *used_direct) {
    int fd;
    if (used_direct) *used_direct = 0;
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, mode);
    if (fd >= 0) {
        if (used_direct) *used_direct = 1;
        return fd;
    }
    if (!direct_io_fallback_errno(errno)) {
        perror(path);
        return -1;
    }
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
        perror(path);
        return -1;
    }
    return fd;
}

int ensure_dir_exists(const char *path, mode_t mode) {
    struct stat st;
    if (lstat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Target exists but is not a directory: %s\n", path);
            return -1;
        }
        return 0;
    }
    if (mkdir(path, mode) != 0) {
        perror(path);
        return -1;
    }
    stats_inc_dirs_created();
    return 0;
}

int set_file_times(const char *dst, const struct stat *src_st) {
    struct timespec ts[2];
    ts[0] = src_st->st_atim;
    ts[1] = src_st->st_mtim;
    if (utimensat(AT_FDCWD, dst, ts, 0) != 0) {
        perror("utimensat");
        return -1;
    }
    return 0;
}

int same_size_and_mtime(const struct stat *a, const struct stat *b) {
    return a->st_size == b->st_size &&
           a->st_mtim.tv_sec == b->st_mtim.tv_sec &&
           a->st_mtim.tv_nsec == b->st_mtim.tv_nsec;
}

int finalize_copied_file(const char *dst, const struct stat *src_st) {
    if (truncate(dst, src_st->st_size) != 0) { perror("truncate"); return -1; }
    if (chmod(dst, src_st->st_mode & 07777) != 0) { perror("chmod"); return -1; }
    return set_file_times(dst, src_st);
}
