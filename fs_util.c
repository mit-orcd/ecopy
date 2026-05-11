/*
 * fs_util.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "fs_util.h"
#include "stats.h"
#include "progress.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

static int direct_io_fallback_errno(int err) {
    return err == EINVAL || err == EOPNOTSUPP || err == ENOTSUP || err == ENOSYS;
}

static int chown_permission_errno(int err) {
    return err == EPERM || err == EACCES;
}

int copy_file_range_enabled(void) {
    static int g_copy_file_range_enabled = -1;

    if (g_copy_file_range_enabled >= 0) {
        return g_copy_file_range_enabled;
    }

    const char *env = getenv("DIRECT_COPY_DISABLE_COPY_FILE_RANGE");
    if (!env || !*env || strcmp(env, "0") == 0) {
        g_copy_file_range_enabled = 1;
    } else {
        g_copy_file_range_enabled = 0;
    }

    return g_copy_file_range_enabled;
}

static int env_disable_flag(const char *name)
{
    const char *env = getenv(name);
    return env && *env && strcmp(env, "0") != 0;
}

int direct_io_enabled(void) {
    return read_direct_io_enabled() && write_direct_io_enabled();
}

int read_direct_io_enabled(void)
{
    static int g_read_direct_io_enabled = -1;

    if (g_read_direct_io_enabled >= 0) {
        return g_read_direct_io_enabled;
    }

    if (env_disable_flag("DIRECT_COPY_DISABLE_READ_DIRECT_IO") ||
        env_disable_flag("DIRECT_COPY_DISABLE_DIRECT_IO")) {
        g_read_direct_io_enabled = 0;
    } else {
        g_read_direct_io_enabled = 1;
    }

    return g_read_direct_io_enabled;
}

int write_direct_io_enabled(void)
{
    static int g_write_direct_io_enabled = -1;

    if (g_write_direct_io_enabled >= 0) {
        return g_write_direct_io_enabled;
    }

    if (env_disable_flag("DIRECT_COPY_DISABLE_WRITE_DIRECT_IO") ||
        env_disable_flag("DIRECT_COPY_DISABLE_DIRECT_IO")) {
        g_write_direct_io_enabled = 0;
    } else {
        g_write_direct_io_enabled = 1;
    }

    return g_write_direct_io_enabled;
}

int open_read_maybe_direct(const char *path, int *used_direct) {
    int fd;
    if (used_direct) *used_direct = 0;

    if (read_direct_io_enabled()) {
        fd = open(path, O_RDONLY | O_DIRECT);
        if (fd >= 0) {
            if (used_direct) *used_direct = 1;
            stats_record_read_open(1);
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            progress_interrupt();
        perror(path);
            return -1;
        }
    }

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        progress_interrupt();
        perror(path);
        return -1;
    }
    stats_record_read_open(0);
    return fd;
}

int open_write_maybe_direct(const char *path, mode_t mode, int *used_direct) {
    int fd;
    if (used_direct) *used_direct = 0;

    if (write_direct_io_enabled()) {
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, mode);
        if (fd >= 0) {
            if (used_direct) *used_direct = 1;
            stats_record_write_open(1);
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            progress_interrupt();
        perror(path);
            return -1;
        }
    }

    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
        progress_interrupt();
        perror(path);
        return -1;
    }
    stats_record_write_open(0);
    return fd;
}

int ensure_dir_exists(const char *path, mode_t mode) {
    struct stat st;
    mode_t create_mode;

    if (lstat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            progress_interrupt();
            fprintf(stderr, "Target exists but is not a directory: %s\n", path);
            return -1;
        }
        return 0;
    }

    /* Keep newly created directories writable/searchable until final metadata restore. */
    create_mode = (mode & 07777) | S_IRUSR | S_IWUSR | S_IXUSR;
    if (mkdir(path, create_mode) != 0) {
        progress_interrupt();
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
        progress_interrupt();
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

int preserve_fd_metadata(int fd, const char *path_for_warning, const struct stat *src_st) {
    static int warned_chown_permission = 0;
    struct timespec ts[2];

    if (fchown(fd, src_st->st_uid, src_st->st_gid) != 0) {
        if (chown_permission_errno(errno)) {
            if (!warned_chown_permission) {
                progress_interrupt();
                fprintf(stderr,
                        "Warning: chown not permitted; continuing without preserving uid/gid ownership.\n");
                warned_chown_permission = 1;
            }
        } else {
            if (path_for_warning && *path_for_warning) {
                fprintf(stderr, "%s: ", path_for_warning);
            }
            progress_interrupt();
            perror("fchown");
            return -1;
        }
    }

    if (fchmod(fd, src_st->st_mode & 07777) != 0) {
        if (path_for_warning && *path_for_warning) {
            fprintf(stderr, "%s: ", path_for_warning);
        }
        progress_interrupt();
        perror("fchmod");
        return -1;
    }

    ts[0] = src_st->st_atim;
    ts[1] = src_st->st_mtim;
    if (futimens(fd, ts) != 0) {
        if (path_for_warning && *path_for_warning) {
            fprintf(stderr, "%s: ", path_for_warning);
        }
        progress_interrupt();
        perror("futimens");
        return -1;
    }

    return 0;
}

int preserve_path_metadata(const char *dst, const struct stat *src_st) {
    int fd = open(dst, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        progress_interrupt();
        perror(dst);
        return -1;
    }

    int rc = preserve_fd_metadata(fd, dst, src_st);
    close(fd);
    return rc;
}

int finalize_copied_file(const char *dst, const struct stat *src_st) {
    return preserve_path_metadata(dst, src_st);
}

int finalize_copied_file_fd(int fd, const char *path_for_warning, const struct stat *src_st) {
    return preserve_fd_metadata(fd, path_for_warning, src_st);
}
