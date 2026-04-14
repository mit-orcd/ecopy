#define _GNU_SOURCE
#include "fs_util.h"
#include "stats.h"

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
            perror(path);
            return -1;
        }
    }

    fd = open(path, O_RDONLY);
    if (fd < 0) {
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
            perror(path);
            return -1;
        }
    }

    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
        perror(path);
        return -1;
    }
    stats_record_write_open(0);
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

int preserve_path_metadata(const char *dst, const struct stat *src_st) {
    static int warned_chown_permission = 0;

    if (chown(dst, src_st->st_uid, src_st->st_gid) != 0) {
        if (chown_permission_errno(errno)) {
            if (!warned_chown_permission) {
                fprintf(stderr,
                        "Warning: chown not permitted; continuing without preserving uid/gid ownership.\n");
                warned_chown_permission = 1;
            }
        } else {
            perror("chown");
            return -1;
        }
    }
    if (chmod(dst, src_st->st_mode & 07777) != 0) {
        perror("chmod");
        return -1;
    }
    return set_file_times(dst, src_st);
}

int finalize_copied_file(const char *dst, const struct stat *src_st) {
    return preserve_path_metadata(dst, src_st);
}
