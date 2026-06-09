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
#include <pthread.h>
#include <stdint.h>

static int direct_io_fallback_errno(int err) {
    return err == EINVAL || err == EOPNOTSUPP || err == ENOTSUP || err == ENOSYS;
}

static int chown_permission_errno(int err) {
    return err == EPERM || err == EACCES;
}

static int g_copy_file_range_enabled = 1;
static pthread_once_t g_copy_file_range_once = PTHREAD_ONCE_INIT;
static int g_read_direct_io_enabled = 1;
static pthread_once_t g_read_direct_io_once = PTHREAD_ONCE_INIT;
static int g_write_direct_io_enabled = 1;
static pthread_once_t g_write_direct_io_once = PTHREAD_ONCE_INIT;
static int g_warned_chown_permission = 0;
static pthread_mutex_t g_warning_lock = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_tmp_counter = 0;
static pthread_mutex_t g_tmp_lock = PTHREAD_MUTEX_INITIALIZER;

dir_handle_t *dir_handle_create(const char *src, const char *dst, int src_fd, int dst_fd)
{
    dir_handle_t *dir = calloc(1, sizeof(*dir));
    if (!dir) {
        perror("calloc");
        return NULL;
    }

    snprintf(dir->src, sizeof(dir->src), "%s", src);
    snprintf(dir->dst, sizeof(dir->dst), "%s", dst);
    dir->src_fd = src_fd;
    dir->dst_fd = dst_fd;
    dir->refs = 1;
    pthread_mutex_init(&dir->lock, NULL);
    return dir;
}

void dir_handle_retain(dir_handle_t *dir)
{
    if (!dir) {
        return;
    }
    pthread_mutex_lock(&dir->lock);
    dir->refs++;
    pthread_mutex_unlock(&dir->lock);
}

void dir_handle_release(dir_handle_t *dir)
{
    int should_free = 0;

    if (!dir) {
        return;
    }

    pthread_mutex_lock(&dir->lock);
    if (dir->refs > 0) {
        dir->refs--;
    }
    should_free = dir->refs == 0;
    pthread_mutex_unlock(&dir->lock);

    if (!should_free) {
        return;
    }

    if (dir->src_fd >= 0) {
        close(dir->src_fd);
    }
    if (dir->dst_fd >= 0) {
        close(dir->dst_fd);
    }
    pthread_mutex_destroy(&dir->lock);
    free(dir);
}

static mode_t copy_data_mode(mode_t final_mode) {
    return (final_mode & 0777) | S_IRUSR | S_IWUSR;
}

static int open_existing_regular_for_chmod(const char *path) {
    int fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0 && errno == EACCES) {
        fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    }
    if (fd < 0 && errno == EACCES) {
        fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    }
    return fd;
}

static int add_owner_rw_to_existing_regular(const char *path) {
    int fd;
    int saved_errno;
    struct stat st;

    fd = open_existing_regular_for_chmod(path);
    if (fd < 0) {
        return -1;
    }

    if (fstat(fd, &st) != 0) {
        saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        close(fd);
        errno = S_ISDIR(st.st_mode) ? EISDIR : EINVAL;
        return -1;
    }

    if (fchmod(fd, copy_data_mode(st.st_mode)) != 0) {
        saved_errno = errno;
        stats_inc_metadata_error();
        close(fd);
        errno = saved_errno;
        return -1;
    }

    close(fd);
    return 0;
}

static int clear_nonblock(int fd, const char *path)
{
    int flags = fcntl(fd, F_GETFL);

    if (flags < 0) {
        progress_interrupt();
        perror(path);
        return -1;
    }
    if ((flags & O_NONBLOCK) == 0) {
        return 0;
    }
    if (fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) != 0) {
        progress_interrupt();
        perror(path);
        return -1;
    }
    return 0;
}

static int same_opened_source(const struct stat *expected, const struct stat *actual)
{
    return expected->st_dev == actual->st_dev &&
           expected->st_ino == actual->st_ino &&
           (expected->st_mode & S_IFMT) == (actual->st_mode & S_IFMT) &&
           same_size_and_mtime(expected, actual);
}

static int validate_opened_source_file(int fd,
                                       const char *display_path,
                                       const struct stat *expected_st)
{
    struct stat st;

    if (fstat(fd, &st) != 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        progress_interrupt();
        fprintf(stderr, "Source is not a regular file: %s\n", display_path);
        errno = EINVAL;
        return -1;
    }
    if (expected_st && !same_opened_source(expected_st, &st)) {
        progress_interrupt();
        fprintf(stderr, "Source changed during traversal: %s\n", display_path);
        errno = ESTALE;
        return -1;
    }
    return 0;
}

static void init_copy_file_range_enabled(void)
{
    const char *env = getenv("DIRECT_COPY_DISABLE_COPY_FILE_RANGE");
    if (!env || !*env || strcmp(env, "0") == 0) {
        g_copy_file_range_enabled = 1;
    } else {
        g_copy_file_range_enabled = 0;
    }
}

int copy_file_range_enabled(void) {
    pthread_once(&g_copy_file_range_once, init_copy_file_range_enabled);
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

static void init_read_direct_io_enabled(void)
{
    if (env_disable_flag("DIRECT_COPY_DISABLE_READ_DIRECT_IO") ||
        env_disable_flag("DIRECT_COPY_DISABLE_DIRECT_IO")) {
        g_read_direct_io_enabled = 0;
    } else {
        g_read_direct_io_enabled = 1;
    }
}

int read_direct_io_enabled(void)
{
    pthread_once(&g_read_direct_io_once, init_read_direct_io_enabled);
    return g_read_direct_io_enabled;
}

static void init_write_direct_io_enabled(void)
{
    if (env_disable_flag("DIRECT_COPY_DISABLE_WRITE_DIRECT_IO") ||
        env_disable_flag("DIRECT_COPY_DISABLE_DIRECT_IO")) {
        g_write_direct_io_enabled = 0;
    } else {
        g_write_direct_io_enabled = 1;
    }
}

int write_direct_io_enabled(void)
{
    pthread_once(&g_write_direct_io_once, init_write_direct_io_enabled);
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

int open_read_at_maybe_direct(int dir_fd,
                              const char *name,
                              const char *display_path,
                              const struct stat *expected_st,
                              int *used_direct) {
    int fd;
    if (used_direct) *used_direct = 0;

    if (read_direct_io_enabled()) {
        fd = openat(dir_fd, name, O_RDONLY | O_DIRECT | O_CLOEXEC | O_NOFOLLOW);
        if (fd >= 0) {
            if (validate_opened_source_file(fd, display_path, expected_st) != 0) {
                close(fd);
                return -1;
            }
            if (used_direct) *used_direct = 1;
            stats_record_read_open(1);
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            progress_interrupt();
            perror(display_path);
            return -1;
        }
    }

    fd = openat(dir_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    if (validate_opened_source_file(fd, display_path, expected_st) != 0) {
        close(fd);
        return -1;
    }
    stats_record_read_open(0);
    return fd;
}

int open_read_at_buffered(int dir_fd,
                          const char *name,
                          const char *display_path,
                          const struct stat *expected_st)
{
    int fd = openat(dir_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    if (validate_opened_source_file(fd, display_path, expected_st) != 0) {
        close(fd);
        return -1;
    }
    stats_record_read_open(0);
    return fd;
}

static void report_target_not_regular(const char *path)
{
    progress_interrupt();
    fprintf(stderr, "Target exists but is not a regular file: %s\n", path);
}

static void report_target_open_error(const char *path)
{
    progress_interrupt();
    if (errno == ELOOP) {
        fprintf(stderr, "Target exists but is not a regular file: %s\n", path);
    } else {
        perror(path);
    }
}

static int validate_opened_regular_file(int fd, const char *path)
{
    struct stat st;

    if (fstat(fd, &st) != 0) {
        progress_interrupt();
        perror(path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        report_target_not_regular(path);
        errno = EEXIST;
        return -1;
    }
    if (clear_nonblock(fd, path) != 0) {
        return -1;
    }
    return 0;
}

static int ensure_target_regular_or_missing(const char *path)
{
    struct stat st;

    if (lstat(path, &st) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
        progress_interrupt();
        perror(path);
        return -1;
    }

    if (!S_ISREG(st.st_mode)) {
        report_target_not_regular(path);
        errno = EEXIST;
        return -1;
    }

    return 0;
}

int open_write_maybe_direct(const char *path, mode_t mode, int *used_direct) {
    int fd;
    mode_t open_mode = copy_data_mode(mode);
    if (used_direct) *used_direct = 0;

    if (ensure_target_regular_or_missing(path) != 0) {
        return -1;
    }

    if (write_direct_io_enabled()) {
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, open_mode);
        if (fd < 0 && errno == EACCES) {
            int saved_errno = errno;
            if (add_owner_rw_to_existing_regular(path) == 0) {
                fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, open_mode);
            } else {
                errno = saved_errno;
            }
        }
        if (fd >= 0) {
            if (validate_opened_regular_file(fd, path) != 0) {
                close(fd);
                return -1;
            }
            if (fchmod(fd, open_mode) != 0) {
                int saved_errno = errno;
                stats_inc_metadata_error();
                close(fd);
                errno = saved_errno;
                progress_interrupt();
                perror("fchmod");
                return -1;
            }
            if (used_direct) *used_direct = 1;
            stats_record_write_open(1);
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            report_target_open_error(path);
            return -1;
        }
    }

    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, open_mode);
    if (fd < 0 && errno == EACCES) {
        int saved_errno = errno;
        if (add_owner_rw_to_existing_regular(path) == 0) {
            fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, open_mode);
        } else {
            errno = saved_errno;
        }
    }
    if (fd < 0) {
        report_target_open_error(path);
        return -1;
    }
    if (validate_opened_regular_file(fd, path) != 0) {
        close(fd);
        return -1;
    }
    if (fchmod(fd, open_mode) != 0) {
        int saved_errno = errno;
        stats_inc_metadata_error();
        close(fd);
        errno = saved_errno;
        progress_interrupt();
        perror("fchmod");
        return -1;
    }
    stats_record_write_open(0);
    return fd;
}

int open_write_existing_maybe_direct(const char *path, int *used_direct) {
    int fd;
    if (used_direct) *used_direct = 0;

    if (ensure_target_regular_or_missing(path) != 0) {
        return -1;
    }

    if (write_direct_io_enabled()) {
        fd = open(path, O_WRONLY | O_DIRECT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (fd < 0 && errno == EACCES) {
            int saved_errno = errno;
            if (add_owner_rw_to_existing_regular(path) == 0) {
                fd = open(path, O_WRONLY | O_DIRECT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
            } else {
                errno = saved_errno;
            }
        }
        if (fd >= 0) {
            if (validate_opened_regular_file(fd, path) != 0) {
                close(fd);
                return -1;
            }
            if (used_direct) *used_direct = 1;
            stats_record_write_open(1);
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            report_target_open_error(path);
            return -1;
        }
    }

    fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0 && errno == EACCES) {
        int saved_errno = errno;
        if (add_owner_rw_to_existing_regular(path) == 0) {
            fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        } else {
            errno = saved_errno;
        }
    }
    if (fd < 0) {
        report_target_open_error(path);
        return -1;
    }
    if (validate_opened_regular_file(fd, path) != 0) {
        close(fd);
        return -1;
    }
    stats_record_write_open(0);
    return fd;
}

int open_write_existing_buffered(const char *path) {
    int fd;

    if (ensure_target_regular_or_missing(path) != 0) {
        return -1;
    }

    fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0 && errno == EACCES) {
        int saved_errno = errno;
        if (add_owner_rw_to_existing_regular(path) == 0) {
            fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        } else {
            errno = saved_errno;
        }
    }
    if (fd < 0) {
        report_target_open_error(path);
        return -1;
    }
    if (validate_opened_regular_file(fd, path) != 0) {
        close(fd);
        return -1;
    }
    stats_record_write_open(0);
    return fd;
}

static int make_temp_name(char *tmp_name, size_t tmp_name_sz)
{
    uint64_t n;

    pthread_mutex_lock(&g_tmp_lock);
    n = ++g_tmp_counter;
    pthread_mutex_unlock(&g_tmp_lock);

    if (snprintf(tmp_name,
                 tmp_name_sz,
                 ".ecopy.tmp.%ld.%llu",
                 (long)getpid(),
                 (unsigned long long)n) >= (int)tmp_name_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int validate_opened_temp_regular(int fd, const char *display_path)
{
    struct stat st;

    if (fstat(fd, &st) != 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        progress_interrupt();
        fprintf(stderr, "Temporary target is not a regular file for %s\n", display_path);
        errno = EEXIST;
        return -1;
    }
    if (clear_nonblock(fd, display_path) != 0) {
        return -1;
    }
    return 0;
}

static int open_temp_created_once(int dir_fd,
                                  const char *tmp_name,
                                  const char *display_path,
                                  mode_t open_mode,
                                  int direct,
                                  int *used_direct)
{
    int flags = O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    int fd;

    if (direct) {
        flags |= O_DIRECT;
    }

    fd = openat(dir_fd, tmp_name, flags, open_mode);
    if (fd < 0) {
        return -1;
    }
    if (validate_opened_temp_regular(fd, display_path) != 0) {
        close(fd);
        return -1;
    }
    if (fchmod(fd, open_mode) != 0) {
        int saved_errno = errno;
        stats_inc_metadata_error();
        close(fd);
        errno = saved_errno;
        progress_interrupt();
        perror("fchmod");
        return -1;
    }
    if (used_direct) {
        *used_direct = direct ? 1 : 0;
    }
    stats_record_write_open(direct ? 1 : 0);
    return fd;
}

int create_temp_write_at_maybe_direct(int dir_fd,
                                      const char *display_path,
                                      mode_t mode,
                                      char *tmp_name,
                                      size_t tmp_name_sz,
                                      int *used_direct)
{
    mode_t open_mode = copy_data_mode(mode);
    int attempt;

    if (used_direct) {
        *used_direct = 0;
    }

    for (attempt = 0; attempt < 100; attempt++) {
        int fd;

        if (make_temp_name(tmp_name, tmp_name_sz) != 0) {
            progress_interrupt();
            perror(display_path);
            return -1;
        }

        if (write_direct_io_enabled()) {
            fd = open_temp_created_once(dir_fd, tmp_name, display_path, open_mode, 1, used_direct);
            if (fd >= 0) {
                return fd;
            }
            if (errno == EEXIST) {
                continue;
            }
            if (!direct_io_fallback_errno(errno)) {
                progress_interrupt();
                perror(display_path);
                return -1;
            }
            unlinkat(dir_fd, tmp_name, 0);
        }

        fd = open_temp_created_once(dir_fd, tmp_name, display_path, open_mode, 0, used_direct);
        if (fd >= 0) {
            return fd;
        }
        if (errno == EEXIST) {
            continue;
        }
        progress_interrupt();
        perror(display_path);
        return -1;
    }

    progress_interrupt();
    fprintf(stderr, "Could not create a unique temporary target for %s\n", display_path);
    errno = EEXIST;
    return -1;
}

static int open_final_created_once(int dir_fd,
                                   const char *name,
                                   const char *display_path,
                                   mode_t open_mode,
                                   int direct,
                                   int *used_direct)
{
    int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    int fd;

    if (direct) {
        flags |= O_DIRECT;
    }

    fd = openat(dir_fd, name, flags, open_mode);
    if (fd < 0 && errno == EACCES) {
        /*
         * An existing destination that is not owner-writable cannot be opened
         * for truncation. Add owner write (the final mode is restored later by
         * the metadata pass) and retry once.
         */
        if (fchmodat(dir_fd, name, open_mode, 0) == 0) {
            fd = openat(dir_fd, name, flags, open_mode);
        }
    }
    if (fd < 0) {
        return -1;
    }
    if (validate_opened_temp_regular(fd, display_path) != 0) {
        close(fd);
        return -1;
    }
    if (fchmod(fd, open_mode) != 0) {
        int saved_errno = errno;
        stats_inc_metadata_error();
        close(fd);
        errno = saved_errno;
        progress_interrupt();
        perror("fchmod");
        return -1;
    }
    if (used_direct) {
        *used_direct = direct ? 1 : 0;
    }
    stats_record_write_open(direct ? 1 : 0);
    return fd;
}

/*
 * In-place variant of create_temp_write_at_maybe_direct(): opens (creating or
 * truncating) the final destination name directly instead of a temporary file.
 * This avoids the rename step and therefore halves the per-file directory
 * inode-lock operations, at the cost of crash atomicity (an interrupted copy
 * can leave a partially written destination).
 */
int create_final_write_at_maybe_direct(int dir_fd,
                                       const char *name,
                                       const char *display_path,
                                       mode_t mode,
                                       int *used_direct)
{
    mode_t open_mode = copy_data_mode(mode);
    int fd;

    if (used_direct) {
        *used_direct = 0;
    }

    if (write_direct_io_enabled()) {
        fd = open_final_created_once(dir_fd, name, display_path, open_mode, 1, used_direct);
        if (fd >= 0) {
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            progress_interrupt();
            perror(display_path);
            return -1;
        }
    }

    fd = open_final_created_once(dir_fd, name, display_path, open_mode, 0, used_direct);
    if (fd < 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    return fd;
}

int open_temp_write_existing_at_maybe_direct(int dir_fd,
                                             const char *tmp_name,
                                             const char *display_path,
                                             int *used_direct)
{
    int fd;
    if (used_direct) *used_direct = 0;

    if (write_direct_io_enabled()) {
        fd = openat(dir_fd, tmp_name, O_WRONLY | O_DIRECT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (fd >= 0) {
            if (validate_opened_temp_regular(fd, display_path) != 0) {
                close(fd);
                return -1;
            }
            if (used_direct) *used_direct = 1;
            stats_record_write_open(1);
            return fd;
        }
        if (!direct_io_fallback_errno(errno)) {
            progress_interrupt();
            perror(display_path);
            return -1;
        }
    }

    return open_temp_write_existing_at_buffered(dir_fd, tmp_name, display_path);
}

int open_temp_write_existing_at_buffered(int dir_fd,
                                         const char *tmp_name,
                                         const char *display_path)
{
    int fd = openat(dir_fd, tmp_name, O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    if (validate_opened_temp_regular(fd, display_path) != 0) {
        close(fd);
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

int same_size_and_mtime(const struct stat *a, const struct stat *b) {
    return a->st_size == b->st_size &&
           a->st_mtim.tv_sec == b->st_mtim.tv_sec &&
           a->st_mtim.tv_nsec == b->st_mtim.tv_nsec;
}

int preserve_fd_metadata(int fd, const char *path_for_warning, const struct stat *src_st) {
    struct timespec ts[2];

    if (fchown(fd, src_st->st_uid, src_st->st_gid) != 0) {
        if (chown_permission_errno(errno)) {
            stats_inc_metadata_warning();
            pthread_mutex_lock(&g_warning_lock);
            if (!g_warned_chown_permission) {
                progress_interrupt();
                fprintf(stderr,
                        "Warning: chown not permitted; continuing without preserving uid/gid ownership.\n");
                g_warned_chown_permission = 1;
            }
            pthread_mutex_unlock(&g_warning_lock);
        } else {
            progress_interrupt();
            if (path_for_warning && *path_for_warning) {
                fprintf(stderr, "%s: ", path_for_warning);
            }
            perror("fchown");
            stats_inc_metadata_error();
            return -1;
        }
    }

    if (fchmod(fd, src_st->st_mode & 07777) != 0) {
        progress_interrupt();
        if (path_for_warning && *path_for_warning) {
            fprintf(stderr, "%s: ", path_for_warning);
        }
        perror("fchmod");
        stats_inc_metadata_error();
        return -1;
    }

    ts[0] = src_st->st_atim;
    ts[1] = src_st->st_mtim;
    if (futimens(fd, ts) != 0) {
        progress_interrupt();
        if (path_for_warning && *path_for_warning) {
            fprintf(stderr, "%s: ", path_for_warning);
        }
        perror("futimens");
        stats_inc_metadata_error();
        return -1;
    }

    return 0;
}

int preserve_path_metadata(const char *dst, const struct stat *src_st) {
    int fd = open(dst, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        progress_interrupt();
        stats_inc_metadata_error();
        if (errno == ELOOP) {
            fprintf(stderr, "Target exists but is not a regular file or directory: %s\n", dst);
        } else {
            perror(dst);
        }
        return -1;
    }

    int rc = preserve_fd_metadata(fd, dst, src_st);
    close(fd);
    return rc;
}

int preserve_path_metadata_at(int dir_fd,
                              const char *name,
                              const char *display_path,
                              const struct stat *src_st,
                              mode_t expected_type)
{
    struct stat st;
    int fd = openat(dir_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) {
        progress_interrupt();
        stats_inc_metadata_error();
        if (errno == ELOOP) {
            fprintf(stderr, "Target exists but is not the expected type: %s\n", display_path);
        } else {
            perror(display_path);
        }
        return -1;
    }

    if (fstat(fd, &st) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        progress_interrupt();
        perror(display_path);
        stats_inc_metadata_error();
        return -1;
    }
    if ((st.st_mode & S_IFMT) != expected_type) {
        close(fd);
        progress_interrupt();
        fprintf(stderr, "Target exists but is not the expected type: %s\n", display_path);
        stats_inc_metadata_error();
        errno = EINVAL;
        return -1;
    }

    int rc = preserve_fd_metadata(fd, display_path, src_st);
    close(fd);
    return rc;
}

int rename_temp_to_final_at(int dir_fd,
                            const char *tmp_name,
                            const char *final_name,
                            const char *display_path)
{
    struct stat st;

    if (fstatat(dir_fd, tmp_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        progress_interrupt();
        fprintf(stderr, "Temporary target is not a regular file for %s\n", display_path);
        errno = EINVAL;
        return -1;
    }

    if (fstatat(dir_fd, final_name, &st, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISREG(st.st_mode)) {
            progress_interrupt();
            fprintf(stderr, "Target exists but is not a regular file: %s\n", display_path);
            errno = EEXIST;
            return -1;
        }
    } else if (errno != ENOENT) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }

    if (renameat(dir_fd, tmp_name, dir_fd, final_name) != 0) {
        progress_interrupt();
        perror(display_path);
        return -1;
    }
    return 0;
}

void unlink_temp_at(int dir_fd, const char *tmp_name)
{
    if (tmp_name && *tmp_name) {
        (void)unlinkat(dir_fd, tmp_name, 0);
    }
}

int finalize_copied_file(const char *dst, const struct stat *src_st) {
    return preserve_path_metadata(dst, src_st);
}

int finalize_copied_file_fd(int fd, const char *path_for_warning, const struct stat *src_st) {
    return preserve_fd_metadata(fd, path_for_warning, src_st);
}
