/*
 * traversal.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "traversal.h"
#include "stats.h"
#include "fs_util.h"
#include "workers.h"
#include "ssh_transport.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

typedef struct dir_node {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    struct stat src_st;
    int depth;
    struct dir_node *next;
} dir_node_t;

typedef struct dir_record {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    struct stat src_st;
    int depth;
} dir_record_t;

static pthread_t *g_threads = NULL;
static int g_traversal_workers = 0;
static int g_status = 0;
static pthread_mutex_t g_status_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_src_root[PATH_MAX];
static char g_dst_root[PATH_MAX];

static pthread_mutex_t g_dir_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_dir_cond = PTHREAD_COND_INITIALIZER;
static dir_node_t *g_dir_head = NULL;
static dir_node_t *g_dir_tail = NULL;
static int g_dir_active = 0;
static int g_dir_done = 0;

static pthread_mutex_t g_finalize_lock = PTHREAD_MUTEX_INITIALIZER;
static dir_record_t *g_finalize_dirs = NULL;
static size_t g_finalize_dir_count = 0;
static size_t g_finalize_dir_cap = 0;

static int env_int_or_default(const char *name, int defval, int minval, int maxval)
{
    const char *s = getenv(name);
    if (!s || !*s) {
        return defval;
    }

    errno = 0;
    char *end = NULL;
    long v = strtol(s, &end, 10);

    if (errno != 0 || end == s || *end != '\0') {
        fprintf(stderr,
                "Warning: %s=%s is invalid; using default %d.\n",
                name,
                s,
                defval);
        return defval;
    }
    if (v < minval) {
        fprintf(stderr,
                "Warning: %s=%ld is below minimum %d; using %d.\n",
                name,
                v,
                minval,
                minval);
        return minval;
    }
    if (v > maxval) {
        fprintf(stderr,
                "Warning: %s=%ld exceeds maximum %d; using %d.\n",
                name,
                v,
                maxval,
                maxval);
        return maxval;
    }
    return (int)v;
}

static int path_is_same_or_child(const char *base, const char *path)
{
    size_t n = strlen(base);
    if (strncmp(base, path, n) != 0) {
        return 0;
    }
    return path[n] == '\0' || path[n] == '/';
}

static int copy_path_checked(char *dst, size_t dst_sz, const char *src, const char *label)
{
    if (snprintf(dst, dst_sz, "%s", src) >= (int)dst_sz) {
        fprintf(stderr, "%s path too long: %s\n", label, src);
        return -1;
    }
    return 0;
}

/*
 * Join "<parent>/<name>" into out. Done by hand because this runs for every
 * directory entry during traversal; snprintf("%s/%s") showed up as a real cost
 * under perf. Returns 0 on success, -1 if the result would not fit.
 */
static int join_path(char *out, size_t out_sz, const char *parent, const char *name)
{
    size_t pl = strlen(parent);
    size_t nl = strlen(name);

    if (pl + 1 + nl + 1 > out_sz) {
        return -1;
    }
    memcpy(out, parent, pl);
    out[pl] = '/';
    memcpy(out + pl + 1, name, nl);
    out[pl + 1 + nl] = '\0';
    return 0;
}

static void mark_traversal_error(void)
{
    pthread_mutex_lock(&g_status_lock);
    g_status = 1;
    pthread_mutex_unlock(&g_status_lock);
}

static int same_source_entry(const struct stat *a, const struct stat *b)
{
    return a->st_dev == b->st_dev &&
           a->st_ino == b->st_ino &&
           (a->st_mode & S_IFMT) == (b->st_mode & S_IFMT);
}

/*
 * Directory file descriptors are opened lazily, when a worker pops a directory
 * record off the queue, rather than at discovery time. This keeps the number of
 * open directory descriptors bounded by the number of traversal workers instead
 * of by the (potentially enormous) queue depth, which is what previously led to
 * "Too many open files" on directories with millions of children.
 */
static int open_verified_source_dir_path(const char *path,
                                         const struct stat *expected_st)
{
    struct stat opened_st;
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        perror(path);
        return -1;
    }
    if (fstat(fd, &opened_st) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        perror(path);
        return -1;
    }
    if (!S_ISDIR(opened_st.st_mode) || !same_source_entry(expected_st, &opened_st)) {
        close(fd);
        fprintf(stderr, "Source directory changed during traversal: %s\n", path);
        errno = ESTALE;
        return -1;
    }
    return fd;
}

static int open_or_create_target_dir_path(const char *path, mode_t mode)
{
    struct stat st;
    mode_t create_mode = (mode & 07777) | S_IRUSR | S_IWUSR | S_IXUSR;
    int fd;

    if (lstat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Target exists but is not a directory: %s\n", path);
            errno = EEXIST;
            return -1;
        }
    } else if (errno == ENOENT) {
        if (mkdir(path, create_mode) != 0 && errno != EEXIST) {
            perror(path);
            return -1;
        }
        if (lstat(path, &st) != 0) {
            perror(path);
            return -1;
        }
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Target exists but is not a directory: %s\n", path);
            errno = EEXIST;
            return -1;
        }
        stats_inc_dirs_created();
    } else {
        perror(path);
        return -1;
    }

    fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        perror(path);
        return -1;
    }
    return fd;
}

static int process_file(dir_handle_t *dir,
                        const char *name,
                        const char *src,
                        const char *dst)
{
    struct stat src_st, dst_st;
    if (fstatat(dir->src_fd, name, &src_st, AT_SYMLINK_NOFOLLOW) != 0) { perror(src); return -1; }
    if (!S_ISREG(src_st.st_mode)) return 0;
    stats_inc_files_seen();
    if (fstatat(dir->dst_fd, name, &dst_st, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISREG(dst_st.st_mode)) {
            fprintf(stderr, "Target exists but is not a regular file: %s\n", dst);
            return -1;
        }
        if (S_ISREG(dst_st.st_mode) && same_size_and_mtime(&src_st, &dst_st)) {
            if (preserve_path_metadata_at(dir->dst_fd, name, dst, &src_st, S_IFREG) != 0) {
                return -1;
            }
            stats_add_skipped_bytes((uint64_t)src_st.st_size);
            stats_inc_files_skipped();
            return 0;
        }
    } else if (errno != ENOENT) {
        perror(dst);
        return -1;
    }
    stats_add_planned_copy_bytes((uint64_t)src_st.st_size);
    if (workers_file_is_large(src_st.st_size)) {
        return workers_enqueue_large_file(dir, name, src, dst, &src_st);
    }
    return workers_enqueue_small_file(dir, name, src, dst, &src_st);
}

static int push_dir_locked(const char *src,
                           const char *dst,
                           const struct stat *src_st,
                           int depth)
{
    dir_node_t *n = calloc(1, sizeof(*n));
    if (!n) {
        perror("calloc");
        return -1;
    }

    snprintf(n->src, sizeof(n->src), "%s", src);
    snprintf(n->dst, sizeof(n->dst), "%s", dst);
    n->src_st = *src_st;
    n->depth = depth;

    if (g_dir_tail) {
        g_dir_tail->next = n;
    } else {
        g_dir_head = n;
    }
    g_dir_tail = n;
    pthread_cond_signal(&g_dir_cond);
    return 0;
}

static int record_directory_for_finalize(const char *src,
                                         const char *dst,
                                         const struct stat *src_st,
                                         int depth)
{
    dir_record_t *new_dirs;

    pthread_mutex_lock(&g_finalize_lock);
    if (g_finalize_dir_count == g_finalize_dir_cap) {
        size_t new_cap = g_finalize_dir_cap ? g_finalize_dir_cap * 2 : 1024;
        new_dirs = realloc(g_finalize_dirs, new_cap * sizeof(*g_finalize_dirs));
        if (!new_dirs) {
            pthread_mutex_unlock(&g_finalize_lock);
            perror("realloc");
            return -1;
        }
        g_finalize_dirs = new_dirs;
        g_finalize_dir_cap = new_cap;
    }

    new_dirs = g_finalize_dirs;
    snprintf(new_dirs[g_finalize_dir_count].src, sizeof(new_dirs[g_finalize_dir_count].src), "%s", src);
    snprintf(new_dirs[g_finalize_dir_count].dst, sizeof(new_dirs[g_finalize_dir_count].dst), "%s", dst);
    new_dirs[g_finalize_dir_count].src_st = *src_st;
    new_dirs[g_finalize_dir_count].depth = depth;
    g_finalize_dir_count++;
    pthread_mutex_unlock(&g_finalize_lock);
    return 0;
}

static int dir_record_cmp_desc_depth(const void *a, const void *b)
{
    const dir_record_t *da = (const dir_record_t *)a;
    const dir_record_t *db = (const dir_record_t *)b;

    if (da->depth != db->depth) {
        return db->depth - da->depth;
    }
    return strcmp(da->dst, db->dst);
}

typedef struct finalize_batch_ctx {
    size_t start;
    size_t end;
    size_t next;
    int failed;
    pthread_mutex_t lock;
} finalize_batch_ctx_t;

static void *finalize_batch_worker(void *arg)
{
    finalize_batch_ctx_t *ctx = (finalize_batch_ctx_t *)arg;

    for (;;) {
        size_t idx_local;

        pthread_mutex_lock(&ctx->lock);
        if (ctx->next >= ctx->end) {
            pthread_mutex_unlock(&ctx->lock);
            break;
        }
        idx_local = ctx->next++;
        pthread_mutex_unlock(&ctx->lock);

        int frc;
        if (sshx_active()) {
            frc = sshx_setmeta(g_finalize_dirs[idx_local].dst,
                               &g_finalize_dirs[idx_local].src_st, 1);
        } else {
            frc = preserve_path_metadata(g_finalize_dirs[idx_local].dst,
                                         &g_finalize_dirs[idx_local].src_st);
        }
        if (frc != 0) {
            pthread_mutex_lock(&ctx->lock);
            ctx->failed = 1;
            pthread_mutex_unlock(&ctx->lock);
        }
    }

    return NULL;
}

static int finalize_directories_parallel(void)
{
    size_t start;

    if (g_finalize_dir_count == 0) {
        return 0;
    }

    qsort(g_finalize_dirs,
          g_finalize_dir_count,
          sizeof(*g_finalize_dirs),
          dir_record_cmp_desc_depth);

    for (start = 0; start < g_finalize_dir_count; ) {
        size_t end = start + 1;
        int worker_count;
        pthread_t *threads;
        finalize_batch_ctx_t ctx;
        int i;
        int rc = 0;

        while (end < g_finalize_dir_count &&
               g_finalize_dirs[end].depth == g_finalize_dirs[start].depth) {
            end++;
        }

        worker_count = g_traversal_workers;
        if (worker_count < 1) {
            worker_count = 1;
        }
        if ((size_t)worker_count > end - start) {
            worker_count = (int)(end - start);
        }

        threads = calloc((size_t)worker_count, sizeof(*threads));
        if (!threads) {
            perror("calloc");
            return -1;
        }

        ctx.start = start;
        ctx.end = end;
        ctx.next = start;
        ctx.failed = 0;
        pthread_mutex_init(&ctx.lock, NULL);

        for (i = 0; i < worker_count; i++) {
            if (pthread_create(&threads[i], NULL, finalize_batch_worker, &ctx) != 0) {
                perror("pthread_create");
                ctx.failed = 1;
                worker_count = i;
                rc = -1;
                break;
            }
        }

        for (i = 0; i < worker_count; i++) {
            pthread_join(threads[i], NULL);
        }

        if (ctx.failed) {
            rc = -1;
        }

        pthread_mutex_destroy(&ctx.lock);
        free(threads);

        if (rc != 0) {
            return -1;
        }

        start = end;
    }

    return 0;
}

/*
 * Remote destination: process one directory's entries with batched bulk STAT.
 * Regular files are gathered in chunks and their destination stats fetched in a
 * single round-trip (the main small-file latency killer), then skip/enqueue
 * decisions are made locally. Subdirectories are queued immediately.
 */
#define REMOTE_STAT_BATCH 512

typedef struct {
    char name[256];
    struct stat st;
} remote_entry_t;

static int remote_flush_batch(dir_handle_t *handle,
                              const dir_node_t *node,
                              remote_entry_t *batch,
                              int n,
                              const char **names,
                              int *present,
                              struct stat *dst_st)
{
    int rc = 0;

    if (n == 0) {
        return 0;
    }
    for (int i = 0; i < n; i++) {
        names[i] = batch[i].name;
    }
    if (sshx_stat_bulk(node->dst, names, n, present, dst_st) != 0) {
        fprintf(stderr, "Bulk stat failed under %s\n", node->dst);
        return -1;
    }

    for (int i = 0; i < n; i++) {
        char src_path[PATH_MAX], dst_path[PATH_MAX];

        if (join_path(src_path, sizeof(src_path), node->src, batch[i].name) != 0 ||
            join_path(dst_path, sizeof(dst_path), node->dst, batch[i].name) != 0) {
            fprintf(stderr, "Path too long under %s\n", node->src);
            rc = -1;
            continue;
        }

        if (present[i]) {
            if (!S_ISREG(dst_st[i].st_mode)) {
                fprintf(stderr, "Target exists but is not a regular file: %s\n", dst_path);
                rc = -1;
                continue;
            }
            if (same_size_and_mtime(&batch[i].st, &dst_st[i])) {
                if (sshx_setmeta(dst_path, &batch[i].st, 0) != 0) {
                    rc = -1;
                    continue;
                }
                stats_add_skipped_bytes((uint64_t)batch[i].st.st_size);
                stats_inc_files_skipped();
                continue;
            }
        }

        stats_add_planned_copy_bytes((uint64_t)batch[i].st.st_size);
        if (workers_enqueue_small_file(handle, batch[i].name, src_path, dst_path,
                                       &batch[i].st) != 0) {
            rc = -1;
        }
    }
    return rc;
}

static void process_dir_entries_remote(dir_handle_t *handle,
                                       const dir_node_t *node,
                                       DIR *dir)
{
    struct dirent *entry;
    remote_entry_t *batch = malloc(sizeof(*batch) * REMOTE_STAT_BATCH);
    const char **names = malloc(sizeof(*names) * REMOTE_STAT_BATCH);
    int *present = malloc(sizeof(*present) * REMOTE_STAT_BATCH);
    struct stat *dst_st = malloc(sizeof(*dst_st) * REMOTE_STAT_BATCH);
    int n = 0;

    if (!batch || !names || !present || !dst_st) {
        perror("malloc");
        mark_traversal_error();
        goto done;
    }

    while ((entry = readdir(dir)) != NULL) {
        struct stat st;
        char src_path[PATH_MAX], dst_path[PATH_MAX];

        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;

        if (fstatat(handle->src_fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            perror(entry->d_name);
            mark_traversal_error();
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            if (join_path(src_path, sizeof(src_path), node->src, entry->d_name) != 0 ||
                join_path(dst_path, sizeof(dst_path), node->dst, entry->d_name) != 0) {
                fprintf(stderr, "Path too long under %s\n", node->src);
                mark_traversal_error();
                continue;
            }
            pthread_mutex_lock(&g_dir_lock);
            if (push_dir_locked(src_path, dst_path, &st, node->depth + 1) != 0) {
                mark_traversal_error();
            }
            pthread_mutex_unlock(&g_dir_lock);
        } else if (S_ISREG(st.st_mode)) {
            if (strlen(entry->d_name) >= sizeof(batch[0].name)) {
                fprintf(stderr, "Name too long: %s\n", entry->d_name);
                mark_traversal_error();
                continue;
            }
            stats_inc_files_seen();
            snprintf(batch[n].name, sizeof(batch[n].name), "%s", entry->d_name);
            batch[n].st = st;
            n++;
            if (n == REMOTE_STAT_BATCH) {
                if (remote_flush_batch(handle, node, batch, n, names, present, dst_st) != 0) {
                    mark_traversal_error();
                }
                n = 0;
            }
        }
    }

    if (remote_flush_batch(handle, node, batch, n, names, present, dst_st) != 0) {
        mark_traversal_error();
    }

done:
    free(batch);
    free(names);
    free(present);
    free(dst_st);
}

static dir_node_t *pop_dir_locked(void)
{
    dir_node_t *node = g_dir_head;
    if (!node) {
        return NULL;
    }
    g_dir_head = node->next;
    if (!g_dir_head) {
        g_dir_tail = NULL;
    }
    node->next = NULL;
    return node;
}

static void process_directory_node(dir_node_t *node)
{
    struct stat st;
    DIR *dir;
    struct dirent *entry;
    char src_path[PATH_MAX], dst_path[PATH_MAX];
    dir_handle_t *handle;
    int src_fd;
    int dst_fd;
    int dir_stream_fd;

    /*
     * Open the source and destination directory descriptors now, only while
     * this directory is actually being processed. The queue itself holds no
     * open descriptors, so a single parent with millions of subdirectories no
     * longer keeps millions of descriptors open at once.
     */
    src_fd = open_verified_source_dir_path(node->src, &node->src_st);
    if (src_fd < 0) {
        mark_traversal_error();
        return;
    }
    if (fstat(src_fd, &st) != 0) {
        perror(node->src);
        close(src_fd);
        mark_traversal_error();
        return;
    }
    if (sshx_active()) {
        /* Remote destination: create the directory over the protocol; there is
         * no local destination fd. */
        if (sshx_mkdir(node->dst, node->src_st.st_mode & 07777) != 0) {
            perror(node->dst);
            close(src_fd);
            mark_traversal_error();
            return;
        }
        stats_inc_dirs_created();
        dst_fd = -1;
    } else {
        dst_fd = open_or_create_target_dir_path(node->dst, node->src_st.st_mode & 07777);
        if (dst_fd < 0) {
            close(src_fd);
            mark_traversal_error();
            return;
        }
    }
    handle = dir_handle_create(node->src, node->dst, src_fd, dst_fd);
    if (!handle) {
        close(src_fd);
        close(dst_fd);
        mark_traversal_error();
        return;
    }

    stats_inc_dirs_seen();
    if (record_directory_for_finalize(node->src, node->dst, &st, node->depth) != 0) {
        dir_handle_release(handle);
        mark_traversal_error();
        return;
    }

    dir_stream_fd = dup(handle->src_fd);
    if (dir_stream_fd < 0) {
        perror(node->src);
        dir_handle_release(handle);
        mark_traversal_error();
        return;
    }
    dir = fdopendir(dir_stream_fd);
    if (!dir) {
        close(dir_stream_fd);
        perror(node->src);
        dir_handle_release(handle);
        mark_traversal_error();
        return;
    }

    if (sshx_active()) {
        process_dir_entries_remote(handle, node, dir);
        closedir(dir);
        dir_handle_release(handle);
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;

        if (join_path(src_path, sizeof(src_path), node->src, entry->d_name) != 0) {
            fprintf(stderr, "Source path too long: %s/%s\n", node->src, entry->d_name);
            mark_traversal_error();
            continue;
        }
        if (join_path(dst_path, sizeof(dst_path), node->dst, entry->d_name) != 0) {
            fprintf(stderr, "Target path too long: %s/%s\n", node->dst, entry->d_name);
            mark_traversal_error();
            continue;
        }

        if (path_is_same_or_child(g_dst_root, src_path)) {
            continue;
        }

        if (fstatat(handle->src_fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            perror(src_path);
            mark_traversal_error();
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            pthread_mutex_lock(&g_dir_lock);
            if (push_dir_locked(src_path, dst_path, &st, node->depth + 1) != 0) {
                mark_traversal_error();
            }
            pthread_mutex_unlock(&g_dir_lock);
        } else if (S_ISREG(st.st_mode)) {
            if (process_file(handle, entry->d_name, src_path, dst_path) != 0) {
                mark_traversal_error();
            }
        }
    }

    closedir(dir);
    dir_handle_release(handle);
}

static void *traversal_worker_main(void *arg)
{
    (void)arg;

    for (;;) {
        dir_node_t *node;

        pthread_mutex_lock(&g_dir_lock);
        for (;;) {
            node = pop_dir_locked();
            if (node) {
                g_dir_active++;
                break;
            }
            if (g_dir_done) {
                pthread_mutex_unlock(&g_dir_lock);
                return NULL;
            }
            pthread_cond_wait(&g_dir_cond, &g_dir_lock);
        }
        pthread_mutex_unlock(&g_dir_lock);

        process_directory_node(node);
        free(node);

        pthread_mutex_lock(&g_dir_lock);
        g_dir_active--;
        if (!g_dir_head && g_dir_active == 0) {
            g_dir_done = 1;
            pthread_cond_broadcast(&g_dir_cond);
        }
        pthread_mutex_unlock(&g_dir_lock);
    }
}

int traversal_start(const char *src_dir, const char *dst_dir) {
    int i;
    struct stat root_st;

    pthread_mutex_lock(&g_status_lock);
    g_status = 0;
    pthread_mutex_unlock(&g_status_lock);
    pthread_mutex_lock(&g_finalize_lock);
    free(g_finalize_dirs);
    g_finalize_dirs = NULL;
    g_finalize_dir_count = 0;
    g_finalize_dir_cap = 0;
    pthread_mutex_unlock(&g_finalize_lock);
    if (copy_path_checked(g_src_root, sizeof(g_src_root), src_dir, "Source root") != 0 ||
        copy_path_checked(g_dst_root, sizeof(g_dst_root), dst_dir, "Target root") != 0) {
        return -1;
    }

    if (lstat(g_src_root, &root_st) != 0) {
        perror(g_src_root);
        return -1;
    }
    if (!S_ISDIR(root_st.st_mode)) {
        fprintf(stderr, "Source is not a directory: %s\n", g_src_root);
        return -1;
    }

    g_traversal_workers = env_int_or_default("DIRECT_COPY_TRAVERSAL_WORKERS", 8, 1, 128);
    g_threads = calloc((size_t)g_traversal_workers, sizeof(*g_threads));
    if (!g_threads) {
        perror("calloc");
        return -1;
    }

    pthread_mutex_lock(&g_dir_lock);
    g_dir_head = NULL;
    g_dir_tail = NULL;
    g_dir_active = 0;
    g_dir_done = 0;
    if (push_dir_locked(g_src_root, g_dst_root, &root_st, 0) != 0) {
        pthread_mutex_unlock(&g_dir_lock);
        free(g_threads);
        g_threads = NULL;
        return -1;
    }
    pthread_mutex_unlock(&g_dir_lock);

    for (i = 0; i < g_traversal_workers; i++) {
        if (pthread_create(&g_threads[i], NULL, traversal_worker_main, NULL) != 0) {
            perror("pthread_create");
            pthread_mutex_lock(&g_dir_lock);
            g_dir_done = 1;
            pthread_cond_broadcast(&g_dir_cond);
            pthread_mutex_unlock(&g_dir_lock);
            while (--i >= 0) {
                pthread_join(g_threads[i], NULL);
            }
            free(g_threads);
            g_threads = NULL;
            return -1;
        }
    }
    return 0;
}

void traversal_wait(void)
{
    int i;
    for (i = 0; i < g_traversal_workers; i++) {
        pthread_join(g_threads[i], NULL);
    }
    free(g_threads);
    g_threads = NULL;
    stats_set_traversal_done();
}

int traversal_finalize_metadata(void)
{
    int rc = finalize_directories_parallel();

    if (rc != 0) {
        mark_traversal_error();
    } else {
        stats_set_finalize_done();
    }
    pthread_mutex_lock(&g_finalize_lock);
    free(g_finalize_dirs);
    g_finalize_dirs = NULL;
    g_finalize_dir_count = 0;
    g_finalize_dir_cap = 0;
    pthread_mutex_unlock(&g_finalize_lock);
    return rc == 0 ? 0 : -1;
}

int traversal_status(void)
{
    int status;

    pthread_mutex_lock(&g_status_lock);
    status = g_status;
    pthread_mutex_unlock(&g_status_lock);
    return status;
}
