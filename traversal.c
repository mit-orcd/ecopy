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
    dir_handle_t *dir;
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
static pthread_cond_t g_dir_space_cond = PTHREAD_COND_INITIALIZER;
static dir_node_t *g_dir_head = NULL;
static dir_node_t *g_dir_tail = NULL;
static size_t g_dir_queue_depth = 0;
static int g_max_queued_dirs = 0;
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

static int open_verified_source_dir(int parent_fd,
                                    const char *name,
                                    const char *display_path,
                                    const struct stat *expected_st)
{
    struct stat opened_st;
    int fd = openat(parent_fd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        perror(display_path);
        return -1;
    }
    if (fstat(fd, &opened_st) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        perror(display_path);
        return -1;
    }
    if (!S_ISDIR(opened_st.st_mode) || !same_source_entry(expected_st, &opened_st)) {
        close(fd);
        fprintf(stderr, "Source directory changed during traversal: %s\n", display_path);
        errno = ESTALE;
        return -1;
    }
    return fd;
}

static int open_or_create_target_dir(int parent_fd,
                                     const char *name,
                                     const char *display_path,
                                     mode_t mode)
{
    struct stat st;
    mode_t create_mode = (mode & 07777) | S_IRUSR | S_IWUSR | S_IXUSR;
    int fd;

    if (fstatat(parent_fd, name, &st, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Target exists but is not a directory: %s\n", display_path);
            errno = EEXIST;
            return -1;
        }
    } else if (errno == ENOENT) {
        if (mkdirat(parent_fd, name, create_mode) != 0 && errno != EEXIST) {
            perror(display_path);
            return -1;
        }
        if (fstatat(parent_fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            perror(display_path);
            return -1;
        }
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Target exists but is not a directory: %s\n", display_path);
            errno = EEXIST;
            return -1;
        }
        stats_inc_dirs_created();
    } else {
        perror(display_path);
        return -1;
    }

    fd = openat(parent_fd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        perror(display_path);
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

static int push_dir_locked(dir_handle_t *dir, int depth)
{
    dir_node_t *n;

    while (g_dir_queue_depth >= (size_t)g_max_queued_dirs && !g_dir_done) {
        pthread_cond_wait(&g_dir_space_cond, &g_dir_lock);
    }
    if (g_dir_done) {
        return -1;
    }

    n = calloc(1, sizeof(*n));
    if (!n) {
        perror("calloc");
        return -1;
    }

    n->dir = dir;
    n->depth = depth;

    if (g_dir_tail) {
        g_dir_tail->next = n;
    } else {
        g_dir_head = n;
    }
    g_dir_tail = n;
    g_dir_queue_depth++;
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

        if (preserve_path_metadata(g_finalize_dirs[idx_local].dst,
                                   &g_finalize_dirs[idx_local].src_st) != 0) {
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
    if (g_dir_queue_depth > 0) {
        g_dir_queue_depth--;
    }
    pthread_cond_broadcast(&g_dir_space_cond);
    return node;
}

static void process_directory_node(dir_node_t *node)
{
    struct stat st;
    DIR *dir;
    struct dirent *entry;
    char src_path[PATH_MAX], dst_path[PATH_MAX];
    int dir_stream_fd;

    if (fstat(node->dir->src_fd, &st) != 0) {
        perror(node->dir->src);
        mark_traversal_error();
        return;
    }
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source is not a directory: %s\n", node->dir->src);
        mark_traversal_error();
        return;
    }

    stats_inc_dirs_seen();
    if (record_directory_for_finalize(node->dir->src, node->dir->dst, &st, node->depth) != 0) {
        mark_traversal_error();
        return;
    }

    dir_stream_fd = dup(node->dir->src_fd);
    if (dir_stream_fd < 0) {
        perror(node->dir->src);
        mark_traversal_error();
        return;
    }
    dir = fdopendir(dir_stream_fd);
    if (!dir) {
        close(dir_stream_fd);
        perror(node->dir->src);
        mark_traversal_error();
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;

        if (snprintf(src_path, sizeof(src_path), "%s/%s", node->dir->src, entry->d_name) >= (int)sizeof(src_path)) {
            fprintf(stderr, "Source path too long: %s/%s\n", node->dir->src, entry->d_name);
            mark_traversal_error();
            continue;
        }
        if (snprintf(dst_path, sizeof(dst_path), "%s/%s", node->dir->dst, entry->d_name) >= (int)sizeof(dst_path)) {
            fprintf(stderr, "Target path too long: %s/%s\n", node->dir->dst, entry->d_name);
            mark_traversal_error();
            continue;
        }

        if (path_is_same_or_child(g_dst_root, src_path)) {
            continue;
        }

        if (fstatat(node->dir->src_fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            perror(src_path);
            mark_traversal_error();
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            int child_src_fd = open_verified_source_dir(node->dir->src_fd, entry->d_name, src_path, &st);
            int child_dst_fd;
            dir_handle_t *child_dir;

            if (child_src_fd < 0) {
                mark_traversal_error();
                continue;
            }
            child_dst_fd = open_or_create_target_dir(node->dir->dst_fd,
                                                     entry->d_name,
                                                     dst_path,
                                                     st.st_mode & 07777);
            if (child_dst_fd < 0) {
                close(child_src_fd);
                mark_traversal_error();
                continue;
            }
            child_dir = dir_handle_create(src_path, dst_path, child_src_fd, child_dst_fd);
            if (!child_dir) {
                close(child_src_fd);
                close(child_dst_fd);
                mark_traversal_error();
                continue;
            }
            pthread_mutex_lock(&g_dir_lock);
            if (push_dir_locked(child_dir, node->depth + 1) != 0) {
                dir_handle_release(child_dir);
                mark_traversal_error();
            }
            pthread_mutex_unlock(&g_dir_lock);
        } else if (S_ISREG(st.st_mode)) {
            if (process_file(node->dir, entry->d_name, src_path, dst_path) != 0) {
                mark_traversal_error();
            }
        }
    }

    closedir(dir);
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
        dir_handle_release(node->dir);
        free(node);

        pthread_mutex_lock(&g_dir_lock);
        g_dir_active--;
        if (!g_dir_head && g_dir_active == 0) {
            g_dir_done = 1;
            pthread_cond_broadcast(&g_dir_cond);
            pthread_cond_broadcast(&g_dir_space_cond);
        }
        pthread_mutex_unlock(&g_dir_lock);
    }
}

int traversal_start(const char *src_dir, const char *dst_dir) {
    int i;
    int src_root_fd = -1;
    int dst_root_fd = -1;
    dir_handle_t *root_dir = NULL;

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

    src_root_fd = open(g_src_root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (src_root_fd < 0) {
        perror(g_src_root);
        return -1;
    }
    dst_root_fd = open(g_dst_root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (dst_root_fd < 0) {
        perror(g_dst_root);
        close(src_root_fd);
        return -1;
    }
    root_dir = dir_handle_create(g_src_root, g_dst_root, src_root_fd, dst_root_fd);
    if (!root_dir) {
        close(src_root_fd);
        close(dst_root_fd);
        return -1;
    }

    g_traversal_workers = env_int_or_default("DIRECT_COPY_TRAVERSAL_WORKERS", 8, 1, 128);
    g_max_queued_dirs = env_int_or_default("DIRECT_COPY_MAX_QUEUED_DIRS", 65536, 64, 16777216);
    g_threads = calloc((size_t)g_traversal_workers, sizeof(*g_threads));
    if (!g_threads) {
        perror("calloc");
        dir_handle_release(root_dir);
        return -1;
    }

    pthread_mutex_lock(&g_dir_lock);
    g_dir_head = NULL;
    g_dir_tail = NULL;
    g_dir_queue_depth = 0;
    g_dir_active = 0;
    g_dir_done = 0;
    if (push_dir_locked(root_dir, 0) != 0) {
        pthread_mutex_unlock(&g_dir_lock);
        dir_handle_release(root_dir);
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
            pthread_cond_broadcast(&g_dir_space_cond);
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
