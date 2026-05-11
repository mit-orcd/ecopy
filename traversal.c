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

typedef struct dir_node {
    char src[PATH_MAX];
    char dst[PATH_MAX];
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
        return defval;
    }
    if (v < minval) {
        return minval;
    }
    if (v > maxval) {
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

static int process_file(const char *src, const char *dst)
{
    struct stat src_st, dst_st;
    if (lstat(src, &src_st) != 0) { perror(src); return -1; }
    if (!S_ISREG(src_st.st_mode)) return 0;
    stats_inc_files_seen();
    if (lstat(dst, &dst_st) == 0) {
        if (!S_ISREG(dst_st.st_mode)) {
            fprintf(stderr, "Target exists but is not a regular file: %s\n", dst);
            return -1;
        }
        if (S_ISREG(dst_st.st_mode) && same_size_and_mtime(&src_st, &dst_st)) {
            if (preserve_path_metadata(dst, &src_st) != 0) {
                return -1;
            }
            stats_inc_files_skipped();
            return 0;
        }
    } else if (errno != ENOENT) {
        perror(dst);
        return -1;
    }
    stats_add_planned_copy_bytes((uint64_t)src_st.st_size);
    if (workers_file_is_large(src_st.st_size)) return workers_enqueue_large_file(src, dst, &src_st);
    return workers_enqueue_small_file(src, dst, &src_st);
}

static int push_dir_locked(const char *src, const char *dst, int depth)
{
    dir_node_t *n = calloc(1, sizeof(*n));
    if (!n) { perror("calloc"); return -1; }

    if (copy_path_checked(n->src, sizeof(n->src), src, "Source") != 0 ||
        copy_path_checked(n->dst, sizeof(n->dst), dst, "Target") != 0) {
        free(n);
        return -1;
    }
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
    return node;
}

static void process_directory_node(dir_node_t *node)
{
    struct stat st;
    DIR *dir;
    struct dirent *entry;
    char src_path[PATH_MAX], dst_path[PATH_MAX];

    if (lstat(node->src, &st) != 0) {
        perror(node->src);
        mark_traversal_error();
        return;
    }

    stats_inc_dirs_seen();
    if (ensure_dir_exists(node->dst, st.st_mode & 07777) != 0) {
        mark_traversal_error();
        return;
    }
    if (record_directory_for_finalize(node->src, node->dst, &st, node->depth) != 0) {
        mark_traversal_error();
        return;
    }

    dir = opendir(node->src);
    if (!dir) {
        perror(node->src);
        mark_traversal_error();
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;

        if (snprintf(src_path, sizeof(src_path), "%s/%s", node->src, entry->d_name) >= (int)sizeof(src_path)) {
            fprintf(stderr, "Source path too long: %s/%s\n", node->src, entry->d_name);
            mark_traversal_error();
            continue;
        }
        if (snprintf(dst_path, sizeof(dst_path), "%s/%s", node->dst, entry->d_name) >= (int)sizeof(dst_path)) {
            fprintf(stderr, "Target path too long: %s/%s\n", node->dst, entry->d_name);
            mark_traversal_error();
            continue;
        }

        if (path_is_same_or_child(g_dst_root, src_path)) {
            continue;
        }

        if (lstat(src_path, &st) != 0) {
            perror(src_path);
            mark_traversal_error();
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            pthread_mutex_lock(&g_dir_lock);
            if (push_dir_locked(src_path, dst_path, node->depth + 1) != 0) {
                mark_traversal_error();
            }
            pthread_mutex_unlock(&g_dir_lock);
        } else if (S_ISREG(st.st_mode)) {
            if (process_file(src_path, dst_path) != 0) {
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
    if (push_dir_locked(g_src_root, g_dst_root, 0) != 0) {
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
    if (finalize_directories_parallel() != 0) {
        mark_traversal_error();
        return -1;
    }
    stats_set_finalize_done();
    pthread_mutex_lock(&g_finalize_lock);
    free(g_finalize_dirs);
    g_finalize_dirs = NULL;
    g_finalize_dir_count = 0;
    g_finalize_dir_cap = 0;
    pthread_mutex_unlock(&g_finalize_lock);
    return 0;
}

int traversal_status(void)
{
    int status;

    pthread_mutex_lock(&g_status_lock);
    status = g_status;
    pthread_mutex_unlock(&g_status_lock);
    return status;
}
