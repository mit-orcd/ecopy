/*
 * dirwalk.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * See dirwalk.h. The queue holds exact-length heap paths rather than PATH_MAX
 * arrays: trees with tens of millions of directories otherwise pin ~8 KiB per
 * queued directory, which is what once OOM-killed ecopy on such trees.
 */

#define _GNU_SOURCE
#include "compat.h"
#include "dirwalk.h"
#include "path_utils.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef ECOPY_HAVE_GETDENTS64
#include <sys/syscall.h>
#endif

/* ------------------------------------------------------------------------ */
/* Walk state                                                                */

static dirwalk_ops_t g_ops;
static dirwalk_cfg_t g_cfg;
static pthread_t *g_threads = NULL;
static int g_thread_count = 0;

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static dirwalk_node_t *g_head = NULL;
static dirwalk_node_t *g_tail = NULL;
static int g_active = 0;
static int g_done = 0;

static int stop_requested(void)
{
    return g_cfg.stop && atomic_load_explicit(g_cfg.stop, memory_order_relaxed);
}

static void report_error(const char *path)
{
    if (g_ops.error) {
        g_ops.error(path);
    }
}

/* Caller holds g_lock. */
static int push_locked(const char *path, size_t path_len, const struct stat *st, int depth)
{
    dirwalk_node_t *n = malloc(sizeof(*n) + path_len + 1);
    if (!n) {
        perror("malloc");
        return -1;
    }
    memcpy(n->path, path, path_len + 1);
    n->st = *st;
    n->depth = depth;
    n->next = NULL;
    if (g_tail) {
        g_tail->next = n;
    } else {
        g_head = n;
    }
    g_tail = n;
    pthread_cond_signal(&g_cond);
    return 0;
}

/* Caller holds g_lock. */
static dirwalk_node_t *pop_locked(void)
{
    dirwalk_node_t *n = g_head;
    if (!n) {
        return NULL;
    }
    g_head = n->next;
    if (!g_head) {
        g_tail = NULL;
    }
    n->next = NULL;
    return n;
}

/* ------------------------------------------------------------------------ */
/* Entry stream: raw getdents64 into a per-thread buffer, else libc readdir  */

#ifdef ECOPY_HAVE_GETDENTS64
struct dirwalk_dirent64 {
    uint64_t       d_ino;
    int64_t        d_off;
    unsigned short d_reclen;
    unsigned char  d_type;
    char           d_name[];
};
#endif

typedef struct {
    int fd;          /* getdents stream fd; -1 => libc fallback */
    DIR *dirp;       /* libc fallback */
    char *buf;       /* borrowed from the thread's scratch */
    size_t buf_cap;
    size_t buf_len;
    size_t buf_off;
} dirreader_t;

/* Lazily allocated per walker thread, released at thread exit. NULL after an
 * allocation failure, in which case the thread silently uses readdir. */
static __thread char *t_getdents_buf = NULL;
static __thread size_t t_getdents_cap = 0;

static void thread_scratch_free(void)
{
    free(t_getdents_buf);
    t_getdents_buf = NULL;
    t_getdents_cap = 0;
}

/* Takes ownership of stream_fd on success; the caller keeps it on failure. */
static int dirreader_open(dirreader_t *rd, int stream_fd)
{
    rd->fd = -1;
    rd->dirp = NULL;
    rd->buf = NULL;
    rd->buf_cap = rd->buf_len = rd->buf_off = 0;

    if (g_cfg.getdents_buf > 0) {
        if (!t_getdents_buf) {
            t_getdents_buf = malloc(g_cfg.getdents_buf);
            if (t_getdents_buf) {
                t_getdents_cap = g_cfg.getdents_buf;
            }
        }
        if (t_getdents_buf) {
            rd->fd = stream_fd;
            rd->buf = t_getdents_buf;
            rd->buf_cap = t_getdents_cap;
            return 0;
        }
    }
    rd->dirp = fdopendir(stream_fd);
    return rd->dirp ? 0 : -1;
}

/* 1 = entry (*name_out valid until the next call), 0 = end, -1 = error. */
static int dirreader_next(dirreader_t *rd, const char **name_out)
{
    if (rd->fd < 0) {
        struct dirent *de = readdir(rd->dirp);
        if (!de) {
            return 0;
        }
        *name_out = de->d_name;
        return 1;
    }
#ifdef ECOPY_HAVE_GETDENTS64
    if (rd->buf_off >= rd->buf_len) {
        long n = syscall(SYS_getdents64, rd->fd, rd->buf, rd->buf_cap);
        if (n < 0) {
            perror("getdents64");
            return -1;
        }
        if (n == 0) {
            return 0;
        }
        rd->buf_len = (size_t)n;
        rd->buf_off = 0;
    }
    {
        struct dirwalk_dirent64 *d =
            (struct dirwalk_dirent64 *)(void *)(rd->buf + rd->buf_off);
        if (d->d_reclen == 0) {
            return 0; /* defensive: never advance by zero */
        }
        rd->buf_off += d->d_reclen;
        *name_out = d->d_name;
        return 1;
    }
#else
    return -1; /* dirreader_open never selects the raw path here */
#endif
}

static void dirreader_close(dirreader_t *rd)
{
    if (rd->fd >= 0) {
        close(rd->fd);
        rd->fd = -1;
    } else if (rd->dirp) {
        closedir(rd->dirp);
        rd->dirp = NULL;
    }
}

/* ------------------------------------------------------------------------ */
/* Per-directory processing                                                 */

static int same_entry(const struct stat *a, const struct stat *b)
{
    return a->st_dev == b->st_dev && a->st_ino == b->st_ino &&
           (a->st_mode & S_IFMT) == (b->st_mode & S_IFMT);
}

/*
 * Open a directory found earlier by lstat and make sure it is still the same
 * object, so a symlink swapped in between discovery and processing is refused
 * rather than followed. Refreshes *st with the fstat of the open descriptor.
 */
static int open_verified_dir(const char *path, struct stat *st)
{
    struct stat opened;
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        perror(path);
        return -1;
    }
    if (fstat(fd, &opened) != 0) {
        perror(path);
        close(fd);
        return -1;
    }
    if (!S_ISDIR(opened.st_mode) || !same_entry(st, &opened)) {
        fprintf(stderr, "Directory changed during traversal: %s\n", path);
        close(fd);
        errno = ESTALE;
        return -1;
    }
    *st = opened;
    return fd;
}

static void process_node(dirwalk_node_t *node)
{
    void *ctx = NULL;
    dirreader_t rd;
    const char *name;
    int dir_fd, stream_fd, rc;
    size_t path_len;

    dir_fd = open_verified_dir(node->path, &node->st);
    if (dir_fd < 0) {
        report_error(node->path);
        return;
    }
    if (g_ops.dir_begin(node, dir_fd, &ctx) != 0) {
        close(dir_fd);
        return;
    }

    /*
     * A private dup for enumeration: the reader consumes its file offset
     * while dir_fd keeps serving offset-independent fstatat/openat.
     */
    stream_fd = dup(dir_fd);
    if (stream_fd < 0 || dirreader_open(&rd, stream_fd) != 0) {
        perror(node->path);
        if (stream_fd >= 0) {
            close(stream_fd);
        }
        report_error(node->path);
        g_ops.dir_end(node, dir_fd, ctx, -1);
        close(dir_fd);
        return;
    }

    path_len = strlen(node->path);
    while ((rc = dirreader_next(&rd, &name)) == 1) {
        struct stat st;

        if (stop_requested()) {
            break;
        }
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }
        if (fstatat(dir_fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            perror(name);
            report_error(node->path);
            continue;
        }
        if (g_ops.entry(node, dir_fd, ctx, name, &st) == DIRWALK_DESCEND && S_ISDIR(st.st_mode)) {
            char child[PATH_MAX];
            size_t name_len = strlen(name);

            if (path_join_fast(node->path, path_len, name, name_len, child, sizeof(child)) != 0) {
                fprintf(stderr, "Path too long: %s/%s\n", node->path, name);
                report_error(node->path);
                continue;
            }
            pthread_mutex_lock(&g_lock);
            if (push_locked(child, path_len + 1 + name_len, &st, node->depth + 1) != 0) {
                report_error(child);
            }
            pthread_mutex_unlock(&g_lock);
        }
    }
    dirreader_close(&rd);
    if (rc < 0) {
        report_error(node->path);
    }

    g_ops.dir_end(node, dir_fd, ctx, rc < 0 ? -1 : 0);
    close(dir_fd);
}

static void *worker_main(void *arg)
{
    int index = (int)(intptr_t)arg;

    if (g_ops.thread_start) {
        g_ops.thread_start(index);
    }

    for (;;) {
        dirwalk_node_t *node;

        pthread_mutex_lock(&g_lock);
        for (;;) {
            if (stop_requested()) {
                pthread_mutex_unlock(&g_lock);
                goto out;
            }
            node = pop_locked();
            if (node) {
                g_active++;
                break;
            }
            if (g_done) {
                pthread_mutex_unlock(&g_lock);
                goto out;
            }
            pthread_cond_wait(&g_cond, &g_lock);
        }
        pthread_mutex_unlock(&g_lock);

        process_node(node);
        free(node);

        pthread_mutex_lock(&g_lock);
        g_active--;
        if (!g_head && g_active == 0) {
            g_done = 1;
            pthread_cond_broadcast(&g_cond);
        }
        pthread_mutex_unlock(&g_lock);
    }

out:
    thread_scratch_free();
    if (g_ops.thread_end) {
        g_ops.thread_end();
    }
    return NULL;
}

/* ------------------------------------------------------------------------ */
/* Public API                                                               */

int dirwalk_start(const char *root, const struct stat *root_st,
                  const dirwalk_cfg_t *cfg, const dirwalk_ops_t *ops)
{
    int i;

    if (!root || !root_st || !cfg || !ops || !ops->dir_begin || !ops->entry ||
        !ops->dir_end || cfg->threads < 1) {
        errno = EINVAL;
        return -1;
    }
    if (!S_ISDIR(root_st->st_mode)) {
        fprintf(stderr, "Not a directory: %s\n", root);
        errno = ENOTDIR;
        return -1;
    }
    g_ops = *ops;
    g_cfg = *cfg;

    g_threads = calloc((size_t)cfg->threads, sizeof(*g_threads));
    if (!g_threads) {
        perror("calloc");
        return -1;
    }
    g_thread_count = cfg->threads;

    pthread_mutex_lock(&g_lock);
    g_head = g_tail = NULL;
    g_active = 0;
    g_done = 0;
    if (push_locked(root, strlen(root), root_st, 0) != 0) {
        pthread_mutex_unlock(&g_lock);
        free(g_threads);
        g_threads = NULL;
        return -1;
    }
    pthread_mutex_unlock(&g_lock);

    for (i = 0; i < g_thread_count; i++) {
        if (pthread_create(&g_threads[i], NULL, worker_main, (void *)(intptr_t)i) != 0) {
            perror("pthread_create");
            dirwalk_request_stop();
            while (--i >= 0) {
                pthread_join(g_threads[i], NULL);
            }
            pthread_mutex_lock(&g_lock);
            while (g_head) {
                free(pop_locked());
            }
            pthread_mutex_unlock(&g_lock);
            free(g_threads);
            g_threads = NULL;
            g_thread_count = 0;
            return -1;
        }
    }
    return 0;
}

void dirwalk_wait(void)
{
    int i;
    for (i = 0; i < g_thread_count; i++) {
        pthread_join(g_threads[i], NULL);
    }
    free(g_threads);
    g_threads = NULL;
    g_thread_count = 0;
}

void dirwalk_request_stop(void)
{
    pthread_mutex_lock(&g_lock);
    g_done = 1;
    pthread_cond_broadcast(&g_cond);
    pthread_mutex_unlock(&g_lock);
}

/* ------------------------------------------------------------------------ */
/* Deepest-first grouped pass                                                */

typedef struct {
    size_t end;
    size_t next;
    int failed;
    int bind_seq;
    pthread_mutex_t lock;
    int (*fn)(size_t i);
    void (*thread_start)(int index);
} group_ctx_t;

static void *group_worker(void *arg)
{
    group_ctx_t *ctx = (group_ctx_t *)arg;

    if (ctx->thread_start) {
        pthread_mutex_lock(&ctx->lock);
        int idx = ctx->bind_seq++;
        pthread_mutex_unlock(&ctx->lock);
        ctx->thread_start(idx);
    }

    for (;;) {
        size_t i;

        pthread_mutex_lock(&ctx->lock);
        if (ctx->next >= ctx->end) {
            pthread_mutex_unlock(&ctx->lock);
            break;
        }
        i = ctx->next++;
        pthread_mutex_unlock(&ctx->lock);

        if (ctx->fn(i) != 0) {
            pthread_mutex_lock(&ctx->lock);
            ctx->failed = 1;
            pthread_mutex_unlock(&ctx->lock);
        }
    }
    return NULL;
}

int dirwalk_depth_groups(size_t n, int threads,
                         int (*depth_of)(size_t i),
                         int (*fn)(size_t i),
                         int (*between_groups)(void),
                         void (*thread_start)(int index))
{
    size_t start;

    if (threads < 1) {
        threads = 1;
    }

    for (start = 0; start < n; ) {
        size_t end = start + 1;
        int depth = depth_of(start);
        int count, i, rc = 0;
        pthread_t *tids;
        group_ctx_t ctx;

        while (end < n && depth_of(end) == depth) {
            end++;
        }

        count = threads;
        if ((size_t)count > end - start) {
            count = (int)(end - start);
        }
        tids = calloc((size_t)count, sizeof(*tids));
        if (!tids) {
            perror("calloc");
            return -1;
        }

        ctx.end = end;
        ctx.next = start;
        ctx.failed = 0;
        ctx.bind_seq = 0;
        ctx.fn = fn;
        ctx.thread_start = thread_start;
        pthread_mutex_init(&ctx.lock, NULL);

        for (i = 0; i < count; i++) {
            if (pthread_create(&tids[i], NULL, group_worker, &ctx) != 0) {
                perror("pthread_create");
                ctx.failed = 1;
                count = i;
                break;
            }
        }
        for (i = 0; i < count; i++) {
            pthread_join(tids[i], NULL);
        }
        if (ctx.failed) {
            rc = -1;
        }
        pthread_mutex_destroy(&ctx.lock);
        free(tids);

        if (rc != 0) {
            return -1;
        }
        if (between_groups && between_groups() != 0) {
            return -1;
        }
        start = end;
    }
    return 0;
}
