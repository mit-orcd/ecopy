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
#include "copy_policy.h"
#include "verify.h"
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
            if (frc == 0 && verify_metadata_enabled()) {
                frc = verify_metadata_path(g_finalize_dirs[idx_local].dst,
                                           &g_finalize_dirs[idx_local].src_st, 1);
            }
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

    /*
     * Remote SETMETA is processed by the server apply pool. Drain all file and
     * mkdir work once before finalization, then drain after each depth group:
     * children must finish before their parent receives its final timestamp.
     */
    if (sshx_active() && sshx_barrier(0) != 0) {
        return -1;
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

        if (sshx_active() && sshx_barrier(0) != 0) {
            return -1;
        }

        start = end;
    }

    return 0;
}

/*
 * Both destination backends use the same traversal batch. SSH resolves the
 * destination states with one protocol request; local filesystems use fstatat
 * per entry, but still avoid duplicate source stats and per-file queue locks.
 */
#define FILE_STAT_BATCH 512

typedef struct {
    char name[256];
    struct stat st;
} file_entry_t;

/*
 * Per-directory stat scratch shared by both backends. These arrays are large
 * enough that malloc/free per directory caused mmap/munmap and page-fault
 * churn. Each traversal worker therefore keeps one lazily allocated set.
 * Fresh destinations need only the source batch; SSH additionally needs the
 * names array for its bulk request.
 */
typedef struct {
    file_entry_t *batch;
    const char **names;
    int *present;
    struct stat *dst_st;
} file_scratch_t;

static __thread file_scratch_t g_file_scratch;

static file_scratch_t *file_scratch_get(int remote)
{
    file_scratch_t *s = &g_file_scratch;
    int incremental = !copy_policy_destination_fresh();

    if (!s->batch) {
        s->batch = malloc(sizeof(*s->batch) * FILE_STAT_BATCH);
        if (!s->batch) {
            perror("malloc");
            return NULL;
        }
    }
    if (incremental && !s->present) {
        s->present = malloc(sizeof(*s->present) * FILE_STAT_BATCH);
        s->dst_st = malloc(sizeof(*s->dst_st) * FILE_STAT_BATCH);
        if (!s->present || !s->dst_st) {
            perror("malloc");
            return NULL;
        }
    }
    if (incremental && remote && !s->names) {
        s->names = malloc(sizeof(*s->names) * FILE_STAT_BATCH);
        if (!s->names) {
            perror("malloc");
            return NULL;
        }
    }
    return s;
}

static int stat_destination_batch(dir_handle_t *handle,
                                  const dir_node_t *node,
                                  file_scratch_t *s,
                                  int n,
                                  int remote)
{
    int rc = 0;

    if (copy_policy_destination_fresh()) {
        return 0;
    }
    if (remote) {
        for (int i = 0; i < n; i++) {
            s->names[i] = s->batch[i].name;
        }
        if (sshx_stat_bulk(node->dst, s->names, n, s->present, s->dst_st) != 0) {
            fprintf(stderr, "Bulk stat failed under %s\n", node->dst);
            return -1;
        }
        return 0;
    }

    for (int i = 0; i < n; i++) {
        if (fstatat(handle->dst_fd, s->batch[i].name, &s->dst_st[i],
                    AT_SYMLINK_NOFOLLOW) == 0) {
            s->present[i] = 1;
        } else if (errno == ENOENT) {
            s->present[i] = 0;
        } else {
            char dst_path[PATH_MAX];
            if (join_path(dst_path, sizeof(dst_path), node->dst,
                          s->batch[i].name) == 0) {
                perror(dst_path);
            } else {
                perror(node->dst);
            }
            s->present[i] = -1;
            rc = -1;
        }
    }
    return rc;
}

static int flush_file_batch(dir_handle_t *handle,
                            const dir_node_t *node,
                            file_scratch_t *s,
                            int n,
                            int remote)
{
    file_entry_t *batch = s->batch;
    workers_batch_item_t enqueue_items[FILE_STAT_BATCH];
    int enqueue_count = 0;
    int rc;

    if (n == 0) {
        return 0;
    }
    rc = stat_destination_batch(handle, node, s, n, remote);
    if (rc != 0 && remote) {
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

        /* Fresh destination: everything is new; skip the existence check. */
        if (!copy_policy_destination_fresh() && s->present[i] < 0) {
            continue;
        }
        if (!copy_policy_destination_fresh() && s->present[i]) {
            if (!S_ISREG(s->dst_st[i].st_mode)) {
                fprintf(stderr, "Target exists but is not a regular file: %s\n", dst_path);
                rc = -1;
                continue;
            }
            if (same_size_and_mtime(&batch[i].st, &s->dst_st[i])) {
                int meta_rc = remote
                                  ? sshx_setmeta(dst_path, &batch[i].st, 0)
                                  : preserve_path_metadata_at(handle->dst_fd,
                                                              batch[i].name,
                                                              dst_path,
                                                              &batch[i].st,
                                                              S_IFREG);
                if (meta_rc != 0) {
                    rc = -1;
                    continue;
                }
                stats_add_skipped_bytes((uint64_t)batch[i].st.st_size);
                stats_inc_files_skipped();
                if (verify_queue_file(src_path, dst_path, &batch[i].st, 1) != 0) {
                    fprintf(stderr, "ecopy: unable to queue verification for %s\n",
                            dst_path);
                    rc = -1;
                }
                continue;
            }
        }

        stats_add_planned_copy_bytes((uint64_t)batch[i].st.st_size);
        enqueue_items[enqueue_count].name = batch[i].name;
        enqueue_items[enqueue_count].src_st = &batch[i].st;
        enqueue_count++;
    }

    if (workers_enqueue_batch(handle, enqueue_items,
                              (size_t)enqueue_count) != 0) {
        rc = -1;
    }
    return rc;
}

static void process_dir_entries(dir_handle_t *handle,
                                const dir_node_t *node,
                                DIR *dir,
                                int remote)
{
    struct dirent *entry;
    file_scratch_t *s = file_scratch_get(remote);
    int n = 0;
    int saw_file = 0;

    if (!s) {
        mark_traversal_error();
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        struct stat st;
        char src_path[PATH_MAX], dst_path[PATH_MAX];

        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;

        if (!remote) {
            if (join_path(src_path, sizeof(src_path), node->src,
                          entry->d_name) != 0) {
                fprintf(stderr, "Source path too long: %s/%s\n",
                        node->src, entry->d_name);
                mark_traversal_error();
                continue;
            }
            if (path_is_same_or_child(g_dst_root, src_path)) {
                continue;
            }
        }

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
            if (strlen(entry->d_name) >= sizeof(s->batch[0].name)) {
                fprintf(stderr, "Name too long: %s\n", entry->d_name);
                mark_traversal_error();
                continue;
            }
            stats_inc_files_seen();
            saw_file = 1;
            snprintf(s->batch[n].name, sizeof(s->batch[n].name), "%s", entry->d_name);
            s->batch[n].st = st;
            n++;
            if (n == FILE_STAT_BATCH) {
                if (flush_file_batch(handle, node, s, n, remote) != 0) {
                    mark_traversal_error();
                }
                n = 0;
            }
        }
    }

    if (flush_file_batch(handle, node, s, n, remote) != 0) {
        mark_traversal_error();
    }

    /*
     * A directory that holds at least one file is materialized by that file's
     * PUTFILE/OPEN (both mkdir -p their parent), so the explicit MKDIR would be
     * a redundant round trip. Only send it for directories with no files
     * (empty leaves and directories that hold only subdirectories), which
     * guarantees empty subtrees are still created. The finalize SETMETA later
     * fixes the mode/times of every directory regardless of how it was made.
     */
    if (remote && !saw_file) {
        if (sshx_mkdir(node->dst, node->src_st.st_mode & 07777) != 0) {
            perror(node->dst);
            mark_traversal_error();
        }
    }
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
        /*
         * Remote destination: no local descriptor. The directory is created
         * lazily by its first file (PUTFILE/OPEN both mkdir -p their parent),
         * so we only send an explicit MKDIR for directories that turn out to
         * hold no files -- see process_dir_entries_remote. There is no local
         * destination fd.
         */
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

    process_dir_entries(handle, node, dir, sshx_active());
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
