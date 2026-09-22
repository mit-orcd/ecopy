/*
 * traversal.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Source-tree traversal for a copy: the shared dirwalk walker enumerates the
 * tree; the callbacks here create target directories, stat destinations in
 * batches, hand regular files to the copy workers, recreate symlinks, and
 * record every directory so its metadata can be finalized deepest-first once
 * all of its contents are in place.
 */

#define _GNU_SOURCE
#include "compat.h"
#include "traversal.h"
#include "env_util.h"
#include "dirwalk.h"
#include "path_utils.h"
#include "stats.h"
#include "fs_util.h"
#include "copy_policy.h"
#include "verify.h"
#include "workers.h"
#include "ssh_transport.h"
#include "hardlinks.h"
#include "shutdown.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

/*
 * Finalize records carry heap-allocated, exact-length path strings rather than
 * PATH_MAX arrays: the list lives until the final metadata pass, and with tens
 * of millions of directories ~8 KiB each is what once OOM-killed ecopy.
 */
typedef struct dir_record {
    char *src;
    char *dst;
    struct stat src_st;
    int depth;
} dir_record_t;

static int g_traversal_workers = 0;
static int g_status = 0;
static pthread_mutex_t g_status_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_src_root[PATH_MAX];
static char g_dst_root[PATH_MAX];
static size_t g_src_root_len;

static pthread_mutex_t g_finalize_lock = PTHREAD_MUTEX_INITIALIZER;
static dir_record_t *g_finalize_dirs = NULL;
static size_t g_finalize_dir_count = 0;
static size_t g_finalize_dir_cap = 0;

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

/* Map a source directory path to its destination: replace the root prefix. */
static int dst_path_for(const char *src, char *out, size_t out_sz)
{
    const char *suffix = src + g_src_root_len; /* "" for the root, else "/..." */
    if (snprintf(out, out_sz, "%s%s", g_dst_root, suffix) >= (int)out_sz) {
        fprintf(stderr, "Target path too long: %s%s\n", g_dst_root, suffix);
        return -1;
    }
    return 0;
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

/* ------------------------------------------------------------------------ */
/* Finalize list                                                            */

/* Release every finalize record and the array. Caller holds g_finalize_lock. */
static void free_finalize_dirs_locked(void)
{
    for (size_t i = 0; i < g_finalize_dir_count; i++) {
        free(g_finalize_dirs[i].src);
        free(g_finalize_dirs[i].dst);
    }
    free(g_finalize_dirs);
    g_finalize_dirs = NULL;
    g_finalize_dir_count = 0;
    g_finalize_dir_cap = 0;
}

static int record_directory_for_finalize(const char *src,
                                         const char *dst,
                                         const struct stat *src_st,
                                         int depth)
{
    dir_record_t *new_dirs;
    dir_record_t *rec;
    char *src_copy = strdup(src);
    char *dst_copy = strdup(dst);

    if (!src_copy || !dst_copy) {
        free(src_copy);
        free(dst_copy);
        perror("strdup");
        return -1;
    }

    pthread_mutex_lock(&g_finalize_lock);
    if (g_finalize_dir_count == g_finalize_dir_cap) {
        size_t new_cap = g_finalize_dir_cap ? g_finalize_dir_cap * 2 : 1024;
        new_dirs = realloc(g_finalize_dirs, new_cap * sizeof(*g_finalize_dirs));
        if (!new_dirs) {
            pthread_mutex_unlock(&g_finalize_lock);
            free(src_copy);
            free(dst_copy);
            perror("realloc");
            return -1;
        }
        g_finalize_dirs = new_dirs;
        g_finalize_dir_cap = new_cap;
    }

    rec = &g_finalize_dirs[g_finalize_dir_count];
    rec->src = src_copy;
    rec->dst = dst_copy;
    rec->src_st = *src_st;
    rec->depth = depth;
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

static int finalize_depth_of(size_t i)
{
    return g_finalize_dirs[i].depth;
}

static int finalize_one(size_t i)
{
    const dir_record_t *rec = &g_finalize_dirs[i];
    int frc;

    if (sshx_active()) {
        frc = sshx_setmeta(rec->dst, &rec->src_st, 1);
    } else {
        frc = preserve_path_metadata(rec->dst, &rec->src_st);
    }
    if (frc == 0 && verify_metadata_enabled()) {
        frc = verify_queue_directory(rec->src, rec->dst, &rec->src_st);
    }
    return frc;
}

/*
 * Remote SETMETA is processed by the server apply pool, and children may have
 * been written by any server in the pool, so drain every connection between
 * depth groups: children must finish before their parent gets its final
 * timestamp.
 */
static int finalize_barrier(void)
{
    return sshx_active() ? sshx_barrier_all(0) : 0;
}

/* Spread finalize SETMETA across the SSH connection pool (no-op locally). */
static void finalize_thread_start(int index)
{
    sshx_bind_thread(index);
}

static int finalize_directories_parallel(void)
{
    if (g_finalize_dir_count == 0) {
        return 0;
    }
    if (finalize_barrier() != 0) {
        return -1;
    }
    qsort(g_finalize_dirs, g_finalize_dir_count, sizeof(*g_finalize_dirs),
          dir_record_cmp_desc_depth);
    return dirwalk_depth_groups(g_finalize_dir_count, g_traversal_workers,
                                finalize_depth_of, finalize_one,
                                finalize_barrier, finalize_thread_start);
}

/* ------------------------------------------------------------------------ */
/* Per-directory file batching                                              */

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

/* Release this worker's scratch buffers (called at traversal thread exit). */
static void file_scratch_free(void)
{
    file_scratch_t *s = &g_file_scratch;

    free(s->batch);
    free(s->names);
    free(s->present);
    free(s->dst_st);
    memset(s, 0, sizeof(*s));
}

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

/* State for one directory while its entries are being processed. */
typedef struct {
    dir_handle_t *handle;   /* src/dst paths and descriptors, refcounted by file tasks */
    file_scratch_t *s;
    int n;                  /* files in the current batch */
    int saw_file;
    int remote;
} trav_dir_t;

static int join_under(char *out, size_t out_sz, const char *parent, const char *name)
{
    return path_join_fast(parent, strlen(parent), name, strlen(name), out, out_sz);
}

static int stat_destination_batch(trav_dir_t *d, int n)
{
    dir_handle_t *handle = d->handle;
    file_scratch_t *s = d->s;
    int rc = 0;

    if (copy_policy_destination_fresh()) {
        return 0;
    }
    if (d->remote) {
        for (int i = 0; i < n; i++) {
            s->names[i] = s->batch[i].name;
        }
        if (sshx_stat_bulk(handle->dst, s->names, n, s->present, s->dst_st) != 0) {
            fprintf(stderr, "Bulk stat failed under %s\n", handle->dst);
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
            if (join_under(dst_path, sizeof(dst_path), handle->dst, s->batch[i].name) == 0) {
                perror(dst_path);
            } else {
                perror(handle->dst);
            }
            s->present[i] = -1;
            rc = -1;
        }
    }
    return rc;
}

static int flush_file_batch(trav_dir_t *d)
{
    dir_handle_t *handle = d->handle;
    file_entry_t *batch = d->s->batch;
    file_scratch_t *s = d->s;
    workers_batch_item_t enqueue_items[FILE_STAT_BATCH];
    int enqueue_count = 0;
    int n = d->n;
    int rc;
    uint64_t planned_bytes = 0;
    uint64_t skipped_bytes = 0;

    d->n = 0;
    if (n == 0) {
        return 0;
    }
    rc = stat_destination_batch(d, n);
    if (rc != 0 && d->remote) {
        return -1;
    }

    for (int i = 0; i < n; i++) {
        char src_path[PATH_MAX], dst_path[PATH_MAX];

        if (join_under(src_path, sizeof(src_path), handle->src, batch[i].name) != 0 ||
            join_under(dst_path, sizeof(dst_path), handle->dst, batch[i].name) != 0) {
            fprintf(stderr, "Path too long under %s\n", handle->src);
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
                int meta_rc = d->remote
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
                /*
                 * Count only the data that would have crossed the wire, not the
                 * logical size: a skipped sparse file must not inflate the
                 * skipped/completed ("payload") totals by its hole size (e.g. a
                 * 900 TiB sparse image holding 1 GiB of real data). This mirrors
                 * the allocated-bytes weight used for the copied payload.
                 */
                off_t skip_alloc = (off_t)batch[i].st.st_blocks * 512;
                off_t skip_payload = batch[i].st.st_size < skip_alloc
                                         ? batch[i].st.st_size : skip_alloc;
                if (skip_payload < 0) {
                    skip_payload = 0;
                }
                skipped_bytes += (uint64_t)skip_payload;
                stats_inc_files_skipped();
                /* Skipped files already exist at their final path (durable). */
                if (verify_queue_file(src_path, dst_path, &batch[i].st, 1, 1) != 0) {
                    fprintf(stderr, "ecopy: unable to queue verification for %s\n",
                            dst_path);
                    rc = -1;
                }
                continue;
            }
        }

        planned_bytes += (uint64_t)batch[i].st.st_size;
        enqueue_items[enqueue_count].name = batch[i].name;
        enqueue_items[enqueue_count].src_st = &batch[i].st;
        enqueue_count++;
    }

    /* One locked add per batch instead of one per file (8 walkers share the lock). */
    if (planned_bytes > 0) {
        stats_add_planned_copy_bytes(planned_bytes);
    }
    if (skipped_bytes > 0) {
        stats_add_skipped_bytes(skipped_bytes);
    }
    if (workers_enqueue_batch(handle, enqueue_items, (size_t)enqueue_count) != 0) {
        rc = -1;
    }
    return rc;
}

/*
 * Recreate a symlink without following it: read the link target and create the
 * same link at the destination (local symlinkat / remote MSG_SYMLINK). The
 * target string is preserved verbatim, matching cp -d semantics.
 */
static void copy_symlink_entry(trav_dir_t *d, const char *name, const struct stat *st)
{
    dir_handle_t *handle = d->handle;
    char target[PATH_MAX];
    ssize_t len = readlinkat(handle->src_fd, name, target, sizeof(target) - 1);
    if (len < 0) {
        perror(name);
        mark_traversal_error();
        return;
    }
    if (len >= (ssize_t)sizeof(target) - 1) {
        fprintf(stderr, "Symlink target too long under %s/%s\n", handle->src, name);
        mark_traversal_error();
        return;
    }
    target[len] = '\0';
    stats_inc_symlink_seen();

    char dst_path[PATH_MAX];
    if (join_under(dst_path, sizeof(dst_path), handle->dst, name) != 0) {
        fprintf(stderr, "Path too long under %s\n", handle->dst);
        mark_traversal_error();
        return;
    }

    if (d->remote) {
        if (sshx_symlink(dst_path, target, st) != 0) {
            perror(dst_path);
            mark_traversal_error();
            return;
        }
    } else if (symlink_recreate_at(handle->dst_fd, name, dst_path, target, st) != 0) {
        mark_traversal_error();
        return;
    }
    stats_inc_symlink_created();
}

/* ------------------------------------------------------------------------ */
/* dirwalk callbacks                                                        */

static void walk_thread_start(int index)
{
    /* Spread mkdir/stat/symlink work across the SSH connection pool. */
    sshx_bind_thread(index);
}

static int walk_dir_begin(const dirwalk_node_t *node, int dir_fd, void **ctx)
{
    trav_dir_t *d;
    char dst[PATH_MAX];
    struct stat st = node->st;
    int src_fd, dst_fd;
    int remote = sshx_active();

    /* Directories get the forced ownership too, not only their files. */
    copy_policy_apply_id_override(&st);

    if (dst_path_for(node->path, dst, sizeof(dst)) != 0) {
        mark_traversal_error();
        return -1;
    }
    d = calloc(1, sizeof(*d));
    if (!d) {
        perror("calloc");
        mark_traversal_error();
        return -1;
    }
    d->remote = remote;
    d->s = file_scratch_get(remote);
    if (!d->s) {
        free(d);
        mark_traversal_error();
        return -1;
    }

    if (remote) {
        /*
         * Remote destination: no local descriptor. The directory is created
         * lazily by its first file (PUTFILE/OPEN both mkdir -p their parent),
         * so an explicit MKDIR is sent only for directories that turn out to
         * hold no files -- see walk_dir_end.
         */
        stats_inc_dirs_created();
        dst_fd = -1;
    } else {
        dst_fd = open_or_create_target_dir_path(dst, st.st_mode & 07777);
        if (dst_fd < 0) {
            free(d);
            mark_traversal_error();
            return -1;
        }
    }

    /* The handle outlives this directory's enumeration (file tasks retain it),
     * so it needs its own descriptor; the walker closes dir_fd itself. */
    src_fd = dup(dir_fd);
    if (src_fd < 0) {
        perror(node->path);
        if (dst_fd >= 0) close(dst_fd);
        free(d);
        mark_traversal_error();
        return -1;
    }
    d->handle = dir_handle_create(node->path, dst, src_fd, dst_fd);
    if (!d->handle) {
        close(src_fd);
        if (dst_fd >= 0) close(dst_fd);
        free(d);
        mark_traversal_error();
        return -1;
    }

    stats_inc_dirs_seen();
    if (record_directory_for_finalize(node->path, dst, &st, node->depth) != 0) {
        dir_handle_release(d->handle);
        free(d);
        mark_traversal_error();
        return -1;
    }

    *ctx = d;
    return 0;
}

static int walk_entry(const dirwalk_node_t *node, int dir_fd, void *ctx,
                      const char *name, const struct stat *st_in)
{
    trav_dir_t *d = ctx;
    struct stat st = *st_in;
    (void)dir_fd;

    copy_policy_apply_id_override(&st);

    if (S_ISDIR(st.st_mode)) {
        if (!d->remote) {
            /* Never descend into the destination tree if it lives inside the source. */
            char child[PATH_MAX];
            if (join_under(child, sizeof(child), node->path, name) == 0 &&
                path_is_under_root(child, g_dst_root)) {
                return DIRWALK_SKIP;
            }
        }
        return DIRWALK_DESCEND;
    }
    if (S_ISLNK(st.st_mode)) {
        copy_symlink_entry(d, name, &st);
        return DIRWALK_SKIP;
    }
    if (!S_ISREG(st.st_mode)) {
        return DIRWALK_SKIP;
    }

    if (strlen(name) >= sizeof(d->s->batch[0].name)) {
        fprintf(stderr, "Name too long: %s\n", name);
        mark_traversal_error();
        return DIRWALK_SKIP;
    }
    /*
     * Hard-linked file: only the first sighting of an inode is copied; later
     * links are materialized after the copy phase (see hardlinks_replay) so
     * the data is never duplicated. Leaving saw_file untouched for a secondary
     * lets a directory that holds only secondaries still get an explicit
     * remote MKDIR, so the finalize SETMETA (and the later hard link) find it.
     */
    if (st.st_nlink > 1) {
        char dst_path[PATH_MAX];
        if (join_under(dst_path, sizeof(dst_path), d->handle->dst, name) != 0) {
            fprintf(stderr, "Path too long under %s\n", d->handle->dst);
            mark_traversal_error();
            return DIRWALK_SKIP;
        }
        hl_result_t hr = hardlinks_note(&st, dst_path);
        if (hr == HL_SECONDARY) {
            return DIRWALK_SKIP;
        }
        if (hr == HL_ERROR) {
            mark_traversal_error();
            /* fall through and copy it as a normal file (no data loss) */
        }
    }
    stats_inc_files_seen();
    d->saw_file = 1;
    snprintf(d->s->batch[d->n].name, sizeof(d->s->batch[d->n].name), "%s", name);
    d->s->batch[d->n].st = st;
    d->n++;
    if (d->n == FILE_STAT_BATCH && flush_file_batch(d) != 0) {
        mark_traversal_error();
    }
    return DIRWALK_SKIP;
}

static void walk_dir_end(const dirwalk_node_t *node, int dir_fd, void *ctx, int rc)
{
    trav_dir_t *d = ctx;
    (void)dir_fd;

    if (rc != 0) {
        mark_traversal_error();
    }
    if (flush_file_batch(d) != 0) {
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
    if (d->remote && !d->saw_file) {
        if (sshx_mkdir(d->handle->dst, node->st.st_mode & 07777) != 0) {
            perror(d->handle->dst);
            mark_traversal_error();
        }
    }
    dir_handle_release(d->handle);
    free(d);
}

static void walk_error(const char *path)
{
    (void)path;
    mark_traversal_error();
}

/* ------------------------------------------------------------------------ */
/* Public API                                                               */

int traversal_start(const char *src_dir, const char *dst_dir)
{
    struct stat root_st;
    dirwalk_cfg_t cfg;
    dirwalk_ops_t ops;

    pthread_mutex_lock(&g_status_lock);
    g_status = 0;
    pthread_mutex_unlock(&g_status_lock);
    pthread_mutex_lock(&g_finalize_lock);
    free_finalize_dirs_locked();
    pthread_mutex_unlock(&g_finalize_lock);
    if (copy_path_checked(g_src_root, sizeof(g_src_root), src_dir, "Source root") != 0 ||
        copy_path_checked(g_dst_root, sizeof(g_dst_root), dst_dir, "Target root") != 0) {
        return -1;
    }
    g_src_root_len = strlen(g_src_root);

    if (lstat(g_src_root, &root_st) != 0) {
        perror(g_src_root);
        return -1;
    }
    if (!S_ISDIR(root_st.st_mode)) {
        fprintf(stderr, "Source is not a directory: %s\n", g_src_root);
        return -1;
    }
    copy_policy_apply_id_override(&root_st);

    g_traversal_workers = env_int_or_default("DIRECT_COPY_TRAVERSAL_WORKERS", 8, 1, 128);

    memset(&cfg, 0, sizeof(cfg));
    cfg.threads = g_traversal_workers;
    cfg.stop = &g_shutdown_requested;
    /*
     * Raw getdents64 read-buffer size (bytes) per traversal worker; 0 falls
     * back to libc readdir. Default 256 KiB returns thousands of entries per
     * syscall on large directories. Clamp a nonzero value to a sane floor.
     */
#ifdef ECOPY_HAVE_GETDENTS64
    {
        int v = env_int_or_default("DIRECT_COPY_GETDENTS_BUF", 262144, 0, 64 * 1024 * 1024);
        if (v > 0 && v < 4096) v = 4096;
        cfg.getdents_buf = (size_t)v;
    }
#else
    cfg.getdents_buf = 0;
#endif

    memset(&ops, 0, sizeof(ops));
    ops.thread_start = walk_thread_start;
    ops.thread_end = file_scratch_free;
    ops.dir_begin = walk_dir_begin;
    ops.entry = walk_entry;
    ops.dir_end = walk_dir_end;
    ops.error = walk_error;

    return dirwalk_start(g_src_root, &root_st, &cfg, &ops);
}

void traversal_wait(void)
{
    dirwalk_wait();
    stats_set_traversal_done();
}

void traversal_request_stop(void)
{
    dirwalk_request_stop();
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
    free_finalize_dirs_locked();
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
