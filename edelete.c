/*
 * edelete.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Parallel tree deleter. Walks the start path with the same dirwalk engine as
 * ecopy's traversal and unlinks every non-directory (regular files, symlinks,
 * pipes, sockets, ...) that matches the filters: optionally an age in whole
 * days on atime/mtime/ctime, optionally an owner uid and/or gid. Dry-run by
 * default; --delete unlinks. Symlinks are never followed, and nothing above
 * the start path is ever touched.
 *
 * Structure mirrors ecopy: walker threads enumerate and stat; eligible paths
 * go onto bounded per-worker queues drained by a separate pool of unlink
 * threads, so a slow unlink (quota accounting, ZFS block frees) never stalls
 * the scan and the number of concurrent unlinks is an explicit knob. Each
 * directory is pinned to one unlink thread (hash of its path), because the
 * kernel serializes unlinks within a directory. After the walk, in delete
 * mode, directories that became empty are removed deepest-first, in parallel
 * per depth level with each parent's children on one thread, up to and
 * including the start path.
 */

#define _GNU_SOURCE
#include "compat.h"
#include "dirwalk.h"
#include "env_util.h"
#include "format.h"
#include "path_utils.h"
#include "shutdown.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_THREADS        16
#define DEFAULT_UNLINK_WORKERS 16
#define UNLINK_QUEUE_MAX       262144   /* backpressure cap on queued unlinks, split across workers */
#define GETDENTS_BUF_BYTES     (256 * 1024)
#define WINDOW_SECONDS         10

typedef enum { TB_ATIME, TB_MTIME, TB_CTIME } time_basis_t;

/* ------------------------------------------------------------------------ */
/* Options                                                                  */

static int g_dry_run = 1;
static int g_force = 0;
static int g_threads = DEFAULT_THREADS;
static int g_unlink_workers = DEFAULT_UNLINK_WORKERS;
static time_basis_t g_basis = TB_MTIME;
static int g_age_days = 0;
static int g_delete_all = 0;
static int g_have_uid_filter = 0;
static int g_have_gid_filter = 0;
static int g_lazy_stat = 0;   /* walker skips fstatat on files (see dirwalk_cfg_t) */
static uid_t g_filter_uid = 0;
static gid_t g_filter_gid = 0;
static time_t g_now = 0;

/* ------------------------------------------------------------------------ */
/* Counters                                                                 */

static atomic_ullong g_entries = 0;        /* everything walked: dirs + non-dirs */
static atomic_ullong g_dirs = 0;
static atomic_ullong g_files = 0;          /* non-directories */
static atomic_ullong g_would_delete = 0;
static atomic_ullong g_deleted = 0;
static atomic_ullong g_removed_dirs = 0;
static atomic_ullong g_errors = 0;

/* Sliding 10 s window of entries walked, for the live rate. */
static atomic_ullong g_window_entries = 0;
static atomic_ullong g_bucket_entries[WINDOW_SECONDS];
static atomic_int g_bucket_index = 0;
static atomic_uint g_seconds_seen = 0;
static atomic_int g_stop_status = 0;
static pthread_mutex_t g_status_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_status_cond = PTHREAD_COND_INITIALIZER;

/* Sleep up to one second, but return at once when the run ends. */
static void status_pause(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;
    pthread_mutex_lock(&g_status_lock);
    while (!atomic_load(&g_stop_status)) {
        if (pthread_cond_timedwait(&g_status_cond, &g_status_lock, &ts) == ETIMEDOUT) break;
    }
    pthread_mutex_unlock(&g_status_lock);
}

static void status_stop(void)
{
    pthread_mutex_lock(&g_status_lock);
    atomic_store(&g_stop_status, 1);
    pthread_cond_broadcast(&g_status_cond);
    pthread_mutex_unlock(&g_status_lock);
}

/*
 * Walker-side counters are accumulated per thread and folded into the shared
 * atomics every COUNTER_FLUSH entries, at the end of each directory, and when
 * the thread exits. With 16 walkers doing four contended RMWs per entry the
 * shared cache lines bounced enough to show walk_entry at ~3% of a delete
 * profile; the status line only reads once a second, so batching costs
 * nothing visible.
 */
#define COUNTER_FLUSH 256

typedef struct {
    unsigned long long entries;
    unsigned long long dirs;
    unsigned long long files;
    unsigned long long would_delete;
} tls_counters_t;

static __thread tls_counters_t t_cnt;

static void counters_flush(void)
{
    tls_counters_t *c = &t_cnt;

    if (c->entries) {
        int idx = atomic_load_explicit(&g_bucket_index, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_entries, c->entries, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_window_entries, c->entries, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_bucket_entries[idx], c->entries, memory_order_relaxed);
    }
    if (c->dirs) atomic_fetch_add_explicit(&g_dirs, c->dirs, memory_order_relaxed);
    if (c->files) atomic_fetch_add_explicit(&g_files, c->files, memory_order_relaxed);
    if (c->would_delete) {
        atomic_fetch_add_explicit(&g_would_delete, c->would_delete, memory_order_relaxed);
    }
    memset(c, 0, sizeof(*c));
}

static void count_entry(void)
{
    if (++t_cnt.entries >= COUNTER_FLUSH) counters_flush();
}

static void count_error(void)
{
    atomic_fetch_add_explicit(&g_errors, 1, memory_order_relaxed);
}

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ------------------------------------------------------------------------ */
/* Eligibility                                                              */

static time_t pick_ts(const struct stat *st)
{
    switch (g_basis) {
    case TB_ATIME: return st->st_atime;
    case TB_CTIME: return st->st_ctime;
    case TB_MTIME:
    default:       return st->st_mtime;
    }
}

static int eligible(const struct stat *st)
{
    if (g_have_uid_filter && st->st_uid != g_filter_uid) return 0;
    if (g_have_gid_filter && st->st_gid != g_filter_gid) return 0;
    if (g_delete_all) return 1;
    if (g_age_days <= 0) return 0;
    return pick_ts(st) <= g_now - (time_t)g_age_days * 86400;
}

/* ------------------------------------------------------------------------ */
/* Unlink queue and worker pool                                             */

/*
 * Work items are per-directory batches, as in ecopy's traversal: up to
 * UNLINK_BATCH names from one directory, packed NUL-separated. A worker opens
 * the directory once (O_NOFOLLOW) and unlinkat()s each name relative to it, so
 * no path is re-walked per file and a symlinked-in component is never
 * followed.
 *
 * Every worker owns its own queue, and an item is routed to the worker chosen
 * by hashing its directory path. Hence all names of one directory, however
 * many batches the walker split them into, are unlinked by a single thread.
 * The kernel serializes unlinks within a directory anyway (the parent's
 * i_rwsem is taken exclusively per unlink, and XFS adds a per-AG unlinked-list
 * lock), so spreading one directory over several workers buys no parallelism
 * — it only makes every unlink thread spin, sleep and wake on that lock. A
 * profile of a shared-FIFO version on a 281k-file tree showed ~19% of all CPU
 * in rwsem_down_write_slowpath and another ~12% in scheduler load-balancing
 * caused by the resulting 94k context switches/s. Distinct directories still
 * run in parallel across workers.
 *
 * Backpressure is per worker: a walker whose current directory hashes to a
 * full queue blocks until that worker catches up, which is exactly the
 * directory the walker cannot usefully run ahead of.
 *
 * Containment. The names in an item were read from one specific directory
 * inode; they must only ever be unlinked from that inode. The worker reopens
 * the directory by path, and O_NOFOLLOW protects just the last component: if
 * an intermediate component were swapped for a symlink between the scan and
 * the unlink, the path would resolve somewhere else. So every item carries the
 * directory's (st_dev, st_ino) as seen by the walker's verified open, and the
 * worker refuses the whole batch when the reopened directory is not that
 * inode. Same rule as the walker's open_verified_dir(), applied on the way out.
 */
#define UNLINK_BATCH        512
#define UNLINK_BATCH_BYTES  (64 * 1024)
#define UNLINK_QUEUE_MIN_PER_WORKER (4 * UNLINK_BATCH)

typedef struct unlink_item {
    struct unlink_item *next;
    int count;
    size_t dir_len;
    dev_t dir_dev;  /* identity of the directory the names came from */
    ino_t dir_ino;
    char data[];   /* dir path, NUL, then `count` NUL-terminated names */
} unlink_item_t;

/* Test hooks (undocumented, only read when set): delays that widen the
 * window between scan and unlink / before the rmdir pass, so the smoke suite
 * can swap a directory for a symlink in between and check that the workers
 * refuse it. Zero in normal use; costs one integer compare per batch. */
static int g_test_unlink_delay_ms = 0;
static int g_test_rmdir_delay_ms = 0;

static void test_delay(int ms)
{
    if (ms > 0) {
        struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
        nanosleep(&ts, NULL);
    }
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    unlink_item_t *head;
    unlink_item_t *tail;
    size_t files;          /* names queued, for backpressure */
    int waiters;           /* walkers blocked in push() on this queue */
    pthread_t tid;
} unlink_queue_t;

static unlink_queue_t *g_uq = NULL;
static int g_uq_count = 0;
static size_t g_uq_cap_per_worker = UNLINK_QUEUE_MAX;
static int g_uq_closed = 0;    /* set once, under every queue's lock */
static int g_unlink_threads_started = 0;

static int stopping(void)
{
    return atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed);
}

/*
 * FNV-1a over the directory path: cheap, and any spread is good enough.
 *
 * Tried and rejected (Sep 2026): routing by the XFS allocation group of the
 * directory inode, so that no two workers share an AGI buffer lock. It halved
 * the voluntary context switches (676k -> 341k on a 1.33M-file tree) but made
 * wall time 0.5 s worse out of 7.4 s in every pair, because directories are
 * not spread evenly over AGs and the affinity costs more parallelism than the
 * AGI sleeps cost. The AGI wait is cheap; keep the hash.
 */
static unsigned unlink_queue_index_for(const unlink_item_t *it)
{
    uint64_t h = UINT64_C(1469598103934665603);
    size_t i;

    for (i = 0; i < it->dir_len; i++) {
        h ^= (unsigned char)it->data[i];
        h *= UINT64_C(1099511628211);
    }
    return (unsigned)(h % (uint64_t)g_uq_count);
}

/* Walker side. Blocks while that worker's queue is full; drops the item on
 * shutdown. */
static void unlink_queue_push(unlink_item_t *it)
{
    unlink_queue_t *q = &g_uq[unlink_queue_index_for(it)];

    pthread_mutex_lock(&q->lock);
    while (q->files >= g_uq_cap_per_worker && !g_uq_closed && !stopping()) {
        q->waiters++;
        pthread_cond_wait(&q->not_full, &q->lock);
        q->waiters--;
    }
    if (g_uq_closed || stopping()) {
        pthread_mutex_unlock(&q->lock);
        free(it);
        return;
    }
    it->next = NULL;
    if (q->tail) q->tail->next = it;
    else q->head = it;
    q->tail = it;
    q->files += (size_t)it->count;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

static void unlink_batch_run(const unlink_item_t *it)
{
    const char *dir = it->data;
    const char *name = dir + it->dir_len + 1;
    struct stat st;
    unsigned long long deleted = 0;
    int fd, i;

    test_delay(g_test_unlink_delay_ms);
    fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        if (errno != ENOENT) {
            fprintf(stderr, "edelete: open %s: %s\n", dir, strerror(errno));
            count_error();
        }
        return;
    }
    if (fstat(fd, &st) != 0 || !S_ISDIR(st.st_mode) ||
        st.st_dev != it->dir_dev || st.st_ino != it->dir_ino) {
        fprintf(stderr,
                "edelete: %s: directory changed since the scan; %d name(s) not unlinked\n",
                dir, it->count);
        count_error();
        close(fd);
        return;
    }
    for (i = 0; i < it->count; i++) {
        if (unlinkat(fd, name, 0) == 0) {
            deleted++;
        } else if (errno != ENOENT) {
            fprintf(stderr, "edelete: unlink %s/%s: %s\n", dir, name, strerror(errno));
            count_error();
        }
        name += strlen(name) + 1;
    }
    close(fd);
    if (deleted) {
        atomic_fetch_add_explicit(&g_deleted, deleted, memory_order_relaxed);
    }
}

/*
 * Each wakeup takes the whole queued list in one lock acquisition, then works
 * through it lock-free; items still unprocessed when a stop is requested are
 * dropped. The walker side is only woken when someone is actually blocked.
 */
static void *unlink_worker_main(void *arg)
{
    unlink_queue_t *q = (unlink_queue_t *)arg;

    for (;;) {
        unlink_item_t *it;
        int wake_pushers;

        pthread_mutex_lock(&q->lock);
        while (!q->head && !g_uq_closed && !stopping()) {
            pthread_cond_wait(&q->not_empty, &q->lock);
        }
        it = q->head;
        if (!it || stopping()) {
            pthread_mutex_unlock(&q->lock);
            return NULL;
        }
        q->head = NULL;
        q->tail = NULL;
        q->files = 0;
        wake_pushers = q->waiters > 0;
        if (wake_pushers) pthread_cond_broadcast(&q->not_full);
        pthread_mutex_unlock(&q->lock);

        while (it) {
            unlink_item_t *next = it->next;
            if (!stopping()) {
                unlink_batch_run(it);
            }
            free(it);
            it = next;
        }
    }
}

/*
 * Per-walker-thread batch under construction: names of the directory being
 * enumerated, flushed as one queue item when full or when the directory ends.
 * Thread-local so the buffer is allocated once per thread, not per directory.
 */
typedef struct {
    char *names;
    size_t used;
    int count;
} name_batch_t;

static __thread name_batch_t t_batch;

/*
 * node->st is the fstat of the directory the walker actually has open and is
 * reading names from (open_verified_dir refreshed it), so its dev/ino is the
 * identity the unlink worker must find again.
 */
static void batch_flush(const dirwalk_node_t *node, size_t dir_len)
{
    name_batch_t *b = &t_batch;
    unlink_item_t *it;

    if (b->count == 0) return;
    it = malloc(sizeof(*it) + dir_len + 1 + b->used);
    if (!it) {
        perror("malloc");
        count_error();
    } else {
        it->count = b->count;
        it->dir_len = dir_len;
        it->dir_dev = node->st.st_dev;
        it->dir_ino = node->st.st_ino;
        memcpy(it->data, node->path, dir_len + 1);
        memcpy(it->data + dir_len + 1, b->names, b->used);
        unlink_queue_push(it);
    }
    b->used = 0;
    b->count = 0;
}

static void batch_add(const dirwalk_node_t *node, size_t dir_len, const char *name)
{
    name_batch_t *b = &t_batch;
    const char *dir = node->path;
    size_t nlen = strlen(name) + 1;

    if (!b->names) {
        b->names = malloc(UNLINK_BATCH_BYTES);
        if (!b->names) {
            perror("malloc");
            count_error();
            return;
        }
    }
    if (b->count == UNLINK_BATCH || b->used + nlen > UNLINK_BATCH_BYTES) {
        batch_flush(node, dir_len);
    }
    if (nlen > UNLINK_BATCH_BYTES) {
        fprintf(stderr, "edelete: name too long under %s\n", dir);
        count_error();
        return;
    }
    memcpy(b->names + b->used, name, nlen);
    b->used += nlen;
    b->count++;
}

static void batch_thread_end(void)
{
    free(t_batch.names);
    memset(&t_batch, 0, sizeof(t_batch));
    counters_flush();
}

/*
 * One queue and one thread per worker. Should a thread fail to start, its
 * queue is dropped from the routing set so no item is ever hashed to a queue
 * nobody drains (the started queues are compacted to the front).
 */
static int unlink_pool_start(void)
{
    int i;

    g_uq = calloc((size_t)g_unlink_workers, sizeof(*g_uq));
    if (!g_uq) {
        perror("calloc");
        return -1;
    }
    for (i = 0; i < g_unlink_workers; i++) {
        unlink_queue_t *q = &g_uq[g_unlink_threads_started];

        pthread_mutex_init(&q->lock, NULL);
        pthread_cond_init(&q->not_empty, NULL);
        pthread_cond_init(&q->not_full, NULL);
        if (pthread_create(&q->tid, NULL, unlink_worker_main, q) != 0) {
            perror("pthread_create");
            pthread_cond_destroy(&q->not_full);
            pthread_cond_destroy(&q->not_empty);
            pthread_mutex_destroy(&q->lock);
            break;
        }
        g_unlink_threads_started++;
    }
    g_uq_count = g_unlink_threads_started;
    if (g_uq_count == 0) {
        free(g_uq);
        g_uq = NULL;
        return -1;
    }
    g_uq_cap_per_worker = UNLINK_QUEUE_MAX / (size_t)g_uq_count;
    if (g_uq_cap_per_worker < UNLINK_QUEUE_MIN_PER_WORKER) {
        g_uq_cap_per_worker = UNLINK_QUEUE_MIN_PER_WORKER;
    }
    return 0;
}

/*
 * Close the queues (workers drain what is left, then exit) and join them. The
 * queue structs themselves stay allocated for the life of the process: the
 * signal watcher may call unlink_pool_wake() at any time, and it must never
 * find a freed array or a destroyed mutex.
 */
static void unlink_pool_finish(void)
{
    int i;

    for (i = 0; i < g_uq_count; i++) {
        unlink_queue_t *q = &g_uq[i];
        pthread_mutex_lock(&q->lock);
        g_uq_closed = 1;
        pthread_cond_broadcast(&q->not_empty);
        pthread_cond_broadcast(&q->not_full);
        pthread_mutex_unlock(&q->lock);
    }
    for (i = 0; i < g_uq_count; i++) {
        pthread_join(g_uq[i].tid, NULL);
    }
    for (i = 0; i < g_uq_count; i++) {
        unlink_queue_t *q = &g_uq[i];
        unlink_item_t *it;

        /* Anything still queued was abandoned by a shutdown request. */
        pthread_mutex_lock(&q->lock);
        while ((it = q->head) != NULL) {
            q->head = it->next;
            free(it);
        }
        q->tail = NULL;
        q->files = 0;
        pthread_mutex_unlock(&q->lock);
    }
}

static void unlink_pool_wake(void)
{
    int i;

    for (i = 0; i < g_uq_count; i++) {
        unlink_queue_t *q = &g_uq[i];
        pthread_mutex_lock(&q->lock);
        pthread_cond_broadcast(&q->not_empty);
        pthread_cond_broadcast(&q->not_full);
        pthread_mutex_unlock(&q->lock);
    }
}

/* ------------------------------------------------------------------------ */
/* Directory list for the rmdir pass                                        */

typedef struct {
    char *path;
    int depth;
    dev_t dev;   /* identity of the directory as the walker had it open */
    ino_t ino;
} dir_rec_t;

static pthread_mutex_t g_dirs_lock = PTHREAD_MUTEX_INITIALIZER;
static dir_rec_t *g_dir_list = NULL;
static size_t g_dir_count = 0;
static size_t g_dir_cap = 0;

static int record_dir(const dirwalk_node_t *node)
{
    char *dup = strdup(node->path);
    if (!dup) {
        perror("strdup");
        return -1;
    }
    pthread_mutex_lock(&g_dirs_lock);
    if (g_dir_count == g_dir_cap) {
        size_t nc = g_dir_cap ? g_dir_cap * 2 : 1024;
        dir_rec_t *np = realloc(g_dir_list, nc * sizeof(*np));
        if (!np) {
            pthread_mutex_unlock(&g_dirs_lock);
            free(dup);
            perror("realloc");
            return -1;
        }
        g_dir_list = np;
        g_dir_cap = nc;
    }
    g_dir_list[g_dir_count].path = dup;
    g_dir_list[g_dir_count].depth = node->depth;
    g_dir_list[g_dir_count].dev = node->st.st_dev;
    g_dir_list[g_dir_count].ino = node->st.st_ino;
    g_dir_count++;
    pthread_mutex_unlock(&g_dirs_lock);
    return 0;
}

static void free_dir_list(void)
{
    for (size_t i = 0; i < g_dir_count; i++) free(g_dir_list[i].path);
    free(g_dir_list);
    g_dir_list = NULL;
    g_dir_count = g_dir_cap = 0;
}

static int dir_rec_cmp_desc_depth(const void *a, const void *b)
{
    const dir_rec_t *da = a, *db = b;
    if (da->depth != db->depth) return db->depth - da->depth;
    return strcmp(da->path, db->path);
}

static int rmdir_depth_of(size_t i)
{
    return g_dir_list[i].depth;
}

/* Length of the parent part of an absolute path (up to, excluding, the last
 * '/'; 0 for a direct child of "/"). */
static size_t parent_len(const char *p)
{
    const char *slash = strrchr(p, '/');
    return slash ? (size_t)(slash - p) : 0;
}

static int rmdir_same_parent(size_t a, size_t b)
{
    const char *pa = g_dir_list[a].path;
    const char *pb = g_dir_list[b].path;
    size_t la = parent_len(pa);

    return la == parent_len(pb) && memcmp(pa, pb, la) == 0;
}

/*
 * Remove one directory that the walk visited, by name relative to its parent,
 * and only if the entry of that name is still the very directory inode the
 * walker had open. A plain rmdir(path) would follow a symlink swapped into an
 * intermediate component and remove an empty directory somewhere else.
 */
static int rmdir_one(size_t i)
{
    const dir_rec_t *d = &g_dir_list[i];
    const char *p = d->path;
    size_t plen = parent_len(p);
    const char *name = p + plen + 1;
    char parent[PATH_MAX];
    struct stat st;
    int pfd;

    if (strcmp(p, "/") == 0) return 0;
    if (plen == 0) {
        parent[0] = '/';
        parent[1] = '\0';
    } else {
        memcpy(parent, p, plen);
        parent[plen] = '\0';
    }

    pfd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (pfd < 0) {
        if (errno != ENOENT) {
            fprintf(stderr, "edelete: open %s: %s\n", parent, strerror(errno));
            count_error();
        }
        return 0;
    }
    if (fstatat(pfd, name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
        if (errno != ENOENT) {
            fprintf(stderr, "edelete: stat %s: %s\n", p, strerror(errno));
            count_error();
        }
        close(pfd);
        return 0;
    }
    if (!S_ISDIR(st.st_mode) || st.st_dev != d->dev || st.st_ino != d->ino) {
        fprintf(stderr, "edelete: %s: directory changed since the scan; not removed\n", p);
        count_error();
        close(pfd);
        return 0;
    }
    if (unlinkat(pfd, name, AT_REMOVEDIR) == 0) {
        atomic_fetch_add_explicit(&g_removed_dirs, 1, memory_order_relaxed);
    } else if (errno != ENOTEMPTY && errno != EEXIST && errno != ENOENT &&
               errno != EBUSY && errno != ENOTDIR) {
        fprintf(stderr, "edelete: rmdir %s: %s\n", p, strerror(errno));
        count_error();
    }
    close(pfd);
    return 0; /* an rmdir failure is counted, not fatal to the pass */
}

/*
 * Every directory the walk visited is a candidate; children sort before their
 * parents, and the depth grouping guarantees a parent is tried only after all
 * of its children have been, so a whole emptied subtree collapses in one pass
 * (including the start path itself when it ends up empty).
 *
 * Within a level the list is sorted by path, so siblings are adjacent; the
 * contiguous slicing with parent-aware boundaries hands each parent's
 * children to exactly one thread. rmdir takes the parent's inode lock
 * exclusively, so any distribution that puts siblings on several threads has
 * them spin on that lock (over half of the rmdir pass in the round-robin
 * version, and still two thirds of it with plain equal slices on a tree
 * whose levels have only two parents).
 */
static void remove_empty_directories(void)
{
    if (g_dir_count == 0) return;
    test_delay(g_test_rmdir_delay_ms);
    qsort(g_dir_list, g_dir_count, sizeof(*g_dir_list), dir_rec_cmp_desc_depth);
    dirwalk_depth_groups_ex(g_dir_count, g_threads, rmdir_depth_of, rmdir_one,
                            NULL, NULL, DIRWALK_GROUPS_CONTIGUOUS,
                            rmdir_same_parent);
}

/* ------------------------------------------------------------------------ */
/* dirwalk callbacks                                                        */

static int walk_dir_begin(const dirwalk_node_t *node, int dir_fd, void **ctx)
{
    (void)dir_fd;
    *ctx = NULL;
    count_entry();
    t_cnt.dirs++;
    if (!g_dry_run && record_dir(node) != 0) {
        count_error();
    }
    return 0;
}

static int walk_entry(const dirwalk_node_t *node, int dir_fd, void *ctx,
                      const char *name, const struct stat *st)
{
    (void)dir_fd;
    (void)ctx;

    /* st == NULL: lazy_stat delivered a non-directory and we run without
     * ownership/age filters, so it is eligible by construction. */
    if (st && S_ISDIR(st->st_mode)) {
        return DIRWALK_DESCEND;
    }

    count_entry();
    t_cnt.files++;
    if (st && !eligible(st)) {
        return DIRWALK_SKIP;
    }
    if (g_dry_run) {
        t_cnt.would_delete++;
        return DIRWALK_SKIP;
    }
    batch_add(node, strlen(node->path), name);
    return DIRWALK_SKIP;
}

static void walk_dir_end(const dirwalk_node_t *node, int dir_fd, void *ctx, int rc)
{
    (void)dir_fd; (void)ctx;
    if (rc != 0) count_error();
    if (!g_dry_run) batch_flush(node, strlen(node->path));
    counters_flush();
}

static void walk_error(const char *path)
{
    (void)path;
    count_error();
}

static void edelete_stop(void)
{
    dirwalk_request_stop();
    unlink_pool_wake();
}

/* ------------------------------------------------------------------------ */
/* Status line                                                              */

static void clear_status_line(void)
{
    if (isatty(STDOUT_FILENO)) printf("\r\033[2K\r");
    else printf("\r%160s\r", "");
    fflush(stdout);
}

static void *status_thread_main(void *arg)
{
    double run_start = *(double *)arg;

    while (!atomic_load(&g_stop_status)) {
        status_pause();
        if (atomic_load(&g_stop_status)) break;
        {
            int next = (atomic_load(&g_bucket_index) + 1) % WINDOW_SECONDS;
            unsigned long long expired = atomic_exchange(&g_bucket_entries[next], 0);
            atomic_fetch_sub(&g_window_entries, expired);
            atomic_store(&g_bucket_index, next);
        }
        {
            unsigned int seen = atomic_load(&g_seconds_seen);
            if (seen < WINDOW_SECONDS) atomic_store(&g_seconds_seen, seen + 1U);
        }
        {
            unsigned int divisor = atomic_load(&g_seconds_seen);
            unsigned long long acted = g_dry_run ? atomic_load(&g_would_delete) : atomic_load(&g_deleted);
            char walked[32], rate[32], acted_buf[32], elapsed[32];

            if (divisor == 0) divisor = 1;
            format_count_si((double)atomic_load(&g_entries), walked, sizeof(walked));
            format_count_si((double)atomic_load(&g_window_entries) / (double)divisor, rate, sizeof(rate));
            format_count_si((double)acted, acted_buf, sizeof(acted_buf));
            format_duration(now_sec() - run_start, elapsed, sizeof(elapsed));
            printf("\r%s walk/s(10s) | walked:%s | %s:%s | el:%s            ",
                   rate, walked, g_dry_run ? "would_unlink" : "unlinked", acted_buf, elapsed);
            fflush(stdout);
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------------ */
/* CLI                                                                      */

static int parse_basis(const char *s, time_basis_t *out)
{
    if (strcmp(s, "atime") == 0) { *out = TB_ATIME; return 0; }
    if (strcmp(s, "mtime") == 0) { *out = TB_MTIME; return 0; }
    if (strcmp(s, "ctime") == 0) { *out = TB_CTIME; return 0; }
    return -1;
}

static int parse_id(const char *label, const char *s, unsigned long *out)
{
    char *end = NULL;
    unsigned long v;

    if (!s || !*s) {
        fprintf(stderr, "edelete: %s requires a numeric argument\n", label);
        return -1;
    }
    errno = 0;
    v = strtoul(s, &end, 10);
    if (errno || end == s || *end) {
        fprintf(stderr, "edelete: %s must be a non-negative integer\n", label);
        return -1;
    }
    *out = v;
    return 0;
}

static int line_confirms_yes(char *p)
{
    while (*p && isspace((unsigned char)*p)) p++;
    if (strncmp(p, "YES", 3) != 0) return 0;
    p += 3;
    while (*p && isspace((unsigned char)*p)) p++;
    return *p == '\0';
}

/* 0 if the user typed YES; -1 on cancel, EOF, or any other answer. */
static int confirm_delete_prompt(const char *root_path, const char *basis_str)
{
    char line[64];

    fprintf(stderr,
            "\n"
            "edelete: --delete will permanently unlink non-directory paths under the start path\n"
            "         and remove directories that become empty (including the start path when empty;\n"
            "         never removes the filesystem root `/`).\n"
            "\n"
            "  Resolved start path: %s\n"
            "  Filter:              %s\n",
            root_path,
            g_delete_all ? "all non-directories (no age filter)" : "age-based (see below)");
    if (!g_delete_all && basis_str) {
        fprintf(stderr,
                "  Time basis:          %s\n"
                "  Minimum age:         %d day(s)\n",
                basis_str, g_age_days);
    }
    if (g_have_uid_filter || g_have_gid_filter) {
        fprintf(stderr, "  Ownership filter:    ");
        if (g_have_uid_filter) fprintf(stderr, "uid=%u", (unsigned)g_filter_uid);
        if (g_have_uid_filter && g_have_gid_filter) fprintf(stderr, " ");
        if (g_have_gid_filter) fprintf(stderr, "gid=%u", (unsigned)g_filter_gid);
        fprintf(stderr, "\n");
    }
    fprintf(stderr,
            "  Walker threads:      %d  (EDELETE_THREADS)\n"
            "  Unlink threads:      %d  (EDELETE_MAX_UNLINK_INFLIGHT)\n"
            "\n"
            "Type YES to proceed, anything else cancels: ",
            g_threads, g_unlink_workers);
    fflush(stderr);

    if (!fgets(line, sizeof(line), stdin)) {
        fprintf(stderr, "\nedelete: cancelled (no input).\n");
        return -1;
    }
    if (!line_confirms_yes(line)) {
        fprintf(stderr, "edelete: cancelled.\n");
        return -1;
    }
    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s [options] <path>\n"
            "       %s [options] <atime|mtime|ctime> <days> <path>\n"
            "\n"
            "  First form:  every non-directory under <path>.\n"
            "  Second form: chosen timestamp at least <days> full days old.\n"
            "  Default is dry-run.  --delete unlinks.  Does not follow symlinks.\n"
            "  Pass <path> itself; globs like parent/* skip hidden names.\n"
            "\n"
            "Options:\n"
            "  --delete                prompt (type YES), then unlink matches and rmdir empty dirs\n"
            "  --force                 with --delete: skip the YES prompt\n"
            "  --uid UID               only this owner\n"
            "  --gid GID               only this group (both apply when set)\n"
            "  -h, --help              show this help\n"
            "\n"
            "Environment:\n"
            "  EDELETE_THREADS                directory walker threads (default %d)\n"
            "  EDELETE_MAX_UNLINK_INFLIGHT    concurrent unlink threads (default %d)\n",
            prog, prog, DEFAULT_THREADS, DEFAULT_UNLINK_WORKERS);
}

int main(int argc, char **argv)
{
    static char root_abs[PATH_MAX];
    const char *basis_str = NULL;
    const char *root_path = NULL;
    struct stat root_st;
    dirwalk_cfg_t cfg;
    dirwalk_ops_t ops;
    pthread_t status_thread;
    int have_status_thread = 0;
    int interrupted = 0;
    double t0, t1;
    int ai = 1;

    while (ai < argc && argv[ai][0] == '-') {
        if (strcmp(argv[ai], "--delete") == 0) {
            g_dry_run = 0;
            ai++;
        } else if (strcmp(argv[ai], "--force") == 0) {
            g_force = 1;
            ai++;
        } else if (strcmp(argv[ai], "--uid") == 0 || strcmp(argv[ai], "--gid") == 0) {
            unsigned long v;
            int is_uid = argv[ai][2] == 'u';
            if (ai + 1 >= argc) {
                fprintf(stderr, "edelete: %s requires an argument\n", argv[ai]);
                usage(argv[0]);
                return 2;
            }
            if (parse_id(argv[ai], argv[ai + 1], &v) != 0) return 2;
            if (is_uid) { g_filter_uid = (uid_t)v; g_have_uid_filter = 1; }
            else        { g_filter_gid = (gid_t)v; g_have_gid_filter = 1; }
            ai += 2;
        } else if (strcmp(argv[ai], "--help") == 0 || strcmp(argv[ai], "-h") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "edelete: unknown option %s\n", argv[ai]);
            usage(argv[0]);
            return 2;
        }
    }

    if (argc - ai == 1) {
        g_delete_all = 1;
        root_path = argv[ai];
    } else if (argc - ai == 3) {
        long d;
        char *end = NULL;

        basis_str = argv[ai];
        if (parse_basis(basis_str, &g_basis) != 0) {
            fprintf(stderr, "edelete: time basis must be atime, mtime, or ctime\n");
            return 2;
        }
        errno = 0;
        d = strtol(argv[ai + 1], &end, 10);
        if (errno || !end || *end || d < 1 || d > 365000L) {
            fprintf(stderr, "edelete: days must be an integer in [1, 365000]\n");
            return 2;
        }
        g_age_days = (int)d;
        root_path = argv[ai + 2];
    } else {
        if (argc - ai > 0) {
            fprintf(stderr, "edelete: use either one argument (<path>) or three "
                            "(<atime|mtime|ctime> <days> <path>)\n");
        }
        usage(argv[0]);
        return 2;
    }

    if (path_resolve_existing(root_path, root_abs, "edelete: ") != 0) return 2;
    root_path = root_abs;
    if (strcmp(root_path, "/") == 0) {
        fprintf(stderr, "edelete: refusing to operate on the filesystem root `/`\n");
        return 2;
    }
    if (lstat(root_path, &root_st) != 0) {
        fprintf(stderr, "edelete: %s: %s\n", root_path, strerror(errno));
        return 2;
    }
    if (!S_ISDIR(root_st.st_mode)) {
        fprintf(stderr, "edelete: %s: not a directory\n", root_path);
        return 2;
    }

    if (!g_delete_all) {
        g_now = time(NULL);
        if (g_now == (time_t)-1) {
            fprintf(stderr, "edelete: time() failed\n");
            return 1;
        }
    }

    g_threads = env_int_or_default("EDELETE_THREADS", DEFAULT_THREADS, 1, 1024);
    g_unlink_workers = env_int_or_default("EDELETE_MAX_UNLINK_INFLIGHT", DEFAULT_UNLINK_WORKERS, 1, 1024);
    g_test_unlink_delay_ms = env_int_or_default("EDELETE_TEST_UNLINK_DELAY_MS", 0, 0, 60000);
    g_test_rmdir_delay_ms = env_int_or_default("EDELETE_TEST_RMDIR_DELAY_MS", 0, 0, 60000);
    /* Without ownership/age filters the walker needs no stat for files:
     * d_type tells directories apart, and that is all walk_entry() reads. */
    g_lazy_stat = g_delete_all && !g_have_uid_filter && !g_have_gid_filter;

    if (!g_dry_run && !g_force && confirm_delete_prompt(root_path, basis_str) != 0) return 3;

    /* Installed after the prompt so a Ctrl+C at the prompt simply exits. */
    shutdown_install_handlers(edelete_stop);

    t0 = now_sec();
    if (pthread_create(&status_thread, NULL, status_thread_main, &t0) == 0) {
        have_status_thread = 1;
    }

    if (!g_dry_run && unlink_pool_start() != 0) {
        fprintf(stderr, "edelete: no unlink threads started\n");
        return 1;
    }

    memset(&cfg, 0, sizeof(cfg));
    cfg.threads = g_threads;
    cfg.stop = &g_shutdown_requested;
    cfg.lazy_stat = g_lazy_stat;
#ifdef ECOPY_HAVE_GETDENTS64
    cfg.getdents_buf = GETDENTS_BUF_BYTES;
#endif
    memset(&ops, 0, sizeof(ops));
    ops.dir_begin = walk_dir_begin;
    ops.entry = walk_entry;
    ops.dir_end = walk_dir_end;
    ops.thread_end = batch_thread_end;
    ops.error = walk_error;

    if (dirwalk_start(root_path, &root_st, &cfg, &ops) != 0) {
        fprintf(stderr, "edelete: could not start the walk\n");
        count_error();
    } else {
        dirwalk_wait();
    }
    if (!g_dry_run) {
        unlink_pool_finish();
    }
    interrupted = stopping();

    if (!g_dry_run && !interrupted) {
        remove_empty_directories();
    }
    free_dir_list();

    status_stop();
    if (have_status_thread) pthread_join(status_thread, NULL);
    clear_status_line();
    if (interrupted) fprintf(stderr, "edelete: interrupted by signal; partial results below.\n");
    t1 = now_sec();

    {
        double elapsed = t1 - t0;
        unsigned long long entries = atomic_load(&g_entries);
        unsigned long long errors = atomic_load(&g_errors);
        char avg[32];

        format_count_si(elapsed > 0.0 ? (double)entries / elapsed : 0.0, avg, sizeof(avg));
        printf("delete_all=%d\n", g_delete_all);
        if (!g_delete_all) {
            printf("basis=%s\n", basis_str);
            printf("age_days=%d\n", g_age_days);
        }
        printf("force=%d\n", g_force);
        printf("filter_uid_set=%d\n", g_have_uid_filter);
        if (g_have_uid_filter) printf("filter_uid=%u\n", (unsigned)g_filter_uid);
        printf("filter_gid_set=%d\n", g_have_gid_filter);
        if (g_have_gid_filter) printf("filter_gid=%u\n", (unsigned)g_filter_gid);
        printf("mode=%s\n", g_dry_run ? "dry-run" : "delete");
        printf("start_path=%s\n", root_path);
        printf("threads=%d\n", g_threads);
        printf("max_unlink_inflight=%d\n", g_dry_run ? 0 : g_unlink_threads_started);
        printf("walk_entries=%llu\n", entries);
        printf("entries_scanned=%llu\n", entries);
        printf("dirs_seen=%llu\n", (unsigned long long)atomic_load(&g_dirs));
        printf("files_seen=%llu\n", (unsigned long long)atomic_load(&g_files));
        printf("deleted_files=%llu\n", (unsigned long long)atomic_load(&g_deleted));
        printf("removed_empty_dirs=%llu\n", (unsigned long long)atomic_load(&g_removed_dirs));
        printf("would_delete=%llu\n", (unsigned long long)atomic_load(&g_would_delete));
        printf("errors=%llu\n", errors);
        printf("elapsed_sec=%.3f\n", elapsed);
        printf("avg_walk_per_sec=%s\n", avg);
        printf("avg_entries_per_sec=%s\n", avg);

        if (interrupted) return 130;
        return errors ? 1 : 0;
    }
}
