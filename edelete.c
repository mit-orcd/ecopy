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
 * go onto a bounded queue drained by a separate pool of unlink threads, so a
 * slow unlink (quota accounting, ZFS block frees) never stalls the scan and
 * the number of concurrent unlinks is an explicit knob. After the walk, in
 * delete mode, directories that became empty are removed deepest-first, in
 * parallel per depth level, up to and including the start path.
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
#define UNLINK_QUEUE_MAX       262144   /* backpressure cap on queued unlinks */
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

static void count_entry(void)
{
    int idx = atomic_load_explicit(&g_bucket_index, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_entries, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_window_entries, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_bucket_entries[idx], 1, memory_order_relaxed);
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
 * followed. Just as important, consecutive files of one directory land on one
 * worker instead of being sprayed across all of them, which would make every
 * unlink thread contend on that directory's inode lock.
 */
#define UNLINK_BATCH        512
#define UNLINK_BATCH_BYTES  (64 * 1024)

typedef struct unlink_item {
    struct unlink_item *next;
    int count;
    size_t dir_len;
    char data[];   /* dir path, NUL, then `count` NUL-terminated names */
} unlink_item_t;

static pthread_mutex_t g_uq_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_uq_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t g_uq_not_full = PTHREAD_COND_INITIALIZER;
static unlink_item_t *g_uq_head = NULL;
static unlink_item_t *g_uq_tail = NULL;
static size_t g_uq_files = 0;       /* names queued, for backpressure */
static int g_uq_closed = 0;
static pthread_t *g_unlink_threads = NULL;
static int g_unlink_threads_started = 0;

static int stopping(void)
{
    return atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed);
}

/* Walker side. Blocks while the queue is full; drops the item on shutdown. */
static void unlink_queue_push(unlink_item_t *it)
{
    pthread_mutex_lock(&g_uq_lock);
    while (g_uq_files >= UNLINK_QUEUE_MAX && !g_uq_closed && !stopping()) {
        pthread_cond_wait(&g_uq_not_full, &g_uq_lock);
    }
    if (g_uq_closed || stopping()) {
        pthread_mutex_unlock(&g_uq_lock);
        free(it);
        return;
    }
    it->next = NULL;
    if (g_uq_tail) g_uq_tail->next = it;
    else g_uq_head = it;
    g_uq_tail = it;
    g_uq_files += (size_t)it->count;
    pthread_cond_signal(&g_uq_not_empty);
    pthread_mutex_unlock(&g_uq_lock);
}

static void unlink_batch_run(const unlink_item_t *it)
{
    const char *dir = it->data;
    const char *name = dir + it->dir_len + 1;
    int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    int i;

    if (fd < 0) {
        if (errno != ENOENT) {
            fprintf(stderr, "edelete: open %s: %s\n", dir, strerror(errno));
            count_error();
        }
        return;
    }
    for (i = 0; i < it->count; i++) {
        if (unlinkat(fd, name, 0) == 0) {
            atomic_fetch_add_explicit(&g_deleted, 1, memory_order_relaxed);
        } else if (errno != ENOENT) {
            fprintf(stderr, "edelete: unlink %s/%s: %s\n", dir, name, strerror(errno));
            count_error();
        }
        name += strlen(name) + 1;
    }
    close(fd);
}

static void *unlink_worker_main(void *arg)
{
    (void)arg;
    for (;;) {
        unlink_item_t *it;

        pthread_mutex_lock(&g_uq_lock);
        while (!g_uq_head && !g_uq_closed && !stopping()) {
            pthread_cond_wait(&g_uq_not_empty, &g_uq_lock);
        }
        it = g_uq_head;
        if (!it || stopping()) {
            pthread_mutex_unlock(&g_uq_lock);
            return NULL;
        }
        g_uq_head = it->next;
        if (!g_uq_head) g_uq_tail = NULL;
        g_uq_files -= (size_t)it->count;
        pthread_cond_broadcast(&g_uq_not_full);
        pthread_mutex_unlock(&g_uq_lock);

        unlink_batch_run(it);
        free(it);
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

static void batch_flush(const char *dir, size_t dir_len)
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
        memcpy(it->data, dir, dir_len + 1);
        memcpy(it->data + dir_len + 1, b->names, b->used);
        unlink_queue_push(it);
    }
    b->used = 0;
    b->count = 0;
}

static void batch_add(const char *dir, size_t dir_len, const char *name)
{
    name_batch_t *b = &t_batch;
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
        batch_flush(dir, dir_len);
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
}

static int unlink_pool_start(void)
{
    int i;

    g_unlink_threads = calloc((size_t)g_unlink_workers, sizeof(*g_unlink_threads));
    if (!g_unlink_threads) {
        perror("calloc");
        return -1;
    }
    for (i = 0; i < g_unlink_workers; i++) {
        if (pthread_create(&g_unlink_threads[i], NULL, unlink_worker_main, NULL) != 0) {
            perror("pthread_create");
            break;
        }
        g_unlink_threads_started++;
    }
    return g_unlink_threads_started > 0 ? 0 : -1;
}

/* Close the queue (workers drain what is left, then exit) and join them. */
static void unlink_pool_finish(void)
{
    int i;
    unlink_item_t *it;

    pthread_mutex_lock(&g_uq_lock);
    g_uq_closed = 1;
    pthread_cond_broadcast(&g_uq_not_empty);
    pthread_cond_broadcast(&g_uq_not_full);
    pthread_mutex_unlock(&g_uq_lock);

    for (i = 0; i < g_unlink_threads_started; i++) {
        pthread_join(g_unlink_threads[i], NULL);
    }
    free(g_unlink_threads);
    g_unlink_threads = NULL;

    /* Anything still queued was abandoned by a shutdown request. */
    while ((it = g_uq_head) != NULL) {
        g_uq_head = it->next;
        free(it);
    }
    g_uq_tail = NULL;
    g_uq_files = 0;
}

static void unlink_pool_wake(void)
{
    pthread_mutex_lock(&g_uq_lock);
    pthread_cond_broadcast(&g_uq_not_empty);
    pthread_cond_broadcast(&g_uq_not_full);
    pthread_mutex_unlock(&g_uq_lock);
}

/* ------------------------------------------------------------------------ */
/* Directory list for the rmdir pass                                        */

typedef struct {
    char *path;
    int depth;
} dir_rec_t;

static pthread_mutex_t g_dirs_lock = PTHREAD_MUTEX_INITIALIZER;
static dir_rec_t *g_dir_list = NULL;
static size_t g_dir_count = 0;
static size_t g_dir_cap = 0;

static int record_dir(const char *path, int depth)
{
    char *dup = strdup(path);
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
    g_dir_list[g_dir_count].depth = depth;
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

static int rmdir_one(size_t i)
{
    const char *p = g_dir_list[i].path;

    if (strcmp(p, "/") == 0) return 0;
    if (rmdir(p) == 0) {
        atomic_fetch_add_explicit(&g_removed_dirs, 1, memory_order_relaxed);
    } else if (errno != ENOTEMPTY && errno != EEXIST && errno != ENOENT &&
               errno != EBUSY && errno != ENOTDIR) {
        fprintf(stderr, "edelete: rmdir %s: %s\n", p, strerror(errno));
        count_error();
    }
    return 0; /* an rmdir failure is counted, not fatal to the pass */
}

/*
 * Every directory the walk visited is a candidate; children sort before their
 * parents, and the depth grouping guarantees a parent is tried only after all
 * of its children have been, so a whole emptied subtree collapses in one pass
 * (including the start path itself when it ends up empty).
 */
static void remove_empty_directories(void)
{
    if (g_dir_count == 0) return;
    qsort(g_dir_list, g_dir_count, sizeof(*g_dir_list), dir_rec_cmp_desc_depth);
    dirwalk_depth_groups(g_dir_count, g_threads, rmdir_depth_of, rmdir_one, NULL, NULL);
}

/* ------------------------------------------------------------------------ */
/* dirwalk callbacks                                                        */

static int walk_dir_begin(const dirwalk_node_t *node, int dir_fd, void **ctx)
{
    (void)dir_fd;
    *ctx = NULL;
    count_entry();
    atomic_fetch_add_explicit(&g_dirs, 1, memory_order_relaxed);
    if (!g_dry_run && record_dir(node->path, node->depth) != 0) {
        count_error();
    }
    return 0;
}

static int walk_entry(const dirwalk_node_t *node, int dir_fd, void *ctx,
                      const char *name, const struct stat *st)
{
    (void)dir_fd;
    (void)ctx;

    if (S_ISDIR(st->st_mode)) {
        return DIRWALK_DESCEND;
    }

    count_entry();
    atomic_fetch_add_explicit(&g_files, 1, memory_order_relaxed);
    if (!eligible(st)) {
        return DIRWALK_SKIP;
    }
    if (g_dry_run) {
        atomic_fetch_add_explicit(&g_would_delete, 1, memory_order_relaxed);
        return DIRWALK_SKIP;
    }
    batch_add(node->path, strlen(node->path), name);
    return DIRWALK_SKIP;
}

static void walk_dir_end(const dirwalk_node_t *node, int dir_fd, void *ctx, int rc)
{
    (void)dir_fd; (void)ctx;
    if (rc != 0) count_error();
    if (!g_dry_run) batch_flush(node->path, strlen(node->path));
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
