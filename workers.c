/*
 * workers.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "compat.h"
#include "workers.h"
#include "config.h"
#include "types.h"
#include "stats.h"
#include "fs_util.h"
#include "copy_policy.h"
#include "verify.h"
#include "telemetry.h"
#include "ssh_transport.h"
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <limits.h>
#include <stdint.h>
#include <time.h>

#ifndef SEEK_DATA
#define SEEK_DATA 3
#endif
#ifndef SEEK_HOLE
#define SEEK_HOLE 4
#endif

/*
 * Runtime overrides:
 *
 *   DIRECT_COPY_MAX_WORKERS
 *   DIRECT_COPY_LARGE_WORKERS
 *   DIRECT_COPY_LARGE_FILE_INFLIGHT
 *   DIRECT_COPY_CHUNK_MB
 *   DIRECT_COPY_DISABLE_COPY_FILE_RANGE
 *   DIRECT_COPY_DISABLE_READ_DIRECT_IO
 *   DIRECT_COPY_DISABLE_WRITE_DIRECT_IO
 *   DIRECT_COPY_LARGE_READERS
 *   DIRECT_COPY_LARGE_WRITERS
 */

typedef enum {
    WORK_NONE = 0,
    WORK_SMALL_FILE,
    WORK_LARGE_FILE_START
} work_kind_t;

typedef struct large_buffer {
    void *data;
    size_t cap;
    size_t len;
    off_t offset;
    struct large_buffer *next;
} large_buffer_t;

typedef struct large_file_ctx {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    char name[PATH_MAX];
    char tmp_name[PATH_MAX];
    struct stat src_st;
    off_t bulk_end;
    off_t next_read_offset;
    dir_handle_t *dir;
    int fd_in;
    int fd_out;
    int in_direct;
    int out_direct;
    int failed;
    int read_done;
    int active_readers;
    int active_writers;
    pthread_mutex_t lock;
    pthread_cond_t free_cond;
    pthread_cond_t ready_cond;
    large_buffer_t *free_head;
    large_buffer_t *free_tail;
    large_buffer_t *ready_head;
    large_buffer_t *ready_tail;
    uint64_t free_count;
    uint64_t ready_count;
    pthread_t *reader_threads;
    pthread_t *writer_threads;
    int reader_count;
    int writer_count;
    int readers_started;
    int writers_started;
    uint64_t service_start_ns;
} large_file_ctx_t;

typedef struct {
    work_kind_t kind;
    file_task_t *file_task;
} work_claim_t;

static pthread_t *g_workers = NULL;
static int g_worker_count = 0;
static int g_large_worker_count = 0;
static int g_large_file_inflight = 0;
static int g_max_active_large_files = 0;
static int g_large_reader_count = 0;
static int g_large_writer_count = 0;
static int g_large_config_clamped = 0;
static off_t g_chunk_size = 0;
static off_t g_large_threshold = 0;
static int g_max_queued_files = 0;
static int g_small_worker_limit = 0;
static off_t g_ssh_putfile_max = 0;   /* max size streamed as one PUTFILE frame */

#define MAX_LARGE_BUFFER_BUDGET_MB 8192

/* -------------------- scheduler state -------------------- */

static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;
/*
 * g_queue_cond wakes worker threads waiting to claim work. g_space_cond wakes
 * producer (traversal) threads waiting for room in the bounded queue. Keeping
 * the two predicates on separate condition variables lets us wake exactly one
 * waiter of the right kind with pthread_cond_signal instead of broadcasting to
 * every parked thread, which otherwise caused severe lock contention on
 * small-file workloads (hundreds of threads waking per enqueue).
 */
static pthread_cond_t  g_queue_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_space_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_large_done_cond = PTHREAD_COND_INITIALIZER;
/*
 * Dispatch queues are max-heaps keyed by file_task_t.sched_key (see enqueue),
 * so the backlog drains biggest-data-first rather than FIFO. The small/large
 * split is unchanged (it selects the execution path and slot budget); only the
 * ordering within each queue changed. Both are guarded by g_queue_lock; len is
 * the queue depth reported to progress and used for backpressure.
 */
typedef struct {
    file_task_t **items;
    size_t        len;
    size_t        cap;
} task_heap_t;
static task_heap_t     g_small_heap;
static task_heap_t     g_large_heap;
/*
 * Recycled file_task_t nodes. Each task carries three PATH_MAX buffers (~12 KiB)
 * so allocating and zeroing one per file dominated small-file CPU under perf.
 * Freed tasks are pushed here (under g_queue_lock) and reused by enqueue_task
 * instead of going back to malloc/calloc. Drained in workers_stop().
 */
static file_task_t    *g_task_freelist = NULL;
static int             g_queue_done = 0;
static uint64_t        g_small_workers_active = 0;
static uint64_t        g_large_workers_active = 0;
/*
 * When set (default), dispatch prefers files with the most allocated data.
 * When 0, sched_key falls back to enqueue order so the heaps behave FIFO.
 */
static int             g_size_priority = 1;
static uint64_t        g_enqueue_seq = 0; /* monotonic, under g_queue_lock */

static int g_workers_error = 0;
static pthread_mutex_t g_workers_error_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_explicit_large_readers = 0;
static int g_explicit_large_writers = 0;
/* When 0, the blocking-wait timers are skipped entirely (no clock_gettime). */
static int g_collect_wait_timing = 0;

static uint64_t monotonic_ns(void);
static void copy_path_field(char *dst, size_t dstsz, const char *src);
static int copy_file_remote(file_task_t *task, uint64_t *payload_bytes);

/* -------------------- runtime config -------------------- */

static void split_large_workers(int total, int *reader_count, int *writer_count)
{
    int readers = (total + 1) / 2;
    int writers = total - readers;

    if (writers < 1) {
        writers = 1;
        readers = total - 1;
    }
    if (readers < 1) {
        readers = 1;
    }

    *reader_count = readers;
    *writer_count = writers;
}

static void normalize_large_pipeline_config(int requested_readers, int requested_writers)
{
    int requested_total;
    int slot_budget = g_worker_count > 0 ? g_worker_count : 1;

    if (requested_readers < 1) {
        requested_readers = 1;
    }
    if (requested_writers < 1) {
        requested_writers = 1;
    }

    requested_total = requested_readers + requested_writers;
    g_large_config_clamped = 0;

    if (requested_total > slot_budget) {
        g_large_config_clamped = 1;
        if (slot_budget == 1) {
            g_large_reader_count = 1;
            g_large_writer_count = 1;
            g_large_worker_count = 1;
            return;
        }

        g_large_reader_count = (int)(((long long)requested_readers * slot_budget +
                                     requested_total / 2) /
                                    requested_total);
        if (g_large_reader_count < 1) {
            g_large_reader_count = 1;
        }
        if (g_large_reader_count > slot_budget - 1) {
            g_large_reader_count = slot_budget - 1;
        }
        g_large_writer_count = slot_budget - g_large_reader_count;
        g_large_worker_count = slot_budget;
        return;
    }

    g_large_reader_count = requested_readers;
    g_large_writer_count = requested_writers;
    g_large_worker_count = requested_total;
}

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

static void init_runtime_config(void)
{
    if (g_worker_count > 0 && g_large_worker_count > 0 && g_large_file_inflight > 0 && g_chunk_size > 0 && g_large_threshold > 0 && g_max_queued_files > 0 && g_small_worker_limit > 0) {
        return;
    }

    g_worker_count = env_int_or_default("DIRECT_COPY_MAX_WORKERS",
                                        MAX_WORKER_SLOTS,
                                        2,
                                        512);

    g_large_worker_count = env_int_or_default("DIRECT_COPY_LARGE_WORKERS",
                                              LARGE_FILE_WORKERS,
                                              2,
                                              g_worker_count);

    g_explicit_large_readers = env_int_or_default("DIRECT_COPY_LARGE_READERS",
                                                  4,
                                                  0,
                                                  g_worker_count);
    g_explicit_large_writers = env_int_or_default("DIRECT_COPY_LARGE_WRITERS",
                                                  2,
                                                  0,
                                                  g_worker_count);

    if ((g_explicit_large_readers > 0) != (g_explicit_large_writers > 0)) {
        fprintf(stderr,
                "DIRECT_COPY_LARGE_READERS and DIRECT_COPY_LARGE_WRITERS must be set together\n");
        g_explicit_large_readers = 4;
        g_explicit_large_writers = 2;
    }

    g_large_file_inflight = env_int_or_default("DIRECT_COPY_LARGE_FILE_INFLIGHT",
                                               LARGE_FILE_INFLIGHT,
                                               1,
                                               1024);

    {
        int chunk_mb = env_int_or_default("DIRECT_COPY_CHUNK_MB",
                                          (int)(CHUNK_SIZE / (1024 * 1024)),
                                          1,
                                          4096);
        g_chunk_size = (off_t)chunk_mb * 1024 * 1024;
    }

    {
        int threshold_mb = env_int_or_default("DIRECT_COPY_LARGE_THRESHOLD_MB",
                                              LARGE_FILE_THRESHOLD_MB,
                                              1,
                                              1024 * 1024);
        g_large_threshold = (off_t)threshold_mb * 1024 * 1024;
    }

    g_max_queued_files = env_int_or_default("DIRECT_COPY_MAX_QUEUED_FILES",
                                            262144,
                                            1,
                                            10000000);

    g_size_priority = env_int_or_default("DIRECT_COPY_SIZE_PRIORITY",
                                         SIZE_PRIORITY_DEFAULT, 0, 1);

    g_small_worker_limit = env_int_or_default("DIRECT_COPY_SMALL_MAX_WORKERS",
                                             SMALL_WORKER_SLOTS,
                                             1,
                                             g_worker_count);

    {
        /* Files at or below this size are shipped to an SSH target as a single
         * fire-and-forget PUTFILE frame (KiB; default 1 MiB). Capped so the
         * frame stays within the protocol limit. */
        int kib = env_int_or_default("DIRECT_COPY_SSH_PUTFILE_MAX", 1024, 0,
                                     (int)((ECOPY_MAX_FRAME - (1u << 16)) / 1024u));
        g_ssh_putfile_max = (off_t)kib * 1024;
    }

    if (g_explicit_large_readers > 0 && g_explicit_large_writers > 0) {
        normalize_large_pipeline_config(g_explicit_large_readers,
                                        g_explicit_large_writers);
    } else {
        int readers = 0;
        int writers = 0;
        split_large_workers(g_large_worker_count, &readers, &writers);
        normalize_large_pipeline_config(readers, writers);
    }

    g_max_active_large_files = g_worker_count / g_large_worker_count;
    if (g_max_active_large_files < 1) {
        g_max_active_large_files = 1;
    }

    {
        long long chunk_mb = (long long)(g_chunk_size / (1024 * 1024));
        long long budget_mb = chunk_mb *
                              (long long)g_large_file_inflight *
                              (long long)g_max_active_large_files;
        if (budget_mb > MAX_LARGE_BUFFER_BUDGET_MB) {
            int clamped_inflight = (int)(MAX_LARGE_BUFFER_BUDGET_MB /
                                         (chunk_mb * (long long)g_max_active_large_files));
            if (clamped_inflight < 1) {
                clamped_inflight = 1;
            }
            fprintf(stderr,
                    "Warning: large-file buffer budget would be %lld MiB; clamping DIRECT_COPY_LARGE_FILE_INFLIGHT from %d to %d.\n",
                    budget_mb,
                    g_large_file_inflight,
                    clamped_inflight);
            g_large_file_inflight = clamped_inflight;
        }
    }
}

static void get_pipeline_thread_counts(int *reader_count, int *writer_count)
{
    int readers;
    int writers;

    init_runtime_config();

    readers = g_large_reader_count;
    writers = g_large_writer_count;

    if (reader_count) {
        *reader_count = readers;
    }
    if (writer_count) {
        *writer_count = writers;
    }
}

static void validate_runtime_config(void)
{
    int readers = 0;
    int writers = 0;

    init_runtime_config();
    get_pipeline_thread_counts(&readers, &writers);

    if (g_large_config_clamped) {
        fprintf(stderr,
                "Warning: requested large-file worker split exceeded DIRECT_COPY_MAX_WORKERS. Using %d readers + %d writers with a slot cost of %d so large files can make progress.\n",
                readers,
                writers,
                g_large_worker_count);
    }

    if (g_max_active_large_files < 2) {
        fprintf(stderr,
                "Warning: current large-file worker split (%d readers + %d writers = %d threads/file) reduces active large files to %d. Consider lowering DIRECT_COPY_LARGE_READERS or DIRECT_COPY_LARGE_WRITERS, or increasing DIRECT_COPY_MAX_WORKERS.\n",
                readers,
                writers,
                readers + writers,
                g_max_active_large_files);
    }

    if (readers > g_large_file_inflight) {
        fprintf(stderr,
                "Warning: DIRECT_COPY_LARGE_READERS=%d exceeds DIRECT_COPY_LARGE_FILE_INFLIGHT=%d. Some readers may spend time waiting for free chunk buffers.\n",
                readers,
                g_large_file_inflight);
    }
}

static off_t runtime_large_threshold(void)
{
    init_runtime_config();
    return g_large_threshold;
}

/*
 * A regular file is treated as sparse when the storage actually allocated to
 * it (st_blocks, in 512-byte units) is meaningfully smaller than its logical
 * size. Such files are copied through the hole-skipping path so that a file
 * which "looks" enormous (for example a multi-petabyte sparse image) only
 * moves and allocates its real data, instead of reading and writing terabytes
 * of zeros. A one-block slack avoids misclassifying files whose tail block is
 * partially filled.
 */
int workers_file_is_sparse(const struct stat *st)
{
    off_t allocated;

    if (!st || !S_ISREG(st->st_mode) || st->st_size <= 0) {
        return 0;
    }
    allocated = (off_t)st->st_blocks * 512;
    return allocated + (off_t)ALIGNMENT < st->st_size;
}

/* -------------------- queue helpers -------------------- */

static void free_file_task(file_task_t *task)
{
    if (!task) {
        return;
    }
    dir_handle_release(task->dir);
    /* Recycle the node instead of freeing it; drained in workers_stop(). */
    pthread_mutex_lock(&g_queue_lock);
    task->next = g_task_freelist;
    g_task_freelist = task;
    pthread_mutex_unlock(&g_queue_lock);
}

/*
 * Bytes a file will actually move: min(logical size, allocated blocks). This is
 * st_size for dense files and the allocated data for sparse files, so a huge
 * logical / tiny real sparse file does not wrongly float to the front.
 */
static uint64_t task_weight(const struct stat *st)
{
    off_t size = st->st_size > 0 ? st->st_size : 0;
    off_t alloc = (off_t)st->st_blocks * 512;
    if (alloc < 0) {
        alloc = 0;
    }
    off_t w = size < alloc ? size : alloc;
    return (uint64_t)w;
}

/* Ensure the heap can hold at least `need` entries. Caller holds g_queue_lock. */
static int heap_reserve(task_heap_t *h, size_t need)
{
    if (h->cap >= need) {
        return 0;
    }
    size_t ncap = h->cap ? h->cap * 2 : 64;
    while (ncap < need) {
        ncap *= 2;
    }
    file_task_t **ni = realloc(h->items, ncap * sizeof(*ni));
    if (!ni) {
        return -1;
    }
    h->items = ni;
    h->cap = ncap;
    return 0;
}

/* Push a task whose sched_key is set. Capacity must be reserved by the caller. */
static void heap_push(task_heap_t *h, file_task_t *t)
{
    size_t i = h->len++;
    uint64_t key = t->sched_key;
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (h->items[parent]->sched_key >= key) {
            break;
        }
        h->items[i] = h->items[parent];
        i = parent;
    }
    h->items[i] = t;
}

/* Remove and return the highest-key task. Caller ensures h->len > 0. */
static file_task_t *heap_pop_max(task_heap_t *h)
{
    file_task_t *top = h->items[0];
    file_task_t *node = h->items[--h->len];
    h->items[h->len] = NULL;
    if (h->len > 0) {
        size_t i = 0;
        uint64_t key = node->sched_key;
        for (;;) {
            size_t l = 2 * i + 1;
            size_t r = 2 * i + 2;
            size_t best = i;
            uint64_t best_key = key;
            if (l < h->len && h->items[l]->sched_key > best_key) {
                best = l;
                best_key = h->items[l]->sched_key;
            }
            if (r < h->len && h->items[r]->sched_key > best_key) {
                best = r;
            }
            if (best == i) {
                break;
            }
            h->items[i] = h->items[best];
            i = best;
        }
        h->items[i] = node;
    }
    return top;
}

static void enqueue_buffer(large_buffer_t **head, large_buffer_t **tail, large_buffer_t *buf)
{
    buf->next = NULL;
    if (*tail) {
        (*tail)->next = buf;
    } else {
        *head = buf;
    }
    *tail = buf;
}

static large_buffer_t *dequeue_buffer(large_buffer_t **head, large_buffer_t **tail)
{
    large_buffer_t *buf = *head;
    if (buf) {
        *head = buf->next;
        if (!*head) {
            *tail = NULL;
        }
        buf->next = NULL;
    }
    return buf;
}

static void mark_large_file_failed_locked(large_file_ctx_t *ctx)
{
    ctx->failed = 1;
    pthread_cond_broadcast(&ctx->free_cond);
    pthread_cond_broadcast(&ctx->ready_cond);
}

static void mark_worker_error(void)
{
    pthread_mutex_lock(&g_workers_error_lock);
    g_workers_error = 1;
    pthread_mutex_unlock(&g_workers_error_lock);
}

static void clear_worker_error(void)
{
    pthread_mutex_lock(&g_workers_error_lock);
    g_workers_error = 0;
    pthread_mutex_unlock(&g_workers_error_lock);
}

int workers_status(void)
{
    int v;
    pthread_mutex_lock(&g_workers_error_lock);
    v = g_workers_error;
    pthread_mutex_unlock(&g_workers_error_lock);
    return v;
}

/* -------------------- copy helpers -------------------- */

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/*
 * Non-cancellable I/O wrappers. glibc's pread/pwrite/read/write are deferred
 * cancellation points, so in a multi-threaded process they bracket every
 * syscall with __pthread_enable_asynccancel/__pthread_disable_asynccancel. Our
 * worker threads are never cancelled (only joined), so we issue the syscalls
 * directly to avoid that per-syscall bookkeeping. errno is still set by the
 * syscall() wrapper on error.
 *
 * Elsewhere these are the plain libc calls: syscall(2) is deprecated on Darwin
 * and there is no supported way to reach the non-cancellable variants, so the
 * bookkeeping is simply paid.
 */
#ifdef __linux__
static inline ssize_t pread_nocancel(int fd, void *buf, size_t count, off_t offset)
{
    return syscall(SYS_pread64, fd, buf, count, offset);
}

static inline ssize_t pwrite_nocancel(int fd, const void *buf, size_t count, off_t offset)
{
    return syscall(SYS_pwrite64, fd, buf, count, offset);
}

static inline ssize_t read_nocancel(int fd, void *buf, size_t count)
{
    return syscall(SYS_read, fd, buf, count);
}

static inline ssize_t write_nocancel(int fd, const void *buf, size_t count)
{
    return syscall(SYS_write, fd, buf, count);
}
#else
#define pread_nocancel  pread
#define pwrite_nocancel pwrite
#define read_nocancel   read
#define write_nocancel  write
#endif

static int copy_file_range_unsupported_errno_local(int err)
{
    return err == ENOSYS ||
           err == EXDEV ||
           err == EINVAL ||
           err == EOPNOTSUPP ||
           err == ENOTSUP ||
           err == EPERM;
}

static void advise_source_streaming(int fd)
{
    if (fd < 0) {
        return;
    }

    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
}

static void advise_dest_streaming(int fd)
{
    if (fd < 0) {
        return;
    }

    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
}

static void advise_source_consumed(int fd, off_t start, off_t len)
{
    if (fd < 0 || len <= 0) {
        return;
    }

    (void)posix_fadvise(fd, start, len, POSIX_FADV_DONTNEED);
}

/*
 * POSIX_FADV_DONTNEED on the already-copied source was issued once per chunk,
 * which is a lot of syscalls on big files. This coalesces contiguous consumed
 * ranges and only drops them once a sizeable span has accumulated. It is only
 * meaningful for buffered reads; with O_DIRECT the source never enters the page
 * cache, so callers skip it entirely.
 */
#define FADV_BATCH_BYTES (32 * 1024 * 1024)

typedef struct {
    int   fd;
    off_t start;
    off_t len;
} fadvise_batch_t;

static void fadvise_batch_init(fadvise_batch_t *b)
{
    b->fd = -1;
    b->start = 0;
    b->len = 0;
}

static void fadvise_batch_flush(fadvise_batch_t *b)
{
    if (b->fd >= 0 && b->len > 0) {
        (void)posix_fadvise(b->fd, b->start, b->len, POSIX_FADV_DONTNEED);
    }
    b->fd = -1;
    b->start = 0;
    b->len = 0;
}

static void fadvise_batch_add(fadvise_batch_t *b, int fd, off_t start, off_t len)
{
    if (fd < 0 || len <= 0) {
        return;
    }
    if (b->fd == fd && b->start + b->len == start) {
        b->len += len;
    } else {
        fadvise_batch_flush(b);
        b->fd = fd;
        b->start = start;
        b->len = len;
    }
    if (b->len >= FADV_BATCH_BYTES) {
        fadvise_batch_flush(b);
    }
}

/*
 * The reader/writer/serial copy loops run the hottest per-chunk code. Timing
 * every single read/write with clock_gettime() (and recording the ready-queue
 * depth on every chunk) showed up as a meaningful slice of user CPU under perf.
 * Instead we sample 1 in IO_SAMPLE_PERIOD iterations: syscall counts stay
 * exact, while the timing and queue-depth values become unbiased estimates
 * extrapolated by the sample period. The counter is per-thread so each worker
 * samples independently without sharing state.
 */
#define IO_SAMPLE_PERIOD 16u

static inline int io_should_sample(void)
{
    static __thread uint32_t io_sample_counter = 0;
    return (io_sample_counter++ % IO_SAMPLE_PERIOD) == 0u;
}

/*
 * Bounded string copy for per-file path fields. These are plain copies of
 * NUL-terminated paths; using snprintf("%s") for them dragged in the whole
 * vfprintf machinery, which was a visible cost on small-file trees.
 */
static void copy_path_field(char *dst, size_t dstsz, const char *src)
{
    size_t n = src ? strlen(src) : 0;
    if (n >= dstsz) {
        n = dstsz - 1;
    }
    if (n > 0) {
        memcpy(dst, src, n);
    }
    dst[n] = '\0';
}

/*
 * Reusable per-worker I/O buffer. The serial small-file path used to do an
 * aligned posix_memalign()/free() (plus first-touch page faults) for every
 * file; instead each worker keeps one buffer, grown on demand and reused across
 * files. Aligned to ALIGNMENT so it is valid for the O_DIRECT bulk path too.
 * Released per worker thread via thread_io_buffer_release().
 */
static __thread void  *g_tls_io_buf = NULL;
static __thread size_t g_tls_io_cap = 0;

static void *thread_io_buffer(size_t want)
{
    void *p = NULL;
    if (g_tls_io_buf && g_tls_io_cap >= want) {
        return g_tls_io_buf;
    }
    free(g_tls_io_buf);
    g_tls_io_buf = NULL;
    g_tls_io_cap = 0;
    if (want == 0) {
        want = ALIGNMENT;
    }
    if (posix_memalign(&p, ALIGNMENT, want) != 0) {
        return NULL;
    }
    g_tls_io_buf = p;
    g_tls_io_cap = want;
    return p;
}

static void thread_io_buffer_release(void)
{
    free(g_tls_io_buf);
    g_tls_io_buf = NULL;
    g_tls_io_cap = 0;
}

static void record_progress_bytes(uint64_t bytes, int use_current_file_stats)
{
    if (use_current_file_stats) {
        stats_advance_current_file(bytes);
    } else {
        stats_add_bytes(bytes);
    }
}

/*
 * The large writers all bump the same global byte counter once per chunk. Once
 * the stats mutex was gone, that shared atomic became a cacheline that ping-pongs
 * between every writer thread. Each writer instead accumulates locally and only
 * folds the total into the global counter every BYTES_FLUSH_THRESHOLD (and at
 * thread exit), trading slightly coarser live progress for far less contention.
 */
#define BYTES_FLUSH_THRESHOLD (16ULL * 1024 * 1024)

static __thread uint64_t g_tls_pending_bytes = 0;

static void progress_add_bytes_batched(uint64_t bytes)
{
    g_tls_pending_bytes += bytes;
    if (g_tls_pending_bytes >= BYTES_FLUSH_THRESHOLD) {
        stats_add_bytes(g_tls_pending_bytes);
        g_tls_pending_bytes = 0;
    }
}

static void progress_flush_bytes(void)
{
    if (g_tls_pending_bytes > 0) {
        stats_add_bytes(g_tls_pending_bytes);
        g_tls_pending_bytes = 0;
    }
}

static int copy_file_range_with_progress(int fd_in,
                                         int fd_out,
                                         off_t *in_off,
                                         off_t *out_off,
                                         size_t len,
                                         int use_current_file_stats)
{
    size_t remaining = len;

    while (remaining > 0) {
        size_t step = remaining;
        off_t old_in = *in_off;

        {
            uint64_t cfr_start_ns = monotonic_ns();
            ssize_t moved = copy_file_range(fd_in, in_off, fd_out, out_off, step, 0);
            stats_record_copy_file_range_io(monotonic_ns() - cfr_start_ns);
            if (moved < 0) {
                if (copy_file_range_unsupported_errno_local(errno)) {
                    return 1;
                }
                perror("copy_file_range");
                return -1;
            }
            if (moved == 0) {
                fprintf(stderr, "copy_file_range hit EOF early\n");
                return -1;
            }

            record_progress_bytes((uint64_t)moved, use_current_file_stats);
            stats_record_copy_file_range_call((uint64_t)moved);
            advise_source_consumed(fd_in, old_in, (off_t)moved);
            remaining -= (size_t)moved;
        }
    }

    return 0;
}

static int copy_tail_buffered_fds(int fd_in,
                                 int fd_out,
                                 off_t start,
                                 off_t end,
                                 int use_current_file_stats)
{
    if (start >= end) {
        return 0;
    }

    if (lseek(fd_in, start, SEEK_SET) < 0) {
        perror("lseek src");
        return -1;
    }

    if (lseek(fd_out, start, SEEK_SET) < 0) {
        perror("lseek dst");
        return -1;
    }

    {
        char buf[ALIGNMENT];
        off_t pos = start;

        while (pos < end) {
            off_t remain = end - pos;
            off_t this_len_off = (remain < (off_t)sizeof(buf)) ? remain : (off_t)sizeof(buf);
            size_t len = (size_t)this_len_off;
            off_t chunk_start = pos;

            uint64_t read_start_ns = monotonic_ns();
            ssize_t r = read(fd_in, buf, len);
            stats_record_read_io(monotonic_ns() - read_start_ns);
            if (r < 0) {
                perror("read tail");
                return -1;
            }
            if (r == 0) {
                break;
            }

            {
                size_t done = 0;
                while (done < (size_t)r) {
                    uint64_t write_start_ns = monotonic_ns();
                    ssize_t w = write(fd_out, buf + done, (size_t)r - done);
                    stats_record_write_io(monotonic_ns() - write_start_ns);
                    if (w < 0) {
                        perror("write tail");
                        return -1;
                    }
                    done += (size_t)w;
                }
            }

            pos += r;
            record_progress_bytes((uint64_t)r, use_current_file_stats);
            advise_source_consumed(fd_in, chunk_start, r);
        }
    }

    return 0;
}

static int copy_tail_buffered_at(int src_dir_fd,
                                 int dst_dir_fd,
                                 const char *name,
                                 const char *tmp_name,
                                 const char *src,
                                 const char *dst,
                                 const struct stat *src_st,
                                 off_t start,
                                 off_t end,
                                 int use_current_file_stats)
{
    if (start >= end) {
        return 0;
    }

    int fd_in = open_read_at_buffered(src_dir_fd, name, src, src_st);
    if (fd_in < 0) {
        return -1;
    }

    int fd_out = open_temp_write_existing_at_buffered(dst_dir_fd, tmp_name, dst);
    if (fd_out < 0) {
        close(fd_in);
        return -1;
    }

    advise_source_streaming(fd_in);
    advise_dest_streaming(fd_out);

    int rc = copy_tail_buffered_fds(fd_in, fd_out, start, end, use_current_file_stats);

    close(fd_in);
    close(fd_out);
    return rc;
}

/*
 * Copy a contiguous byte range [start, end) from fd_in to fd_out using
 * positional (buffered) I/O. pread/pwrite are used so the caller can keep
 * using lseek(SEEK_DATA/SEEK_HOLE) on fd_in without the file offset fighting
 * the data movement.
 */
static int copy_byte_range_buffered(int fd_in,
                                    int fd_out,
                                    off_t start,
                                    off_t end,
                                    void *buf,
                                    size_t bufcap,
                                    uint64_t *payload_bytes)
{
    off_t pos = start;
    fadvise_batch_t fadv;

    fadvise_batch_init(&fadv);

    while (pos < end) {
        off_t remain = end - pos;
        size_t len = (remain < (off_t)bufcap) ? (size_t)remain : bufcap;
        ssize_t r;
        size_t done = 0;

        int r_timed = io_should_sample();
        uint64_t read_start_ns = r_timed ? monotonic_ns() : 0;
        r = pread_nocancel(fd_in, buf, len, pos);
        stats_record_read_op();
        if (r_timed) {
            stats_record_read_time((monotonic_ns() - read_start_ns) * IO_SAMPLE_PERIOD);
        }
        if (r < 0) {
            perror("pread");
            return -1;
        }
        if (r == 0) {
            break;
        }

        while (done < (size_t)r) {
            int w_timed = io_should_sample();
            uint64_t write_start_ns = w_timed ? monotonic_ns() : 0;
            ssize_t w = pwrite_nocancel(fd_out, (char *)buf + done, (size_t)r - done, pos + (off_t)done);
            stats_record_write_op();
            if (w_timed) {
                stats_record_write_time((monotonic_ns() - write_start_ns) * IO_SAMPLE_PERIOD);
            }
            if (w < 0) {
                perror("pwrite");
                return -1;
            }
            done += (size_t)w;
        }

        fadvise_batch_add(&fadv, fd_in, pos, r);
        pos += r;
        record_progress_bytes((uint64_t)r, 1);
        if (payload_bytes) *payload_bytes += (uint64_t)r;
    }

    fadvise_batch_flush(&fadv);
    return 0;
}

/*
 * Copy only the data regions of a sparse file, skipping holes, so the
 * destination keeps the same sparseness and no time is spent moving zeros.
 * Holes are discovered with lseek(SEEK_DATA/SEEK_HOLE). If the underlying
 * filesystem does not support that interface, the remaining range is copied
 * densely, which is still correct (holes simply read back as zeros).
 */
static int copy_data_extents_buffered(int fd_in,
                                      int fd_out,
                                      off_t size,
                                      void *buf,
                                      size_t bufcap,
                                      uint64_t *payload_bytes)
{
    off_t pos = 0;

    while (pos < size) {
        off_t data = lseek(fd_in, pos, SEEK_DATA);
        if (data < 0) {
            if (errno == ENXIO) {
                /* No more data before EOF; the remainder is a hole. */
                break;
            }
            if (errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
                return copy_byte_range_buffered(fd_in, fd_out, pos, size, buf,
                                                bufcap, payload_bytes);
            }
            perror("lseek SEEK_DATA");
            return -1;
        }
        if (data >= size) {
            break;
        }

        off_t hole = lseek(fd_in, data, SEEK_HOLE);
        if (hole < 0) {
            perror("lseek SEEK_HOLE");
            return -1;
        }
        if (hole > size) {
            hole = size;
        }

        if (copy_byte_range_buffered(fd_in, fd_out, data, hole, buf, bufcap,
                                     payload_bytes) != 0) {
            return -1;
        }
        pos = hole;
    }

    return 0;
}

/*
 * Sparse-aware copy. Always uses buffered I/O and the temp-file-plus-rename
 * strategy (sparse files are rare enough that crash-atomicity is worth more
 * than the in-place fast path), copies only data extents, and ftruncate()s the
 * destination to the exact source size so trailing holes are preserved.
 */
static int copy_file_sparse(file_task_t *task, uint64_t *payload_bytes)
{
    int fd_in = -1;
    int fd_out = -1;
    int out_direct = 0;
    int target_created = 0;
    int rc = -1;
    void *buf = NULL;
    size_t bufcap;
    off_t size = task->src_st.st_size;
    char tmp_name[PATH_MAX] = "";

    bufcap = (g_chunk_size > 0) ? (size_t)g_chunk_size : (size_t)(1024 * 1024);
    stats_set_current_file(task->src, (uint64_t)size, 0);

    fd_in = open_read_at_buffered(task->dir->src_fd,
                                  task->name,
                                  task->src,
                                  &task->src_st);
    if (fd_in < 0) {
        goto out;
    }

    fd_out = create_temp_write_at_maybe_direct(task->dir->dst_fd,
                                               task->dst,
                                               task->src_st.st_mode & 07777,
                                               tmp_name,
                                               sizeof(tmp_name),
                                               &out_direct);
    if (fd_out < 0) {
        goto out;
    }
    target_created = 1;

    if (out_direct) {
        /* Reopen the temp buffered: hole boundaries are not guaranteed to be
         * O_DIRECT aligned, and sparse files do not need direct I/O. */
        close(fd_out);
        fd_out = open_temp_write_existing_at_buffered(task->dir->dst_fd, tmp_name, task->dst);
        if (fd_out < 0) {
            fd_out = -1;
            goto out;
        }
    }

    advise_source_streaming(fd_in);
    advise_dest_streaming(fd_out);

    buf = thread_io_buffer(bufcap);
    if (!buf) {
        fprintf(stderr, "thread_io_buffer failed\n");
        goto out;
    }

    if (copy_data_extents_buffered(fd_in, fd_out, size, buf, bufcap,
                                   payload_bytes) != 0) {
        goto out;
    }

    /* Set the exact final size so a trailing hole is preserved and the file is
     * never shorter than the source. */
    if (ftruncate(fd_out, size) != 0) {
        perror("ftruncate");
        goto out;
    }

    if (finalize_copied_file_fd(fd_out, task->dst, &task->src_st) != 0) {
        goto out;
    }
    if (close(fd_out) != 0) {
        fd_out = -1;
        perror(task->dst);
        goto out;
    }
    fd_out = -1;

    if (rename_temp_to_final_at(task->dir->dst_fd, tmp_name, task->name, task->dst) != 0) {
        goto out;
    }
    target_created = 0;
    rc = 0;

out:
    if (fd_in >= 0) {
        close(fd_in);
    }
    if (fd_out >= 0) {
        close(fd_out);
    }
    if (target_created) {
        unlink_temp_at(task->dir->dst_fd, tmp_name);
    }
    stats_clear_current_file(task->src);
    return rc;
}

static int copy_file_serial_small(file_task_t *task, uint64_t *payload_bytes)
{
    int fd_in = -1;
    int fd_out = -1;
    int in_direct = 0;
    int out_direct = 0;
    void *buf = NULL;
    int copy_range_available;
    off_t size;
    off_t bulk_end;
    off_t pos = 0;
    int rc = -1;
    int target_created = 0;
    int inplace = copy_policy_small_inplace();
    const char *write_name;
    char tmp_name[PATH_MAX] = "";
    fadvise_batch_t fadv;

    fadvise_batch_init(&fadv);
    init_runtime_config();

    if (workers_file_is_sparse(&task->src_st)) {
        return copy_file_sparse(task, payload_bytes);
    }

    copy_range_available = copy_file_range_enabled();
    size = task->src_st.st_size;
    bulk_end = (size / ALIGNMENT) * ALIGNMENT;
    stats_set_current_file(task->src, (uint64_t)size, 0);

    fd_in = open_read_at_maybe_direct(task->dir->src_fd,
                                      task->name,
                                      task->src,
                                      &task->src_st,
                                      &in_direct);
    if (fd_in < 0) {
        goto out;
    }

    if (inplace) {
        fd_out = create_final_write_at_maybe_direct(task->dir->dst_fd,
                                                    task->name,
                                                    task->dst,
                                                    task->src_st.st_mode & 07777,
                                                    &out_direct);
        write_name = task->name;
    } else {
        fd_out = create_temp_write_at_maybe_direct(task->dir->dst_fd,
                                                   task->dst,
                                                   task->src_st.st_mode & 07777,
                                                   tmp_name,
                                                   sizeof(tmp_name),
                                                   &out_direct);
        write_name = tmp_name;
    }
    if (fd_out < 0) {
        goto out;
    }
    target_created = 1;

    if (ftruncate(fd_out, size) != 0) {
        perror("ftruncate");
        goto out;
    }

    if (!in_direct) {
        advise_source_streaming(fd_in);
    }
    if (!out_direct) {
        advise_dest_streaming(fd_out);
    }

    buf = thread_io_buffer((size_t)g_chunk_size);
    if (!buf) {
        fprintf(stderr, "thread_io_buffer failed\n");
        goto out;
    }

    while (pos < bulk_end) {
        off_t remain = bulk_end - pos;
        off_t this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
        size_t len = (size_t)this_len_off;

        if (!in_direct && !out_direct && copy_range_available) {
            off_t in_off = pos;
            off_t out_off = pos;
            int cfr_rc = copy_file_range_with_progress(fd_in, fd_out, &in_off, &out_off, len, 1);
            if (cfr_rc == 0) {
                pos += this_len_off;
                continue;
            }
            if (cfr_rc < 0) {
                goto out;
            }
            stats_record_copy_file_range_fallback();
            copy_range_available = 0;
        }

        {
            int r_timed = io_should_sample();
            uint64_t read_start_ns = r_timed ? monotonic_ns() : 0;
            ssize_t r = read_nocancel(fd_in, buf, len);
            stats_record_read_op();
            if (r_timed) {
                stats_record_read_time((monotonic_ns() - read_start_ns) * IO_SAMPLE_PERIOD);
            }
            if (r < 0) {
                perror("read");
                goto out;
            }
            if ((size_t)r != len) {
                fprintf(stderr, "short read on %s: expected %zu got %zd\n", task->src, len, r);
                goto out;
            }
        }

        {
            size_t done = 0;
            while (done < len) {
                int w_timed = io_should_sample();
                uint64_t write_start_ns = w_timed ? monotonic_ns() : 0;
                ssize_t w = write_nocancel(fd_out, (char *)buf + done, len - done);
                stats_record_write_op();
                if (w_timed) {
                    stats_record_write_time((monotonic_ns() - write_start_ns) * IO_SAMPLE_PERIOD);
                }
                if (w < 0) {
                    perror("write");
                    goto out;
                }
                done += (size_t)w;
            }
        }

        if (!in_direct) {
            fadvise_batch_add(&fadv, fd_in, pos, (off_t)len);
        }
        pos += this_len_off;
        record_progress_bytes((uint64_t)len, 1);
    }

    fadvise_batch_flush(&fadv);

    if (!in_direct && !out_direct) {
        if (copy_tail_buffered_fds(fd_in, fd_out, bulk_end, size, 1) != 0) {
            goto out;
        }
        if (finalize_copied_file_fd(fd_out, task->dst, &task->src_st) != 0) {
            goto out;
        }
        if (close(fd_out) != 0) {
            fd_out = -1;
            perror(task->dst);
            goto out;
        }
        fd_out = -1;
        if (!inplace &&
            rename_temp_to_final_at(task->dir->dst_fd, tmp_name, task->name, task->dst) != 0) {
            goto out;
        }
        target_created = 0;
        rc = 0;
        goto out;
    }

    close(fd_out);
    fd_out = -1;

    if (copy_tail_buffered_at(task->dir->src_fd,
                              task->dir->dst_fd,
                              task->name,
                              write_name,
                              task->src,
                              task->dst,
                              &task->src_st,
                              bulk_end,
                              size,
                              1) != 0) {
        goto out;
    }

    if (preserve_path_metadata_at(task->dir->dst_fd,
                                  write_name,
                                  task->dst,
                                  &task->src_st,
                                  S_IFREG) != 0) {
        goto out;
    }
    if (!inplace &&
        rename_temp_to_final_at(task->dir->dst_fd, tmp_name, task->name, task->dst) != 0) {
        goto out;
    }
    target_created = 0;
    rc = 0;
    if (payload_bytes) *payload_bytes = (uint64_t)size;

out:
    if (fd_in >= 0) {
        close(fd_in);
    }
    if (fd_out >= 0) {
        close(fd_out);
    }
    if (target_created) {
        /*
         * Clean up the unfinished destination. For temp+rename that is the temp
         * file; for in-place writes the final name has already been truncated,
         * so removing the partial copy avoids leaving corrupt data behind.
         */
        unlink_temp_at(task->dir->dst_fd, inplace ? task->name : tmp_name);
    }
    stats_clear_current_file(task->src);
    return rc;
}

/* -------------------- large-file pipeline -------------------- */

static void *large_reader_main(void *arg)
{
    large_file_ctx_t *ctx = (large_file_ctx_t *)arg;

    for (;;) {
        large_buffer_t *buf;
        off_t offset;
        off_t remain;
        off_t this_len_off;
        size_t len;

        pthread_mutex_lock(&ctx->lock);
        while (!ctx->failed && !ctx->free_head && ctx->next_read_offset < ctx->bulk_end) {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            pthread_cond_wait(&ctx->free_cond, &ctx->lock);
            if (g_collect_wait_timing) {
                stats_record_reader_buffer_wait_ns(monotonic_ns() - wait_start_ns);
            }
        }

        if (ctx->failed || ctx->next_read_offset >= ctx->bulk_end) {
            ctx->active_readers--;
            if (ctx->active_readers == 0) {
                ctx->read_done = 1;
                pthread_cond_broadcast(&ctx->ready_cond);
            }
            pthread_mutex_unlock(&ctx->lock);
            break;
        }

        buf = dequeue_buffer(&ctx->free_head, &ctx->free_tail);
        if (ctx->free_count > 0) {
            ctx->free_count--;
        }
        offset = ctx->next_read_offset;
        remain = ctx->bulk_end - offset;
        this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
        len = (size_t)this_len_off;
        ctx->next_read_offset += this_len_off;
        pthread_mutex_unlock(&ctx->lock);

        {
            int timed = io_should_sample();
            uint64_t read_start_ns = timed ? monotonic_ns() : 0;
            ssize_t r = pread_nocancel(ctx->fd_in, buf->data, len, offset);
            stats_record_read_op();
            if (timed) {
                stats_record_read_time((monotonic_ns() - read_start_ns) * IO_SAMPLE_PERIOD);
            }
            if (r < 0) {
                perror("pread");
                pthread_mutex_lock(&ctx->lock);
                mark_large_file_failed_locked(ctx);
                enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
                ctx->free_count++;
                ctx->active_readers--;
                if (ctx->active_readers == 0) {
                    ctx->read_done = 1;
                }
                pthread_mutex_unlock(&ctx->lock);
                break;
            }
            if ((size_t)r != len) {
                fprintf(stderr, "short pread at off %lld: expected %zu got %zd\n",
                        (long long)offset, len, r);
                pthread_mutex_lock(&ctx->lock);
                mark_large_file_failed_locked(ctx);
                enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
                ctx->free_count++;
                ctx->active_readers--;
                if (ctx->active_readers == 0) {
                    ctx->read_done = 1;
                }
                pthread_mutex_unlock(&ctx->lock);
                break;
            }
        }

        buf->offset = offset;
        buf->len = len;
        /* With O_DIRECT the source never enters the page cache, so dropping it
         * is a wasted syscall per chunk; only advise for buffered reads. */
        if (!ctx->in_direct) {
            advise_source_consumed(ctx->fd_in, offset, (off_t)len);
        }

        pthread_mutex_lock(&ctx->lock);
        if (ctx->failed) {
            enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
            ctx->free_count++;
            pthread_cond_broadcast(&ctx->free_cond);
            ctx->active_readers--;
            if (ctx->active_readers == 0) {
                ctx->read_done = 1;
            }
            pthread_mutex_unlock(&ctx->lock);
            break;
        }
        enqueue_buffer(&ctx->ready_head, &ctx->ready_tail, buf);
        ctx->ready_count++;
        if (io_should_sample()) {
            stats_record_ready_queue_depth(ctx->ready_count);
        }
        pthread_cond_signal(&ctx->ready_cond);
        pthread_mutex_unlock(&ctx->lock);
    }

    return NULL;
}

static void *large_writer_main(void *arg)
{
    large_file_ctx_t *ctx = (large_file_ctx_t *)arg;

    for (;;) {
        large_buffer_t *buf;

        pthread_mutex_lock(&ctx->lock);
        while (!ctx->failed && !ctx->ready_head && !ctx->read_done) {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            pthread_cond_wait(&ctx->ready_cond, &ctx->lock);
            if (g_collect_wait_timing) {
                stats_record_writer_data_wait_ns(monotonic_ns() - wait_start_ns);
            }
        }

        if ((ctx->failed && !ctx->ready_head) || (!ctx->ready_head && ctx->read_done)) {
            ctx->active_writers--;
            pthread_mutex_unlock(&ctx->lock);
            break;
        }

        buf = dequeue_buffer(&ctx->ready_head, &ctx->ready_tail);
        if (ctx->ready_count > 0) {
            ctx->ready_count--;
        }
        if (io_should_sample()) {
            stats_record_ready_queue_depth(ctx->ready_count);
        }
        pthread_mutex_unlock(&ctx->lock);

        {
            size_t done = 0;
            int failed = 0;

            while (done < buf->len) {
                int timed = io_should_sample();
                uint64_t write_start_ns = timed ? monotonic_ns() : 0;
                ssize_t w = pwrite_nocancel(ctx->fd_out,
                                            (char *)buf->data + done,
                                            buf->len - done,
                                            buf->offset + (off_t)done);
                stats_record_write_op();
                if (timed) {
                    stats_record_write_time((monotonic_ns() - write_start_ns) * IO_SAMPLE_PERIOD);
                }
                if (w < 0) {
                    perror("pwrite");
                    failed = 1;
                    break;
                }
                if (w == 0) {
                    fprintf(stderr, "zero pwrite at off %lld\n",
                            (long long)(buf->offset + (off_t)done));
                    failed = 1;
                    break;
                }
                done += (size_t)w;
            }

            if (!failed) {
                progress_add_bytes_batched((uint64_t)buf->len);
            }

            pthread_mutex_lock(&ctx->lock);
            if (failed) {
                mark_large_file_failed_locked(ctx);
            }
            enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
            ctx->free_count++;
            pthread_cond_signal(&ctx->free_cond);
            pthread_mutex_unlock(&ctx->lock);

            if (failed) {
                break;
            }
        }
    }

    progress_flush_bytes();
    return NULL;
}

static void free_large_buffers(large_buffer_t *head)
{
    while (head) {
        large_buffer_t *next = head->next;
        free(head->data);
        free(head);
        head = next;
    }
}

static void finish_large_file_ctx(large_file_ctx_t *ctx)
{
    int rc = 0;
    int i;

    for (i = 0; i < ctx->readers_started; i++) {
        pthread_join(ctx->reader_threads[i], NULL);
    }
    for (i = 0; i < ctx->writers_started; i++) {
        pthread_join(ctx->writer_threads[i], NULL);
    }

    if (!ctx->failed) {
        if (copy_tail_buffered_at(ctx->dir->src_fd,
                                  ctx->dir->dst_fd,
                                  ctx->name,
                                  ctx->tmp_name,
                                  ctx->src,
                                  ctx->dst,
                                  &ctx->src_st,
                                  ctx->bulk_end,
                                  ctx->src_st.st_size,
                                  0) != 0) {
            rc = -1;
        } else if (finalize_copied_file_fd(ctx->fd_out, ctx->dst, &ctx->src_st) != 0) {
            rc = -1;
        } else if (close(ctx->fd_out) != 0) {
            ctx->fd_out = -1;
            perror(ctx->dst);
            rc = -1;
        } else if (rename_temp_to_final_at(ctx->dir->dst_fd,
                                           ctx->tmp_name,
                                           ctx->name,
                                           ctx->dst) != 0) {
            ctx->fd_out = -1;
            rc = -1;
        } else {
            ctx->fd_out = -1;
            telemetry_note_file(TRANSFER_LARGE,
                                (uint64_t)ctx->src_st.st_size,
                                (uint64_t)ctx->src_st.st_size,
                                monotonic_ns() - ctx->service_start_ns);
            telemetry_flush_thread();
            /* Local temp+rename: the file is durable and at its final path. */
            if (verify_queue_file(ctx->src, ctx->dst, &ctx->src_st, 0, 1) != 0) {
                fprintf(stderr, "ecopy: unable to queue verification for %s\n",
                        ctx->dst);
                rc = -1;
            } else {
                stats_inc_files_copied();
            }
        }
    } else {
        rc = -1;
    }

    if (rc != 0) {
        mark_worker_error();
    }

    if (ctx->fd_in >= 0) {
        close(ctx->fd_in);
    }
    if (ctx->fd_out >= 0) {
        close(ctx->fd_out);
    }
    if (rc != 0) {
        unlink_temp_at(ctx->dir->dst_fd, ctx->tmp_name);
    }
    dir_handle_release(ctx->dir);
    free_large_buffers(ctx->free_head);
    free_large_buffers(ctx->ready_head);
    free(ctx->reader_threads);
    free(ctx->writer_threads);
    pthread_mutex_destroy(&ctx->lock);
    pthread_cond_destroy(&ctx->free_cond);
    pthread_cond_destroy(&ctx->ready_cond);

    pthread_mutex_lock(&g_queue_lock);
    if (g_large_workers_active > 0) {
        g_large_workers_active--;
    }
    pthread_cond_broadcast(&g_queue_cond);
    pthread_cond_broadcast(&g_large_done_cond);
    pthread_mutex_unlock(&g_queue_lock);

    free(ctx);
}

static void *large_finalizer_main(void *arg)
{
    large_file_ctx_t *ctx = (large_file_ctx_t *)arg;
    finish_large_file_ctx(ctx);
    return NULL;
}

static int start_large_file_copy(file_task_t *task)
{
    large_file_ctx_t *ctx = calloc(1, sizeof(*ctx));
    pthread_t finalizer_thread;
    int i;

    if (!ctx) {
        perror("calloc");
        return -1;
    }

    snprintf(ctx->src, sizeof(ctx->src), "%s", task->src);
    snprintf(ctx->dst, sizeof(ctx->dst), "%s", task->dst);
    snprintf(ctx->name, sizeof(ctx->name), "%s", task->name);
    ctx->src_st = task->src_st;
    ctx->service_start_ns = monotonic_ns();
    ctx->bulk_end = (task->src_st.st_size / ALIGNMENT) * ALIGNMENT;
    ctx->next_read_offset = 0;
    dir_handle_retain(task->dir);
    ctx->dir = task->dir;
    ctx->fd_in = -1;
    ctx->fd_out = -1;
    pthread_mutex_init(&ctx->lock, NULL);
    pthread_cond_init(&ctx->free_cond, NULL);
    pthread_cond_init(&ctx->ready_cond, NULL);

    get_pipeline_thread_counts(&ctx->reader_count, &ctx->writer_count);
    ctx->active_readers = ctx->reader_count;
    ctx->active_writers = ctx->writer_count;

    ctx->reader_threads = calloc((size_t)ctx->reader_count, sizeof(*ctx->reader_threads));
    ctx->writer_threads = calloc((size_t)ctx->writer_count, sizeof(*ctx->writer_threads));
    if (!ctx->reader_threads || !ctx->writer_threads) {
        perror("calloc");
        goto fail;
    }

    ctx->fd_in = open_read_at_maybe_direct(ctx->dir->src_fd,
                                           ctx->name,
                                           ctx->src,
                                           &ctx->src_st,
                                           &ctx->in_direct);
    if (ctx->fd_in < 0) {
        goto fail;
    }

    ctx->fd_out = create_temp_write_at_maybe_direct(ctx->dir->dst_fd,
                                                    ctx->dst,
                                                    ctx->src_st.st_mode & 07777,
                                                    ctx->tmp_name,
                                                    sizeof(ctx->tmp_name),
                                                    &ctx->out_direct);
    if (ctx->fd_out < 0) {
        goto fail;
    }

    /*
     * Preallocate the whole file up front. ftruncate() only sets i_size and
     * leaves a hole, so every O_DIRECT pwrite into that hole has to allocate
     * blocks on the fly (xfs_bmapi_write -> free-space btree walk). As the
     * filesystem fills and fragments, that per-write allocation dominates and
     * throughput collapses late in a run. fallocate() does one large allocation
     * (unwritten extents); subsequent writes just convert unwritten -> written,
     * skipping the allocator. Fall back to ftruncate() if the filesystem does
     * not support fallocate.
     */
    if (ctx->src_st.st_size > 0) {
        /*
         * Sparse files never normally reach the large-file pipeline (they are
         * routed to the serial hole-skipping path), but guard against ever
         * fallocate()-ing the full logical size of a sparse file: a 5 PB sparse
         * image would otherwise try to physically reserve 5 PB. For the sparse
         * case fall back to sizing with ftruncate() only.
         */
        if (workers_file_is_sparse(&ctx->src_st)) {
            if (ftruncate(ctx->fd_out, ctx->src_st.st_size) != 0) {
                perror("ftruncate");
                goto fail;
            }
        } else if (fallocate(ctx->fd_out, 0, 0, ctx->src_st.st_size) != 0) {
            if (errno != EOPNOTSUPP && errno != ENOSYS) {
                perror("fallocate");
                goto fail;
            }
            if (ftruncate(ctx->fd_out, ctx->src_st.st_size) != 0) {
                perror("ftruncate");
                goto fail;
            }
        }
    }

    for (i = 0; i < g_large_file_inflight; i++) {
        large_buffer_t *buf = calloc(1, sizeof(*buf));
        if (!buf) {
            perror("calloc");
            goto fail;
        }
        if (posix_memalign(&buf->data, ALIGNMENT, (size_t)g_chunk_size) != 0) {
            fprintf(stderr, "posix_memalign failed\n");
            free(buf);
            goto fail;
        }
        buf->cap = (size_t)g_chunk_size;
        enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
        ctx->free_count++;
        stats_record_large_chunk_buffer_alloc();
    }

    if (!ctx->in_direct) {
        advise_source_streaming(ctx->fd_in);
    }
    if (!ctx->out_direct) {
        advise_dest_streaming(ctx->fd_out);
    }

    for (i = 0; i < ctx->reader_count; i++) {
        if (pthread_create(&ctx->reader_threads[i], NULL, large_reader_main, ctx) != 0) {
            perror("pthread_create");
            goto fail_started;
        }
        ctx->readers_started++;
    }
    for (i = 0; i < ctx->writer_count; i++) {
        if (pthread_create(&ctx->writer_threads[i], NULL, large_writer_main, ctx) != 0) {
            perror("pthread_create");
            goto fail_started;
        }
        ctx->writers_started++;
    }
    if (pthread_create(&finalizer_thread, NULL, large_finalizer_main, ctx) != 0) {
        perror("pthread_create");
        goto fail_started;
    }
    pthread_detach(finalizer_thread);
    return 0;

fail_started:
    pthread_mutex_lock(&ctx->lock);
    mark_large_file_failed_locked(ctx);
    pthread_mutex_unlock(&ctx->lock);
    finish_large_file_ctx(ctx);
    /*
     * finish_large_file_ctx() synchronously releases the large-file scheduler
     * slot and records the worker error for this failed startup. Returning
     * success here tells the caller there is no slot left for it to release.
     */
    return 0;

fail:
    if (ctx->fd_in >= 0) {
        close(ctx->fd_in);
    }
    if (ctx->fd_out >= 0) {
        close(ctx->fd_out);
    }
    unlink_temp_at(ctx->dir ? ctx->dir->dst_fd : AT_FDCWD, ctx->tmp_name);
    dir_handle_release(ctx->dir);
    free_large_buffers(ctx->free_head);
    free_large_buffers(ctx->ready_head);
    free(ctx->reader_threads);
    free(ctx->writer_threads);
    pthread_mutex_destroy(&ctx->lock);
    pthread_cond_destroy(&ctx->free_cond);
    pthread_cond_destroy(&ctx->ready_cond);
    free(ctx);
    return -1;
}

/* -------------------- scheduler -------------------- */

static int total_worker_slots_used_locked(void)
{
    return (int)g_small_workers_active + (int)(g_large_workers_active * (uint64_t)g_large_worker_count);
}

static work_claim_t dequeue_work(void)
{
    work_claim_t claim;
    memset(&claim, 0, sizeof(claim));

    pthread_mutex_lock(&g_queue_lock);

    for (;;) {
        int total_slots_used = total_worker_slots_used_locked();

        if (g_large_heap.len > 0 &&
            (int)g_large_workers_active < g_max_active_large_files &&
            total_slots_used + g_large_worker_count <= g_worker_count) {
            claim.kind = WORK_LARGE_FILE_START;
            claim.file_task = heap_pop_max(&g_large_heap);
            g_large_workers_active++;
            pthread_cond_signal(&g_space_cond);
            break;
        }

        if (g_small_heap.len > 0 &&
            (int)g_small_workers_active < g_small_worker_limit &&
            total_slots_used + 1 <= g_worker_count) {
            claim.kind = WORK_SMALL_FILE;
            claim.file_task = heap_pop_max(&g_small_heap);
            g_small_workers_active++;
            pthread_cond_signal(&g_space_cond);
            break;
        }

        if (g_small_heap.len == 0 && g_large_heap.len == 0 &&
            g_queue_done && g_large_workers_active == 0 && g_small_workers_active == 0) {
            break;
        }

        {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            pthread_cond_wait(&g_queue_cond, &g_queue_lock);
            if (g_collect_wait_timing) {
                stats_record_queue_wait_ns(monotonic_ns() - wait_start_ns);
            }
        }
    }

    pthread_mutex_unlock(&g_queue_lock);
    return claim;
}

/* -------------------- remote (SSH) destination copy -------------------- */

/*
 * Small-file fast path: read the whole file locally and ship it as a single
 * fire-and-forget PUTFILE frame (no OPEN/COMMIT round trip). This is the main
 * lever for gazillion-tiny-file trees, where per-file round trips - not
 * bandwidth - are the bottleneck. Returns 0 on success, -1 on local read
 * error (transport errors surface at the next barrier).
 */
static int copy_file_remote_putfile(file_task_t *task, off_t size)
{
    int fd_in;
    void *buf;
    off_t pos = 0;

    fd_in = open_read_at_buffered(task->dir->src_fd, task->name, task->src, &task->src_st);
    if (fd_in < 0) {
        return -1;
    }

    buf = thread_io_buffer((size_t)(size > 0 ? size : 1));
    if (!buf) {
        fprintf(stderr, "thread_io_buffer failed\n");
        close(fd_in);
        return -1;
    }

    while (pos < size) {
        ssize_t r = pread_nocancel(fd_in, (char *)buf + pos, (size_t)(size - pos), pos);
        if (r < 0) { perror("pread"); close(fd_in); return -1; }
        if (r == 0) break; /* file shrank under us; send what we have */
        pos += r;
    }
    close(fd_in);

    if (sshx_putfile(task->dst, &task->src_st, task->dir->src_mode,
                     buf, (size_t)pos,
                     copy_policy_small_inplace()) != 0) {
        return -1;
    }
    record_progress_bytes((uint64_t)pos, 1);
    return 0;
}

/*
 * Stream one source file to the remote peer. Reads the source locally (O_DIRECT
 * where possible; buffered for sparse files so SEEK_DATA/SEEK_HOLE works) and
 * emits pipelined WRITE frames. Sparse files send only their data extents and a
 * trailing FTRUNCATE so holes are preserved. The COMMIT is the synchronization
 * point that reports any deferred server-side write error.
 *
 * Concurrency across files (many workers each streaming into the shared,
 * pipelined channel) is what hides link latency; a single file is streamed
 * sequentially.
 */
static int copy_file_remote(file_task_t *task, uint64_t *payload_bytes)
{
    int fd_in = -1;
    int in_direct = 0;
    int rc = -1;
    void *buf = NULL;
    off_t size = task->src_st.st_size;
    int sparse = workers_file_is_sparse(&task->src_st);
    size_t chunk = (g_chunk_size > 0) ? (size_t)g_chunk_size : (size_t)(1024 * 1024);
    sshx_file_t *f = NULL;

    init_runtime_config();
    stats_set_current_file(task->src, (uint64_t)size, 0);

    /* Non-sparse small files go in one fire-and-forget frame. Sparse files stay
     * on the streamed path so their holes are preserved on the far side. */
    if (!sparse && size <= g_ssh_putfile_max) {
        int prc = copy_file_remote_putfile(task, size);
        if (prc == 0 && payload_bytes) *payload_bytes = (uint64_t)size;
        stats_clear_current_file(task->src);
        return prc;
    }

    if (sparse) {
        fd_in = open_read_at_buffered(task->dir->src_fd, task->name, task->src, &task->src_st);
    } else {
        fd_in = open_read_at_maybe_direct(task->dir->src_fd, task->name, task->src,
                                          &task->src_st, &in_direct);
    }
    if (fd_in < 0) {
        goto out;
    }

    f = sshx_file_begin(task->dst, task->src_st.st_mode & 07777,
                        task->dir->src_mode, size, sparse,
                        copy_policy_small_inplace());
    if (!f) {
        goto out;
    }

    buf = thread_io_buffer(chunk);
    if (!buf) {
        fprintf(stderr, "thread_io_buffer failed\n");
        goto out;
    }

    if (sparse) {
        off_t pos = 0;
        while (pos < size) {
            off_t data = lseek(fd_in, pos, SEEK_DATA);
            if (data < 0) {
                if (errno == ENXIO) break;
                if (errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
                    data = pos; /* no hole support: treat rest as data */
                } else {
                    perror("lseek SEEK_DATA");
                    goto out;
                }
            }
            if (data >= size) break;
            off_t hole = lseek(fd_in, data, SEEK_HOLE);
            if (hole < 0) { perror("lseek SEEK_HOLE"); goto out; }
            if (hole > size) hole = size;

            off_t p = data;
            while (p < hole) {
                off_t remain = hole - p;
                size_t want = (remain < (off_t)chunk) ? (size_t)remain : chunk;
                ssize_t r = pread_nocancel(fd_in, buf, want, p);
                if (r < 0) { perror("pread"); goto out; }
                if (r == 0) break;
                if (sshx_file_write(f, buf, (size_t)r, p) != 0) goto out;
                p += r;
                record_progress_bytes((uint64_t)r, 1);
                if (payload_bytes) *payload_bytes += (uint64_t)r;
            }
            pos = hole;
        }
        if (sshx_file_ftruncate(f, size) != 0) {
            goto out;
        }
    } else {
        off_t pos = 0;
        while (pos < size) {
            /* Chunk-aligned count keeps O_DIRECT reads valid up to EOF. */
            ssize_t r = pread_nocancel(fd_in, buf, chunk, pos);
            if (r < 0) { perror("pread"); goto out; }
            if (r == 0) break;
            if (sshx_file_write(f, buf, (size_t)r, pos) != 0) goto out;
            pos += r;
            record_progress_bytes((uint64_t)r, 1);
            if (payload_bytes) *payload_bytes += (uint64_t)r;
        }
    }

    if (sshx_file_commit(f, &task->src_st) != 0) {
        f = NULL; /* commit frees the handle even on failure */
        goto out;
    }
    f = NULL;
    rc = 0;

out:
    if (f) {
        sshx_file_abort(f);
    }
    if (fd_in >= 0) {
        close(fd_in);
    }
    stats_clear_current_file(task->src);
    return rc;
}

/* -------------------- worker threads -------------------- */

static void *worker_main(void *arg)
{
    /* Bind this worker to one SSH connection of the pool so a streamed file's
     * OPEN/WRITE/COMMIT frames all land on the same server (a no-op locally). */
    sshx_bind_thread((int)(intptr_t)arg);

    for (;;) {
        work_claim_t claim = dequeue_work();
        if (claim.kind == WORK_NONE) {
            break;
        }

        if (claim.kind == WORK_SMALL_FILE) {
            uint64_t service_start_ns = monotonic_ns();
            uint64_t payload_bytes = 0;
            int sparse = workers_file_is_sparse(&claim.file_task->src_st);
            int ok = sshx_active()
                         ? (copy_file_remote(claim.file_task, &payload_bytes) == 0)
                         : (copy_file_serial_small(claim.file_task, &payload_bytes) == 0);
            if (ok) {
                transfer_class_t cls = sparse ? TRANSFER_SPARSE
                    : (claim.file_task->src_st.st_size >= g_large_threshold
                           ? TRANSFER_LARGE : TRANSFER_SMALL);
                telemetry_note_file(cls,
                                    (uint64_t)claim.file_task->src_st.st_size,
                                    payload_bytes,
                                    monotonic_ns() - service_start_ns);
                /* Fire-and-forget remote PUTFILE frames are not materialized
                 * until the next barrier, so they are non-durable and must be
                 * barrier-gated before verification. Everything else (local
                 * copies, remote streamed COMMIT, sparse streamed) is durable. */
                int durable = !sshx_active() || sparse ||
                    claim.file_task->src_st.st_size > g_ssh_putfile_max;
                if (verify_queue_file(claim.file_task->src,
                                      claim.file_task->dst,
                                      &claim.file_task->src_st, 0, durable) != 0) {
                    fprintf(stderr, "ecopy: unable to queue verification for %s\n",
                            claim.file_task->dst);
                    mark_worker_error();
                } else {
                    stats_inc_files_copied();
                }
            } else {
                mark_worker_error();
            }

            free_file_task(claim.file_task);

            pthread_mutex_lock(&g_queue_lock);
            if (g_small_workers_active > 0) {
                g_small_workers_active--;
            }
            /*
             * A single freed slot normally only needs to wake one waiter. But
             * once shutdown has been requested and this was the last
             * outstanding work, every parked worker must be released so it can
             * observe the termination condition and exit; a lone signal would
             * leave the others blocked forever and hang workers_stop().
             */
            if (g_queue_done && g_small_heap.len == 0 && g_large_heap.len == 0 &&
                g_small_workers_active == 0 && g_large_workers_active == 0) {
                pthread_cond_broadcast(&g_queue_cond);
            } else {
                pthread_cond_signal(&g_queue_cond);
            }
            pthread_mutex_unlock(&g_queue_lock);
            continue;
        }

        if (claim.kind == WORK_LARGE_FILE_START) {
            if (start_large_file_copy(claim.file_task) != 0) {
                mark_worker_error();
                pthread_mutex_lock(&g_queue_lock);
                if (g_large_workers_active > 0) {
                    g_large_workers_active--;
                }
                pthread_cond_broadcast(&g_queue_cond);
                pthread_cond_broadcast(&g_large_done_cond);
                pthread_mutex_unlock(&g_queue_lock);
            }
            free_file_task(claim.file_task);
            continue;
        }
    }

    thread_io_buffer_release();
    telemetry_flush_thread();
    return NULL;
}

/* -------------------- public API -------------------- */

void workers_set_collect_wait_timing(int on)
{
    g_collect_wait_timing = on ? 1 : 0;
}

int workers_start(void)
{
    int i;

    init_runtime_config();
    validate_runtime_config();
    /*
     * Every SSH file is routed through the small queue. Creating the remaining
     * generic workers only leaves hundreds of threads parked on g_queue_cond
     * (and makes a batch wake unnecessarily expensive).
     */
    if (sshx_active() && g_worker_count > g_small_worker_limit) {
        g_worker_count = g_small_worker_limit;
    }
    clear_worker_error();

    pthread_mutex_lock(&g_queue_lock);
    g_queue_done = 0;
    g_small_heap.len = 0;
    g_large_heap.len = 0;
    g_enqueue_seq = 0;
    g_small_workers_active = 0;
    g_large_workers_active = 0;
    pthread_mutex_unlock(&g_queue_lock);

    g_workers = calloc((size_t)g_worker_count, sizeof(*g_workers));
    if (!g_workers) {
        perror("calloc");
        return -1;
    }

    for (i = 0; i < g_worker_count; i++) {
        if (pthread_create(&g_workers[i], NULL, worker_main,
                           (void *)(intptr_t)i) != 0) {
            perror("pthread_create");
            return -1;
        }
    }

    return 0;
}

void workers_stop(void)
{
    int i;

    pthread_mutex_lock(&g_queue_lock);
    g_queue_done = 1;
    pthread_cond_broadcast(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);

    if (g_workers) {
        for (i = 0; i < g_worker_count; i++) {
            pthread_join(g_workers[i], NULL);
        }
    }

    pthread_mutex_lock(&g_queue_lock);
    while (g_large_workers_active > 0) {
        pthread_cond_wait(&g_large_done_cond, &g_queue_lock);
    }
    /*
     * Free any tasks still queued (only happens on an error stop; a clean run
     * drains both heaps to empty) and release the heap backing arrays.
     */
    for (size_t i = 0; i < g_small_heap.len; i++) {
        dir_handle_release(g_small_heap.items[i]->dir);
        free(g_small_heap.items[i]);
    }
    for (size_t i = 0; i < g_large_heap.len; i++) {
        dir_handle_release(g_large_heap.items[i]->dir);
        free(g_large_heap.items[i]);
    }
    free(g_small_heap.items);
    g_small_heap.items = NULL;
    g_small_heap.len = g_small_heap.cap = 0;
    free(g_large_heap.items);
    g_large_heap.items = NULL;
    g_large_heap.len = g_large_heap.cap = 0;
    /* All workers have exited; reclaim the recycled task nodes. */
    while (g_task_freelist) {
        file_task_t *next = g_task_freelist->next;
        free(g_task_freelist);
        g_task_freelist = next;
    }
    pthread_mutex_unlock(&g_queue_lock);

    stats_set_file_work_drained();

    free(g_workers);
    g_workers = NULL;
}

static int build_child_path(char *out, size_t outsz,
                            const char *parent, const char *name)
{
    size_t plen = strlen(parent);
    const char *sep = (plen > 0 && parent[plen - 1] == '/') ? "" : "/";
    int n = snprintf(out, outsz, "%s%s%s", parent, sep, name);
    if (n < 0 || (size_t)n >= outsz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

/*
 * Traversal discovers a directory's files in batches. Build all task nodes
 * first, then partition/append them under one queue lock. This keeps the SSH
 * wakeup optimization and local large-file routing in one implementation.
 */
int workers_enqueue_batch(dir_handle_t *dir,
                          const workers_batch_item_t *items,
                          size_t count)
{
    file_task_t *spares = NULL;
    file_task_t *batch_head = NULL;
    file_task_t *batch_tail = NULL;
    size_t built = 0;

    if (!dir || (!items && count != 0)) {
        errno = EINVAL;
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    init_runtime_config();

    /* Pull recycled nodes in one critical section. */
    pthread_mutex_lock(&g_queue_lock);
    for (size_t i = 0; i < count && g_task_freelist; i++) {
        file_task_t *t = g_task_freelist;
        g_task_freelist = t->next;
        t->next = spares;
        spares = t;
    }
    pthread_mutex_unlock(&g_queue_lock);

    for (size_t i = 0; i < count; i++) {
        file_task_t *t;
        if (!items[i].name || !items[i].src_st) {
            errno = EINVAL;
            goto fail;
        }
        if (spares) {
            t = spares;
            spares = t->next;
        } else {
            t = malloc(sizeof(*t));
            if (!t) {
                errno = ENOMEM;
                goto fail;
            }
        }

        copy_path_field(t->name, sizeof(t->name), items[i].name);
        if (build_child_path(t->src, sizeof(t->src), dir->src, items[i].name) != 0 ||
            build_child_path(t->dst, sizeof(t->dst), dir->dst, items[i].name) != 0) {
            t->next = spares;
            spares = t;
            goto fail;
        }
        dir_handle_retain(dir);
        t->dir = dir;
        t->src_st = *items[i].src_st;
        t->next = NULL;
        if (batch_tail) {
            batch_tail->next = t;
        } else {
            batch_head = t;
        }
        batch_tail = t;
        built++;
    }

    /* Return any excess recycled nodes before potentially waiting for space. */
    if (spares) {
        pthread_mutex_lock(&g_queue_lock);
        while (spares) {
            file_task_t *next = spares->next;
            spares->next = g_task_freelist;
            g_task_freelist = spares;
            spares = next;
        }
        pthread_mutex_unlock(&g_queue_lock);
    }

    while (batch_head) {
        size_t room;
        size_t take;

        pthread_mutex_lock(&g_queue_lock);
        while ((int)(g_small_heap.len + g_large_heap.len) >=
               g_max_queued_files) {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            pthread_cond_wait(&g_space_cond, &g_queue_lock);
            if (g_collect_wait_timing) {
                stats_record_queue_wait_ns(monotonic_ns() - wait_start_ns);
            }
        }
        room = (size_t)(g_max_queued_files -
                        (int)(g_small_heap.len + g_large_heap.len));
        take = built < room ? built : room;

        /*
         * Reserve heap capacity for this slice before pushing so heap_push is
         * infallible. Classify first to size each heap exactly; on OOM bail out
         * (the fail path frees the remaining batch and already-queued items stay
         * valid).
         */
        {
            size_t large_add = 0;
            size_t small_add = 0;
            file_task_t *scan = batch_head;
            for (size_t i = 0; i < take; i++) {
                int use_large = !sshx_active() &&
                                scan->src_st.st_size > runtime_large_threshold() &&
                                !workers_file_is_sparse(&scan->src_st);
                if (use_large) {
                    large_add++;
                } else {
                    small_add++;
                }
                scan = scan->next;
            }
            if (heap_reserve(&g_large_heap, g_large_heap.len + large_add) != 0 ||
                heap_reserve(&g_small_heap, g_small_heap.len + small_add) != 0) {
                pthread_mutex_unlock(&g_queue_lock);
                errno = ENOMEM;
                goto fail;
            }
        }

        for (size_t i = 0; i < take; i++) {
            file_task_t *t = batch_head;
            int use_large;
            batch_head = t->next;
            t->next = NULL;

            use_large = !sshx_active() &&
                        t->src_st.st_size > runtime_large_threshold() &&
                        !workers_file_is_sparse(&t->src_st);
            /*
             * Ordering key for the max-heaps: allocated-bytes weight when size
             * priority is on (biggest data first), else a decreasing sequence
             * so the heap yields FIFO (insertion) order.
             */
            t->sched_key = g_size_priority ? task_weight(&t->src_st)
                                           : (UINT64_MAX - g_enqueue_seq++);
            heap_push(use_large ? &g_large_heap : &g_small_heap, t);
        }
        {
            int free_slots = g_worker_count - total_worker_slots_used_locked();
            int large_wake = 0;
            int small_wake = 0;

            if (g_large_heap.len > 0 &&
                (int)g_large_workers_active < g_max_active_large_files &&
                free_slots >= g_large_worker_count) {
                large_wake = g_max_active_large_files -
                             (int)g_large_workers_active;
                if (large_wake > (int)g_large_heap.len) {
                    large_wake = (int)g_large_heap.len;
                }
                if (large_wake > free_slots / g_large_worker_count) {
                    large_wake = free_slots / g_large_worker_count;
                }
                free_slots -= large_wake * g_large_worker_count;
            }

            if (g_small_heap.len > 0 && free_slots > 0) {
                small_wake = g_small_worker_limit -
                             (int)g_small_workers_active;
                if (small_wake > (int)g_small_heap.len) {
                    small_wake = (int)g_small_heap.len;
                }
                if (small_wake > free_slots) {
                    small_wake = free_slots;
                }
            }

            for (int i = 0; i < large_wake + small_wake; i++) {
                pthread_cond_signal(&g_queue_cond);
            }
        }
        pthread_mutex_unlock(&g_queue_lock);

        built -= take;
    }
    return 0;

fail:
    while (batch_head) {
        file_task_t *next = batch_head->next;
        dir_handle_release(batch_head->dir);
        batch_head->next = spares;
        spares = batch_head;
        batch_head = next;
    }
    pthread_mutex_lock(&g_queue_lock);
    while (spares) {
        file_task_t *next = spares->next;
        spares->next = g_task_freelist;
        g_task_freelist = spares;
        spares = next;
    }
    pthread_mutex_unlock(&g_queue_lock);
    if (errno == ENOMEM) {
        perror("malloc");
    }
    return -1;
}

int workers_enqueue_small_file(dir_handle_t *dir,
                               const char *name,
                               const char *src,
                               const char *dst,
                               const struct stat *src_st)
{
    workers_batch_item_t item = { name, src_st };
    (void)src;
    (void)dst;
    return workers_enqueue_batch(dir, &item, 1);
}

int workers_enqueue_large_file(dir_handle_t *dir,
                               const char *name,
                               const char *src,
                               const char *dst,
                               const struct stat *src_st)
{
    workers_batch_item_t item = { name, src_st };
    (void)src;
    (void)dst;
    return workers_enqueue_batch(dir, &item, 1);
}

uint64_t workers_small_queue_depth(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_queue_lock);
    v = g_small_heap.len;
    pthread_mutex_unlock(&g_queue_lock);
    return v;
}

uint64_t workers_small_active_count(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_queue_lock);
    v = g_small_workers_active;
    pthread_mutex_unlock(&g_queue_lock);
    return v;
}

uint64_t workers_large_queue_depth(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_queue_lock);
    v = g_large_heap.len;
    pthread_mutex_unlock(&g_queue_lock);
    return v;
}

uint64_t workers_large_active_count(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_queue_lock);
    v = g_large_workers_active;
    pthread_mutex_unlock(&g_queue_lock);
    return v;
}

int workers_max_workers(void)
{
    init_runtime_config();
    return g_worker_count;
}

int workers_large_workers(void)
{
    init_runtime_config();
    return g_large_worker_count;
}

int workers_large_file_inflight(void)
{
    init_runtime_config();
    return g_large_file_inflight;
}

int workers_max_active_large_files(void)
{
    init_runtime_config();
    return g_max_active_large_files;
}

int workers_chunk_mb(void)
{
    init_runtime_config();
    return (int)(g_chunk_size / (1024 * 1024));
}

int workers_large_threshold_mb(void)
{
    init_runtime_config();
    return (int)(g_large_threshold / (1024 * 1024));
}


int workers_small_worker_limit(void)
{
    init_runtime_config();
    return g_small_worker_limit;
}

int workers_max_queued_files(void)
{
    init_runtime_config();
    return g_max_queued_files;
}

int workers_traversal_workers(void)
{
    const char *s = getenv("DIRECT_COPY_TRAVERSAL_WORKERS");
    if (!s || !*s) {
        return 8;
    }
    return env_int_or_default("DIRECT_COPY_TRAVERSAL_WORKERS", 8, 1, 128);
}

int workers_file_is_large(off_t size)
{
    return size > runtime_large_threshold();
}

void workers_print_runtime_summary(void)
{
    int reader_count;
    int writer_count;

    init_runtime_config();
    get_pipeline_thread_counts(&reader_count, &writer_count);

    printf("Options used:\n");
    printf("  direct_io enabled           : %s\n", direct_io_enabled() ? "yes" : "no");
    printf("  read direct_io enabled      : %s\n", read_direct_io_enabled() ? "yes" : "no");
    printf("  write direct_io enabled     : %s\n", write_direct_io_enabled() ? "yes" : "no");
    printf("  copy_file_range enabled     : %s\n", copy_file_range_enabled() ? "yes" : "no");
    printf("  max workers                 : %d\n", g_worker_count);
    printf("  small worker limit          : %d\n", g_small_worker_limit);
    printf("  large workers total         : %d\n", g_large_worker_count);
    printf("  active large file limit     : %d\n", g_max_active_large_files);
    printf("  large file readers/file     : %d\n", reader_count);
    printf("  large file writers/file     : %d\n", writer_count);
    printf("  large readers explicit      : %s\n", g_explicit_large_readers > 0 ? "yes" : "no");
    printf("  large file inflight chunks  : %d\n", g_large_file_inflight);
    printf("  chunk size MiB              : %d\n", (int)(g_chunk_size / (1024 * 1024)));
    printf("  large file threshold MiB    : %d\n", (int)(g_large_threshold / (1024 * 1024)));
    printf("  max queued files            : %d\n", g_max_queued_files);
}

void workers_print_startup_config(void)
{
    int reader_count;
    int writer_count;

    init_runtime_config();
    get_pipeline_thread_counts(&reader_count, &writer_count);

    printf("Resolved config:\n");
    printf("  max workers                 : %d\n", g_worker_count);
    printf("  small worker limit          : %d\n", g_small_worker_limit);
    printf("  large workers total         : %d\n", g_large_worker_count);
    printf("  active large file limit     : %d\n", g_max_active_large_files);
    printf("  large file readers/file     : %d\n", reader_count);
    printf("  large file writers/file     : %d\n", writer_count);
    printf("  large readers explicit      : %s\n", g_explicit_large_readers > 0 ? "yes" : "no");
    printf("  large file inflight chunks  : %d\n", g_large_file_inflight);
    printf("  chunk size MiB              : %d\n", (int)(g_chunk_size / (1024 * 1024)));
    printf("  large file threshold MiB    : %d\n", (int)(g_large_threshold / (1024 * 1024)));
    printf("  max queued files            : %d\n", g_max_queued_files);
    printf("  small in-place writes       : %s\n",
           copy_policy_small_inplace() ? "yes" : "no");
    printf("  read direct_io enabled      : %s\n", read_direct_io_enabled() ? "yes" : "no");
    printf("  write direct_io enabled     : %s\n", write_direct_io_enabled() ? "yes" : "no");
    printf("  copy_file_range enabled     : %s\n", copy_file_range_enabled() ? "yes" : "no");
    fflush(stdout);
}
