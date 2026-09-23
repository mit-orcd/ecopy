/*
 * workers.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "compat.h"
#include "workers.h"
#include "env_util.h"
#include "config.h"
#include "types.h"
#include "stats.h"
#include "fs_util.h"
#include "copy_policy.h"
#include "verify.h"
#include "telemetry.h"
#include "ssh_transport.h"
#include "protocol.h"
#include "shutdown.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#ifdef __linux__
#include <sys/vfs.h>
#endif
#include <sys/types.h>
#include <sys/syscall.h>
#include <limits.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>

#ifdef __linux__
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#endif

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
    /*
     * Hole-aware reading. For a sparse source the readers walk a SEEK_DATA/
     * SEEK_HOLE extent map built at start instead of [0, bulk_end): holes
     * are never read (no kernel zero-fill of unwritten extents) and never
     * written (the destination is ftruncate()d, so they stay holes).
     * extents holds ext_count [start,end) pairs, ALIGNMENT-rounded so
     * O_DIRECT preads stay legal; ext_idx is the reader cursor. A dense file
     * has extents == NULL and reads linearly as before.
     */
    off_t *extents;
    size_t ext_count;
    size_t ext_idx;
    uint64_t payload_bytes; /* data bytes actually moved (holes excluded) */
    int sparse;
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
    /*
     * Readers parked on free_cond / writers parked on ready_cond, maintained
     * under lock around the waits. The buffer handoff signals fire only when
     * someone is actually parked: at GiB/s chunk rates a per-buffer signal
     * with no waiter is a wasted futex wake, and with several producers the
     * first wake already lets the woken side drain the whole list.
     */
    int free_waiters;
    int ready_waiters;
    pthread_t *reader_threads;
    pthread_t *writer_threads;
    int reader_count;
    int writer_count;
    int readers_started;
    int writers_started;
    uint64_t service_start_ns;
    /* Intrusive link in g_active_large (under g_queue_lock): lets
     * workers_request_stop() fail parked reader/writer threads on Ctrl+C. */
    struct large_file_ctx *next_active;
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
/*
 * Small files whose ALIGNMENT-rounded bulk is at least this large get that
 * bulk written O_DIRECT (tail buffered) when copy_file_range is not usable,
 * i.e. on cross-device copies. Buffered small writes are paced by the
 * kernel's single per-device flusher and the dirty-page throttle
 * (vm.dirty_bytes): 2.2 GiB/s on a drive fio writes at 3.4 GiB/s with the
 * same 128 KiB O_DIRECT pattern. Below the threshold a synchronous device
 * write per file costs more than the page-cache copy. 0 disables.
 */
static off_t g_small_write_direct_min = -1;
static int g_max_queued_files = 0;
static int g_small_worker_limit = 0;
static off_t g_ssh_putfile_max = 0;   /* max size streamed as one PUTFILE frame */

#define MAX_LARGE_BUFFER_BUDGET_MB 8192

/*
 * Batched pipeline handoff: readers/writers claim up to this many chunk
 * buffers per lock hold and signal once per batch instead of once per buffer.
 * At 1 MiB chunks and multi-GiB/s per-file rates the per-buffer
 * lock + cond_signal + wake cycle dominated the large-file profile (~46%
 * pthread sync on a 9 GiB/s wire-bound fstor007 run). The effective batch is
 * additionally capped at a quarter of the inflight pool so one thread cannot
 * monopolize the buffers of a small pool.
 */
#define LARGE_PIPE_BATCH_MAX 4

static int large_pipe_batch(void)
{
    int b = g_large_file_inflight / 4;
    if (b < 1) {
        b = 1;
    }
    if (b > LARGE_PIPE_BATCH_MAX) {
        b = LARGE_PIPE_BATCH_MAX;
    }
    return b;
}

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
 * Number of worker threads parked in pthread_cond_wait(&g_queue_cond),
 * incremented/decremented around the wait under g_queue_lock. The enqueue
 * side reads it under the same lock to skip futex wakes nobody can consume
 * and to cap how many workers a batch wakes.
 */
static int              g_queue_waiters;
/*
 * Producer (traversal) threads parked in pthread_cond_wait(&g_space_cond)
 * because the queue is full. Consumers wake one only when at least
 * space_wake_min() slots are free, so a woken producer can push a whole
 * stat batch instead of a handful of files and re-parking. Signaling per
 * claimed task, as before, made every claim a futex wake plus a mutex
 * fight with the producer: 42-51% of all cycles in the futex hash-bucket
 * spinlock and ~500k context switches/s on the imagenet profile, where the
 * walkers outrun the copy workers and the queue sits full all run.
 */
static int              g_space_waiters;
#define SPACE_WAKE_MIN 1024
/*
 * The large-file dispatch queue is a max-heap keyed by file_task_t.sched_key
 * (see enqueue), so big files drain biggest-data-first — size priority is
 * what load-balances the multi-worker pipeline. Guarded by g_queue_lock; len
 * is the queue depth reported to progress and used for backpressure.
 *
 * Heap entries carry the ordering key inline: sift comparisons previously
 * dereferenced file_task_t pointers (scattered, cache-line-sized structs) at
 * every level, which made heap_pop_max the top CPU consumer under perf.
 */
typedef struct {
    uint64_t     key;
    file_task_t *task;
} task_heap_entry_t;

typedef struct {
    task_heap_entry_t *items;
    size_t             len;
    size_t             cap;
} task_heap_t;
static task_heap_t     g_large_heap;

/*
 * Small files are queued per source directory and dispatched round-robin
 * across directories. Size priority among sub-threshold files has no
 * load-balance value (that is what the large heap is for), but *which
 * directory* concurrent workers touch matters a great deal: creating a file
 * takes the parent's i_rwsem exclusively, so 32 workers all creating inside
 * the same 1300-file directory serialize on that one lock (49% of cycles in
 * osq_lock on the imagenet profile). The traversal hands us one batch per
 * directory; a claim takes a run of SMALL_CLAIM_BATCH tasks from the head
 * batch and, if that batch is not yet exhausted, rotates it to the tail. With
 * N directories queued (typically hundreds; traversal runs far ahead) each
 * directory has ~workers/N concurrent creators instead of all of them.
 * Same g_queue_lock guarding; len feeds backpressure and progress exactly
 * like the ring's did.
 */
typedef struct dir_batch {
    struct dir_batch *next;
    uint32_t          head;  /* next task to claim */
    uint32_t          count;
    file_task_t      *tasks[];
} dir_batch_t;

typedef struct {
    dir_batch_t *head;
    dir_batch_t *tail;
    size_t       len; /* tasks remaining across all batches */
} small_queue_t;
static small_queue_t   g_small_q;
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
 * Active large-file pipelines, linked via large_file_ctx_t.next_active under
 * g_queue_lock. workers_request_stop() walks it to fail every in-flight large
 * file so parked reader/writer threads wake and unwind on Ctrl+C.
 */
static large_file_ctx_t *g_active_large = NULL;
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
        int kib = env_int_or_default("DIRECT_COPY_SMALL_WRITE_DIRECT_MIN_KB",
                                     64, 0, 1024 * 1024);
        g_small_write_direct_min = (off_t)kib * 1024;
    }

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

/*
 * st_blocks cannot see fallocate()d-but-unwritten extents: they count as
 * allocated, so a 100 GiB file holding 14 GiB of data looks fully dense. The
 * dense O_DIRECT reader then preads the whole 100 GiB and the kernel memsets
 * zeros for the unwritten 86 GiB (80% of cycles in iov_iter_zero on the
 * /data1/erbmi1/001 profile), which we then write out for real.
 *
 * Two ways to see through that:
 *
 *  - FIEMAP reads the extent tree directly and flags unwritten extents
 *    explicitly (FIEMAP_EXTENT_UNWRITTEN). Deterministic. FIEMAP_FLAG_SYNC
 *    first writes back dirty pages so an unwritten extent that was just
 *    written to via the page cache is not misread as empty.
 *  - SEEK_DATA/SEEK_HOLE is portable (NFS 4.2, macOS) but XFS answers it for
 *    unwritten extents by asking the page cache: if something buffered-read
 *    the file recently (sha256sum, cp, a previous verify), the cached zero
 *    pages count as data and the holes vanish until eviction. Correct but
 *    slow, and nondeterministic between runs. Fallback only.
 */

/* Growable [start,end) list, ALIGNMENT-rounded and merged. */
typedef struct {
    off_t   *ext;
    size_t   n;
    size_t   cap;
    uint64_t bytes;
} extent_list_t;

/*
 * Append data run [data,hole) clipped to limit. Boundaries are rounded
 * outward to ALIGNMENT (reading a few KiB of hole as zeros is harmless; an
 * unaligned O_DIRECT pread is not). Returns -1 on OOM.
 */
static int extent_list_add(extent_list_t *l, off_t data, off_t hole, off_t limit)
{
    data = (data / ALIGNMENT) * ALIGNMENT;
    hole = ((hole + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    if (hole > limit) {
        hole = limit;
    }
    if (data < 0) {
        data = 0;
    }
    if (hole <= data) {
        return 0;
    }
    if (l->n > 0 && data <= l->ext[2 * (l->n - 1) + 1]) {
        /* Touches or overlaps the previous run: merge. */
        off_t prev_end = l->ext[2 * (l->n - 1) + 1];
        if (hole > prev_end) {
            l->bytes += (uint64_t)(hole - prev_end);
            l->ext[2 * (l->n - 1) + 1] = hole;
        }
        return 0;
    }
    if (l->n == l->cap) {
        size_t ncap = l->cap ? l->cap * 2 : 64;
        off_t *tmp = realloc(l->ext, ncap * 2 * sizeof(*tmp));
        if (!tmp) {
            return -1;
        }
        l->ext = tmp;
        l->cap = ncap;
    }
    l->ext[2 * l->n] = data;
    l->ext[2 * l->n + 1] = hole;
    l->n++;
    l->bytes += (uint64_t)(hole - data);
    return 0;
}

/*
 * FIEMAP walk over [0, limit). Unwritten extents are treated as holes;
 * anything else (including DELALLOC/UNKNOWN, which SYNC should have
 * resolved anyway) is data. When stop_at_first_hole is set the walk returns
 * as soon as it knows a hole exists (probe use).
 * Returns 1 = mapped (*has_hole set), 0 = FIEMAP unsupported here,
 * -1 = OOM.
 */
static int fiemap_walk(int fd, off_t limit, extent_list_t *l, int stop_at_first_hole,
                       int *has_hole)
{
#ifdef FS_IOC_FIEMAP
    enum { FM_BATCH = 512 };
    struct fiemap *fm;
    uint64_t pos = 0;
    uint64_t expect = 0; /* logical offset the next extent should start at */
    int rc = 1;

    *has_hole = 0;
    fm = malloc(sizeof(*fm) + FM_BATCH * sizeof(struct fiemap_extent));
    if (!fm) {
        return -1;
    }
    for (;;) {
        unsigned int i;
        const struct fiemap_extent *last = NULL;

        memset(fm, 0, sizeof(*fm));
        fm->fm_start = pos;
        fm->fm_length = (uint64_t)limit - pos;
        fm->fm_flags = FIEMAP_FLAG_SYNC;
        fm->fm_extent_count = FM_BATCH;
        if (ioctl(fd, FS_IOC_FIEMAP, fm) != 0) {
            rc = 0; /* EOPNOTSUPP/ENOTTY/EBADR: not available on this fs */
            break;
        }
        if (fm->fm_mapped_extents == 0) {
            break;
        }
        for (i = 0; i < fm->fm_mapped_extents; i++) {
            const struct fiemap_extent *e = &fm->fm_extents[i];
            uint64_t s = e->fe_logical;
            uint64_t en = s + e->fe_length;
            int unwritten = (e->fe_flags & FIEMAP_EXTENT_UNWRITTEN) &&
                            !(e->fe_flags & FIEMAP_EXTENT_DELALLOC);
            last = e;
            if (s > expect || unwritten) {
                *has_hole = 1;
                if (stop_at_first_hole) {
                    goto done;
                }
            }
            if (!unwritten && l && s < (uint64_t)limit) {
                if (extent_list_add(l, (off_t)s, (off_t)(en < (uint64_t)limit ? en : (uint64_t)limit),
                                    limit) != 0) {
                    rc = -1;
                    goto done;
                }
            }
            if (en > expect) {
                expect = en;
            }
        }
        if ((last->fe_flags & FIEMAP_EXTENT_LAST) || expect >= (uint64_t)limit ||
            expect <= pos /* no forward progress: never spin */) {
            break;
        }
        pos = expect;
    }
    if (rc == 1 && expect < (uint64_t)limit) {
        *has_hole = 1; /* trailing hole before limit */
    }
done:
    free(fm);
    return rc;
#else
    (void)fd; (void)limit; (void)l; (void)stop_at_first_hole;
    *has_hole = 0;
    return 0;
#endif
}

/* SEEK_DATA/SEEK_HOLE walk over [0, limit). Same return convention. */
static int seek_walk(int fd, off_t limit, extent_list_t *l, int *has_hole)
{
    off_t pos = 0;

    *has_hole = 0;
    while (pos < limit) {
        off_t data = lseek(fd, pos, SEEK_DATA);
        off_t hole;
        if (data < 0) {
            if (errno == ENXIO) {
                *has_hole = 1; /* only holes remain */
                break;
            }
            return 0; /* unsupported */
        }
        if (data >= limit) {
            *has_hole = 1;
            break;
        }
        if (data > pos) {
            *has_hole = 1;
        }
        hole = lseek(fd, data, SEEK_HOLE);
        if (hole < 0) {
            return 0;
        }
        if (hole < limit) {
            *has_hole = 1;
        }
        if (l && extent_list_add(l, data, hole, limit) != 0) {
            return -1;
        }
        pos = hole;
    }
    return 1;
}

/*
 * Build the data-extent map of fd over [0, limit): FIEMAP first, SEEK_HOLE
 * fallback. Returns 0 with the map and count set, or 0 with a NULL map if
 * neither interface works here (caller then reads the whole range); -1
 * only on OOM.
 */
static int build_extent_map(int fd, off_t limit, off_t **out, size_t *count,
                            uint64_t *data_bytes)
{
    extent_list_t l = { NULL, 0, 0, 0 };
    int has_hole = 0;
    int rc;

    *out = NULL;
    *count = 0;
    *data_bytes = 0;
    rc = fiemap_walk(fd, limit, &l, 0, &has_hole);
    if (rc == 0) {
        free(l.ext);
        l.ext = NULL;
        l.n = l.cap = 0;
        l.bytes = 0;
        rc = seek_walk(fd, limit, &l, &has_hole);
    }
    if (rc < 0) {
        free(l.ext);
        return -1;
    }
    if (rc == 0) {
        free(l.ext);
        return 0; /* dense read */
    }
    if (l.n == 0 && l.ext == NULL) {
        /* All holes: hand back a valid empty map, not "unknown". */
        l.ext = malloc(sizeof(off_t));
        if (!l.ext) {
            return -1;
        }
    }
    *out = l.ext;
    *count = l.n;
    *data_bytes = l.bytes;
    return 0;
}

/*
 * Enqueue-time probe for files big enough to matter: one open + one extent
 * walk (FIEMAP stops at the first unwritten extent or gap) + close per large
 * file. Returns 1 if a hole exists before EOF, 0 if dense or unknowable.
 */
static int source_has_hole(int dir_fd, const char *name, off_t size)
{
    /* open + ioctl/lseek never touch atime, so no O_NOATIME games needed. */
    int fd = openat(dir_fd, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    int has_hole = 0;
    int rc;

    if (fd < 0) {
        return 0;
    }
    rc = fiemap_walk(fd, size, NULL, 1, &has_hole);
    if (rc == 0) {
        rc = seek_walk(fd, size, NULL, &has_hole);
    }
    ecopy_close_nocancel(fd);
    return rc == 1 && has_hole;
}

/*
 * Local files above the threshold take the parallel reader/writer pipeline.
 * A sparse file qualifies too when its *data* (not its logical size) is
 * above the threshold: a 100 GiB image holding 14 GiB is still a big copy
 * and the pipeline skips its holes via the extent map. Sparse files with
 * little data stay on the serial hole-skipping path, where per-file thread
 * spin-up would dominate.
 */
static uint64_t task_weight(const struct stat *st);

static int task_uses_large_pipeline(const file_task_t *t)
{
    off_t threshold = runtime_large_threshold();

    if (sshx_active() || t->src_st.st_size <= threshold) {
        return 0;
    }
    if (!t->sparse) {
        return 1;
    }
    return (off_t)task_weight(&t->src_st) > threshold;
}

/* Decide once, at enqueue time, which data path a file takes. */
static int classify_sparse(const dir_handle_t *dir, const char *name, const struct stat *st)
{
    if (workers_file_is_sparse(st)) {
        return 1;
    }
    if (S_ISREG(st->st_mode) && st->st_size > runtime_large_threshold()) {
        return source_has_hole(dir->src_fd, name, st->st_size);
    }
    return 0;
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
    task_heap_entry_t *ni = realloc(h->items, ncap * sizeof(*ni));
    if (!ni) {
        return -1;
    }
    h->items = ni;
    h->cap = ncap;
    return 0;
}

/*
 * The heaps are 4-ary: depth is log4(n) instead of log2(n) and the four
 * children of a node occupy one or two adjacent cache lines, so sift-down
 * costs about half the dependent cache misses of the binary version at the
 * queue depths a big tree actually reaches (100k+ entries). Pop ordering is
 * unchanged: always the highest sched_key.
 */
#define HEAP_ARITY 4

/* Push a task whose sched_key is set. Capacity must be reserved by the caller. */
static void heap_push(task_heap_t *h, file_task_t *t)
{
    size_t i = h->len++;
    uint64_t key = t->sched_key;
    while (i > 0) {
        size_t parent = (i - 1) / HEAP_ARITY;
        if (h->items[parent].key >= key) {
            break;
        }
        h->items[i] = h->items[parent];
        i = parent;
    }
    h->items[i].key = key;
    h->items[i].task = t;
}

/* Remove and return the highest-key task. Caller ensures h->len > 0. */
static file_task_t *heap_pop_max(task_heap_t *h)
{
    file_task_t *top = h->items[0].task;
    task_heap_entry_t node = h->items[--h->len];
    if (h->len > 0) {
        size_t i = 0;
        uint64_t key = node.key;
        for (;;) {
            size_t child = HEAP_ARITY * i + 1;
            size_t best = i;
            uint64_t best_key = key;
            if (child < h->len) {
                size_t last = child + (HEAP_ARITY - 1);
                if (last >= h->len) {
                    last = h->len - 1;
                }
                for (; child <= last; child++) {
                    if (h->items[child].key > best_key) {
                        best = child;
                        best_key = h->items[child].key;
                    }
                }
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

/* Append a (non-empty) directory batch. Caller holds g_queue_lock. */
static void smallq_push_batch(small_queue_t *q, dir_batch_t *b)
{
    b->next = NULL;
    if (q->tail) {
        q->tail->next = b;
    } else {
        q->head = b;
    }
    q->tail = b;
    q->len += b->count - b->head;
}

/* Detach the head batch. Caller holds g_queue_lock and ensures q->len > 0. */
static dir_batch_t *smallq_pop_batch(small_queue_t *q)
{
    dir_batch_t *b = q->head;
    q->head = b->next;
    if (!q->head) {
        q->tail = NULL;
    }
    q->len -= b->count - b->head;
    return b;
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

static void record_progress_bytes(uint64_t bytes, int use_current_file_stats);

/*
 * The large writers all bump the same global byte counter once per chunk. Once
 * the stats mutex was gone, that shared atomic became a cacheline that ping-pongs
 * between every writer thread. Each writer instead accumulates locally and only
 * folds the total into the global counter every BYTES_FLUSH_THRESHOLD (and at
 * thread exit), trading slightly coarser live progress for far less contention.
 */
#define BYTES_FLUSH_THRESHOLD (16ULL * 1024 * 1024)

/* Small-file chunks flush far below the large-path threshold so the 1 Hz
 * rolling-rate sampler stays responsive on metadata-heavy runs. */
#define SMALL_BYTES_FLUSH_THRESHOLD (256ULL * 1024)

static __thread uint64_t g_tls_pending_bytes = 0;
static __thread uint64_t g_tls_pending_small_bytes = 0;

static void progress_add_bytes_batched(uint64_t bytes)
{
    g_tls_pending_bytes += bytes;
    if (g_tls_pending_bytes >= BYTES_FLUSH_THRESHOLD) {
        stats_add_bytes(g_tls_pending_bytes);
        g_tls_pending_bytes = 0;
    }
}

static void progress_add_small_bytes_batched(uint64_t bytes)
{
    g_tls_pending_small_bytes += bytes;
    if (g_tls_pending_small_bytes >= SMALL_BYTES_FLUSH_THRESHOLD) {
        stats_add_bytes(g_tls_pending_small_bytes);
        g_tls_pending_small_bytes = 0;
    }
}

static void progress_flush_bytes(void)
{
    if (g_tls_pending_bytes > 0) {
        stats_add_bytes(g_tls_pending_bytes);
        g_tls_pending_bytes = 0;
    }
    if (g_tls_pending_small_bytes > 0) {
        stats_add_bytes(g_tls_pending_small_bytes);
        g_tls_pending_small_bytes = 0;
    }
}

static void record_progress_bytes(uint64_t bytes, int use_current_file_stats)
{
    if (use_current_file_stats) {
        /* The thread-exclusive slot bump stays per chunk; the shared global
         * counter is batched to keep it off the contended cacheline. */
        stats_advance_current_file_slot(bytes);
        progress_add_small_bytes_batched(bytes);
    } else {
        stats_add_bytes(bytes);
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

/*
 * Alignment-tail copy (the last size % ALIGNMENT bytes of a direct-I/O file,
 * or the whole file when it is smaller than one aligned chunk). Offset-based
 * pread/pwrite_nocancel keep this path free of the lseek pair and of glibc's
 * per-syscall cancellation bookkeeping; timing is sampled like the bulk loop
 * instead of paying four clock_gettime calls per chunk. Small-file trees run
 * almost entirely through here, so the per-call overhead matters.
 */
static int copy_tail_buffered_fds(int fd_in,
                                 int fd_out,
                                 off_t start,
                                 off_t end,
                                 int use_current_file_stats)
{
    if (start >= end) {
        return 0;
    }

    {
        char buf[ALIGNMENT];
        off_t pos = start;

        while (pos < end) {
            off_t remain = end - pos;
            off_t this_len_off = (remain < (off_t)sizeof(buf)) ? remain : (off_t)sizeof(buf);
            size_t len = (size_t)this_len_off;
            off_t chunk_start = pos;

            int r_timed = io_should_sample();
            uint64_t read_start_ns = r_timed ? monotonic_ns() : 0;
            ssize_t r = pread_nocancel(fd_in, buf, len, pos);
            stats_record_read_op();
            if (r_timed) {
                stats_record_read_time((monotonic_ns() - read_start_ns) * IO_SAMPLE_PERIOD);
            }
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
                    int w_timed = io_should_sample();
                    uint64_t write_start_ns = w_timed ? monotonic_ns() : 0;
                    ssize_t w = pwrite_nocancel(fd_out, buf + done, (size_t)r - done,
                                                pos + (off_t)done);
                    stats_record_write_op();
                    if (w_timed) {
                        stats_record_write_time((monotonic_ns() - write_start_ns) * IO_SAMPLE_PERIOD);
                    }
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
        ecopy_close_nocancel(fd_in);
        return -1;
    }

    advise_source_streaming(fd_in);
    advise_dest_streaming(fd_out);

    int rc = copy_tail_buffered_fds(fd_in, fd_out, start, end, use_current_file_stats);

    ecopy_close_nocancel(fd_in);
    ecopy_close_nocancel(fd_out);
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
    stats_set_current_file(task->src, task->src_len, (uint64_t)size, 0);

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
                                               task->src_st.st_size,
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
        ecopy_close_nocancel(fd_out);
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
    if (ecopy_ftruncate_nocancel(fd_out, size) != 0) {
        perror("ftruncate");
        goto out;
    }

    if (finalize_copied_file_fd(fd_out, task->dst, &task->src_st) != 0) {
        goto out;
    }
    if (ecopy_close_nocancel(fd_out) != 0) {
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
        ecopy_close_nocancel(fd_in);
    }
    if (fd_out >= 0) {
        ecopy_close_nocancel(fd_out);
    }
    if (target_created) {
        unlink_temp_at(task->dir->dst_fd, tmp_name);
    }
    stats_clear_current_file(task->src);
    return rc;
}

/*
 * Switch an open destination between buffered and O_DIRECT. Linux allows
 * O_DIRECT in F_SETFL; elsewhere report failure and the caller stays
 * buffered. Ranges written in the two modes never overlap here (aligned
 * bulk, then the partial last block), so page-cache coherency is not an
 * issue.
 */
static int fd_set_direct_write(int fd, int on)
{
#ifdef __linux__
    int fl = fcntl(fd, F_GETFL);
    if (fl < 0) {
        return -1;
    }
    fl = on ? (fl | O_DIRECT) : (fl & ~O_DIRECT);
    return fcntl(fd, F_SETFL, fl);
#else
    (void)fd;
    (void)on;
    return -1;
#endif
}

/*
 * O_DIRECT small-file bulks pay off on local block filesystems only. On a
 * network destination each direct write is a synchronous RPC with no
 * client-side coalescing: measured 20% slower than buffered on an NFS
 * target, while 1.4x faster on a local NVMe. Probe the destination
 * directory's filesystem once per dir_handle and allowlist local types.
 */
static int dst_fs_direct_ok(dir_handle_t *dir)
{
    int v = atomic_load_explicit(&dir->dst_direct_ok, memory_order_relaxed);
    if (v != 0) {
        return v > 0;
    }
#ifdef __linux__
    {
        struct statfs sf;
        v = -1;
        if (fstatfs(dir->dst_fd, &sf) == 0) {
            switch ((unsigned long)sf.f_type) {
            case 0x58465342UL: /* XFS */
            case 0xEF53UL:     /* ext2/3/4 */
            case 0x9123683EUL: /* btrfs */
            case 0xF2F52010UL: /* f2fs */
                v = 1;
                break;
            default:
                break;
            }
        }
    }
#else
    v = -1;
#endif
    atomic_store_explicit(&dir->dst_direct_ok, v, memory_order_relaxed);
    return v > 0;
}

/* Small buffered destination, copy_file_range unusable: write the aligned
 * bulk O_DIRECT if the file is big enough to make the device write pay. */
static int small_bulk_wants_direct(dir_handle_t *dir, off_t bulk_end)
{
    return g_small_write_direct_min > 0 && bulk_end >= g_small_write_direct_min &&
           write_direct_io_enabled() && dst_fs_direct_ok(dir);
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
    off_t copy_end;
    off_t pos = 0;
    int rc = -1;
    int target_created = 0;
    int bulk_direct = 0;    /* destination bulk written O_DIRECT on a buffered fd */
    int inplace = copy_policy_small_inplace();
    const char *write_name;
    char tmp_name[PATH_MAX] = "";
    fadvise_batch_t fadv;

    fadvise_batch_init(&fadv);
    init_runtime_config();

    if (task->sparse) {
        return copy_file_sparse(task, payload_bytes);
    }

    copy_range_available = copy_file_range_enabled();
    size = task->src_st.st_size;
    bulk_end = (size / ALIGNMENT) * ALIGNMENT;
    stats_set_current_file(task->src, task->src_len, (uint64_t)size, 0);

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
                                                    task->src_st.st_size,
                                                    &out_direct);
        write_name = task->name;
    } else {
        fd_out = create_temp_write_at_maybe_direct(task->dir->dst_fd,
                                                   task->dst,
                                                   task->src_st.st_mode & 07777,
                                                   task->src_st.st_size,
                                                   tmp_name,
                                                   sizeof(tmp_name),
                                                   &out_direct);
        write_name = tmp_name;
    }
    if (fd_out < 0) {
        goto out;
    }
    target_created = 1;

    if (ecopy_ftruncate_nocancel(fd_out, size) != 0) {
        perror("ftruncate");
        goto out;
    }

    /*
     * copy_file_range() either reflinks (same-fs XFS/btrfs) or splices in
     * the kernel; the data never passes through this thread's buffer. A
     * WILLNEED hint before it therefore just drags the whole source through
     * the page cache for nothing (9.8% of cycles and the entire 144 GB
     * dataset re-read on the imagenet profile). Hint only when we know we
     * will read() the data ourselves; the fallback path below hints if
     * copy_file_range turns out not to work.
     */
    if (!in_direct && !(copy_range_available && !out_direct)) {
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

    /*
     * With copy_file_range on a buffered pair, hand it the whole file, not
     * just the ALIGNMENT-rounded bulk. A remap that ends at EOF on both
     * sides may be unaligned, so on XFS/btrfs the partial last block is
     * cloned with the rest and nothing is read; elsewhere the kernel splices
     * the tail in the same call. Copying the tail ourselves cost one cold
     * 4 KiB pread per file (1.33M preads, 257 s blocked, ~8 s of a 14 s wall
     * per worker on the imagenet run) while the reflink itself took 38 us.
     * The O_DIRECT paths keep the buffered tail below.
     */
    copy_end = (!in_direct && !out_direct && copy_range_available) ? size : bulk_end;

    if (!in_direct && !out_direct && !copy_range_available &&
        small_bulk_wants_direct(task->dir, bulk_end) && fd_set_direct_write(fd_out, 1) == 0) {
        bulk_direct = 1;
    }

    while (pos < copy_end) {
        off_t remain = copy_end - pos;
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
            advise_source_streaming(fd_in); /* now we read it ourselves */
            /* Cross-device: nothing has been written yet, so the aligned
             * bulk can still go O_DIRECT straight to the device. */
            if (pos == 0 && small_bulk_wants_direct(task->dir, bulk_end) &&
                fd_set_direct_write(fd_out, 1) == 0) {
                bulk_direct = 1;
            }
            /* Back to the aligned bulk; the tail copy below takes the rest. */
            copy_end = bulk_end;
            remain = copy_end - pos;
            if (remain <= 0) {
                break;
            }
            this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
            len = (size_t)this_len_off;
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
        if (bulk_direct) {
            if (fd_set_direct_write(fd_out, 0) != 0) {
                perror("fcntl(O_DIRECT off)");
                goto out;
            }
            stats_record_small_bulk_direct();
        }
        /* pos == size when copy_file_range covered the whole file: no-op. */
        if (copy_tail_buffered_fds(fd_in, fd_out, pos, size, 1) != 0) {
            goto out;
        }
        if (finalize_copied_file_fd(fd_out, task->dst, &task->src_st) != 0) {
            goto out;
        }
        if (ecopy_close_nocancel(fd_out) != 0) {
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
        if (payload_bytes) *payload_bytes = (uint64_t)size;
        goto out;
    }

    ecopy_close_nocancel(fd_out);
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
        ecopy_close_nocancel(fd_in);
    }
    if (fd_out >= 0) {
        ecopy_close_nocancel(fd_out);
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

/*
 * Position next_read_offset on data. Caller holds ctx->lock. After this,
 * next_read_offset >= bulk_end means the readers are done; otherwise it
 * points into extents[ext_idx] (or anywhere, for a dense file).
 */
static void reader_skip_holes_locked(large_file_ctx_t *ctx)
{
    if (!ctx->extents) {
        return;
    }
    while (ctx->ext_idx < ctx->ext_count &&
           ctx->next_read_offset >= ctx->extents[2 * ctx->ext_idx + 1]) {
        ctx->ext_idx++;
    }
    if (ctx->ext_idx >= ctx->ext_count) {
        ctx->next_read_offset = ctx->bulk_end;
        return;
    }
    if (ctx->next_read_offset < ctx->extents[2 * ctx->ext_idx]) {
        ctx->next_read_offset = ctx->extents[2 * ctx->ext_idx];
    }
}

/* End of the run the next chunk may extend to. Caller holds ctx->lock. */
static off_t reader_run_end_locked(const large_file_ctx_t *ctx)
{
    if (ctx->extents && ctx->ext_idx < ctx->ext_count) {
        return ctx->extents[2 * ctx->ext_idx + 1];
    }
    return ctx->bulk_end;
}

static void *large_reader_main(void *arg)
{
    large_file_ctx_t *ctx = (large_file_ctx_t *)arg;
    const int batch_max = large_pipe_batch();

    for (;;) {
        large_buffer_t *batch[LARGE_PIPE_BATCH_MAX];
        int n = 0;
        int read_failed = 0;
        int i;

        pthread_mutex_lock(&ctx->lock);
        /* Ctrl+C unwinds a multi-GB file through the normal failure teardown
         * (buffers reclaimed, temp file unlinked) instead of copying on. */
        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed) &&
            !ctx->failed) {
            mark_large_file_failed_locked(ctx);
        }
        while (!ctx->failed && !ctx->free_head && ctx->next_read_offset < ctx->bulk_end) {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            ctx->free_waiters++;
            pthread_cond_wait(&ctx->free_cond, &ctx->lock);
            ctx->free_waiters--;
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

        /* Claim up to batch_max free buffers and assign their read ranges in
         * a single lock hold. */
        while (n < batch_max && ctx->free_head && ctx->next_read_offset < ctx->bulk_end) {
            off_t offset;
            off_t remain;
            off_t this_len_off;
            large_buffer_t *b = dequeue_buffer(&ctx->free_head, &ctx->free_tail);
            if (ctx->free_count > 0) {
                ctx->free_count--;
            }
            offset = ctx->next_read_offset;
            remain = reader_run_end_locked(ctx) - offset;
            this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
            b->offset = offset;
            b->len = (size_t)this_len_off;
            ctx->next_read_offset += this_len_off;
            reader_skip_holes_locked(ctx);
            batch[n++] = b;
        }
        pthread_mutex_unlock(&ctx->lock);

        for (i = 0; i < n; i++) {
            int timed = io_should_sample();
            uint64_t read_start_ns = timed ? monotonic_ns() : 0;
            ssize_t r = pread_nocancel(ctx->fd_in, batch[i]->data, batch[i]->len,
                                       batch[i]->offset);
            stats_record_read_op();
            if (timed) {
                stats_record_read_time((monotonic_ns() - read_start_ns) * IO_SAMPLE_PERIOD);
            }
            if (r < 0) {
                perror("pread");
                read_failed = 1;
                break;
            }
            if ((size_t)r != batch[i]->len) {
                fprintf(stderr, "short pread at off %lld: expected %zu got %zd\n",
                        (long long)batch[i]->offset, batch[i]->len, r);
                read_failed = 1;
                break;
            }
            /* With O_DIRECT the source never enters the page cache, so dropping it
             * is a wasted syscall per chunk; only advise for buffered reads. */
            if (!ctx->in_direct) {
                advise_source_consumed(ctx->fd_in, batch[i]->offset, (off_t)batch[i]->len);
            }
        }

        pthread_mutex_lock(&ctx->lock);
        if (read_failed) {
            mark_large_file_failed_locked(ctx);
        }
        if (ctx->failed) {
            /* The file is being torn down; return every buffer we hold to the
             * free list so finish_large_file_ctx() can reclaim it (only the
             * free/ready lists are freed). */
            for (i = 0; i < n; i++) {
                enqueue_buffer(&ctx->free_head, &ctx->free_tail, batch[i]);
                ctx->free_count++;
            }
            pthread_cond_broadcast(&ctx->free_cond);
            ctx->active_readers--;
            if (ctx->active_readers == 0) {
                ctx->read_done = 1;
            }
            pthread_mutex_unlock(&ctx->lock);
            break;
        }
        for (i = 0; i < n; i++) {
            enqueue_buffer(&ctx->ready_head, &ctx->ready_tail, batch[i]);
            ctx->ready_count++;
        }
        if (io_should_sample()) {
            stats_record_ready_queue_depth(ctx->ready_count);
        }
        if (ctx->ready_waiters > 0) {
            pthread_cond_signal(&ctx->ready_cond);
        }
        pthread_mutex_unlock(&ctx->lock);
    }

    stats_flush_io_op_counts();
    return NULL;
}

static void *large_writer_main(void *arg)
{
    large_file_ctx_t *ctx = (large_file_ctx_t *)arg;
    const int batch_max = large_pipe_batch();

    for (;;) {
        large_buffer_t *batch[LARGE_PIPE_BATCH_MAX];
        int n = 0;
        int failed = 0;
        int i;

        pthread_mutex_lock(&ctx->lock);
        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed) &&
            !ctx->failed) {
            mark_large_file_failed_locked(ctx);
        }
        while (!ctx->failed && !ctx->ready_head && !ctx->read_done) {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            ctx->ready_waiters++;
            pthread_cond_wait(&ctx->ready_cond, &ctx->lock);
            ctx->ready_waiters--;
            if (g_collect_wait_timing) {
                stats_record_writer_data_wait_ns(monotonic_ns() - wait_start_ns);
            }
        }

        /* On shutdown do not keep writing already-read buffers. */
        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed) ||
            (ctx->failed && !ctx->ready_head) || (!ctx->ready_head && ctx->read_done)) {
            ctx->active_writers--;
            pthread_mutex_unlock(&ctx->lock);
            break;
        }

        /* Drain up to batch_max ready buffers in a single lock hold. */
        while (n < batch_max && ctx->ready_head) {
            batch[n++] = dequeue_buffer(&ctx->ready_head, &ctx->ready_tail);
            if (ctx->ready_count > 0) {
                ctx->ready_count--;
            }
        }
        if (io_should_sample()) {
            stats_record_ready_queue_depth(ctx->ready_count);
        }
        pthread_mutex_unlock(&ctx->lock);

        for (i = 0; i < n; i++) {
            size_t done = 0;

            while (done < batch[i]->len) {
                int timed = io_should_sample();
                uint64_t write_start_ns = timed ? monotonic_ns() : 0;
                ssize_t w = pwrite_nocancel(ctx->fd_out,
                                            (char *)batch[i]->data + done,
                                            batch[i]->len - done,
                                            batch[i]->offset + (off_t)done);
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
                            (long long)(batch[i]->offset + (off_t)done));
                    failed = 1;
                    break;
                }
                done += (size_t)w;
            }
            if (failed) {
                break;
            }
            progress_add_bytes_batched((uint64_t)batch[i]->len);
        }

        pthread_mutex_lock(&ctx->lock);
        if (failed) {
            mark_large_file_failed_locked(ctx);
        }
        /* Return the whole batch (written or not) to the free list so the
         * readers get one coalesced wake and finish_large_file_ctx() reclaims
         * every buffer. */
        for (i = 0; i < n; i++) {
            enqueue_buffer(&ctx->free_head, &ctx->free_tail, batch[i]);
            ctx->free_count++;
        }
        if (ctx->free_waiters > 0) {
            pthread_cond_signal(&ctx->free_cond);
        }
        pthread_mutex_unlock(&ctx->lock);

        if (failed) {
            break;
        }
    }

    progress_flush_bytes();
    stats_flush_io_op_counts();
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
        } else if (ecopy_close_nocancel(ctx->fd_out) != 0) {
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
            telemetry_note_file(ctx->sparse ? TRANSFER_SPARSE : TRANSFER_LARGE,
                                (uint64_t)ctx->src_st.st_size,
                                ctx->payload_bytes,
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
        ecopy_close_nocancel(ctx->fd_in);
    }
    if (ctx->fd_out >= 0) {
        ecopy_close_nocancel(ctx->fd_out);
    }
    if (rc != 0) {
        unlink_temp_at(ctx->dir->dst_fd, ctx->tmp_name);
    }
    dir_handle_release(ctx->dir);
    free_large_buffers(ctx->free_head);
    free_large_buffers(ctx->ready_head);
    free(ctx->extents);
    free(ctx->reader_threads);
    free(ctx->writer_threads);
    pthread_mutex_destroy(&ctx->lock);
    pthread_cond_destroy(&ctx->free_cond);
    pthread_cond_destroy(&ctx->ready_cond);

    pthread_mutex_lock(&g_queue_lock);
    if (g_large_workers_active > 0) {
        g_large_workers_active--;
    }
    /* Unregister from the active large-file list (lock ordering is
     * g_queue_lock -> ctx->lock everywhere, matching workers_request_stop()). */
    {
        large_file_ctx_t **link = &g_active_large;
        while (*link) {
            if (*link == ctx) {
                *link = ctx->next_active;
                break;
            }
            link = &(*link)->next_active;
        }
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
    stats_flush_io_op_counts();
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
    ctx->sparse = task->sparse;
    ctx->payload_bytes = (uint64_t)task->src_st.st_size;
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

    if (ctx->sparse) {
        uint64_t data_bytes;
        if (build_extent_map(ctx->fd_in, ctx->bulk_end, &ctx->extents,
                             &ctx->ext_count, &data_bytes) != 0) {
            perror("malloc");
            goto fail;
        }
        if (ctx->extents) {
            /* Data extents plus the buffered tail beyond bulk_end. */
            ctx->payload_bytes = data_bytes +
                (uint64_t)(ctx->src_st.st_size - ctx->bulk_end);
            reader_skip_holes_locked(ctx);
        }
    }

    ctx->fd_out = create_temp_write_at_maybe_direct(ctx->dir->dst_fd,
                                                    ctx->dst,
                                                    ctx->src_st.st_mode & 07777,
                                                    ctx->src_st.st_size,
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
         * A sparse source must never be fallocate()d to its logical size (a
         * 5 PB sparse image would try to reserve 5 PB, and the holes would
         * stop being holes). Size it with ftruncate() so skipped ranges stay
         * holes, then preallocate just the data extents so the O_DIRECT
         * writes into them still skip the per-write allocator. Skip the
         * per-extent calls for badly fragmented files where they would cost
         * more than they save; the writes then allocate on the fly.
         */
        if (ctx->sparse) {
            if (ecopy_ftruncate_nocancel(ctx->fd_out, ctx->src_st.st_size) != 0) {
                perror("ftruncate");
                goto fail;
            }
            if (ctx->extents && ctx->ext_count <= 4096) {
                for (size_t e = 0; e < ctx->ext_count; e++) {
                    off_t st = ctx->extents[2 * e];
                    off_t ln = ctx->extents[2 * e + 1] - st;
                    if (fallocate(ctx->fd_out, 0, st, ln) != 0) {
                        break; /* best effort */
                    }
                }
            }
        } else if (fallocate(ctx->fd_out, 0, 0, ctx->src_st.st_size) != 0) {
            if (errno != EOPNOTSUPP && errno != ENOSYS) {
                perror("fallocate");
                goto fail;
            }
            if (ecopy_ftruncate_nocancel(ctx->fd_out, ctx->src_st.st_size) != 0) {
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

    /* Register before spawning threads so workers_request_stop() can fail
     * this file's parked reader/writer threads on Ctrl+C. Removed in
     * finish_large_file_ctx() (also on the fail_started path, which runs
     * finish synchronously). */
    pthread_mutex_lock(&g_queue_lock);
    ctx->next_active = g_active_large;
    g_active_large = ctx;
    pthread_mutex_unlock(&g_queue_lock);

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
        ecopy_close_nocancel(ctx->fd_in);
    }
    if (ctx->fd_out >= 0) {
        ecopy_close_nocancel(ctx->fd_out);
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

/* Free slots a parked producer should see before it is worth waking. */
static int space_wake_min(void)
{
    int m = g_max_queued_files / 4;
    if (m > SPACE_WAKE_MIN) {
        m = SPACE_WAKE_MIN;
    }
    return m > 0 ? m : 1;
}

/* Caller holds g_queue_lock; call after removing work from the queues. */
static void wake_producer_if_room_locked(void)
{
    if (g_space_waiters > 0 &&
        g_max_queued_files - (int)(g_small_q.len + g_large_heap.len) >= space_wake_min()) {
        pthread_cond_signal(&g_space_cond);
    }
}

/*
 * Small-file claims are refilled in batches: a worker pops up to
 * SMALL_CLAIM_BATCH tasks under one g_queue_lock acquisition into a local
 * stash and only re-enters the scheduler when the stash is empty, cutting
 * lock round-trips per dispatched file. A stash is a run of consecutive
 * files from one directory batch (the batch is then rotated behind the other
 * queued directories, see small_queue_t).
 *
 * g_small_workers_active counts worker THREADS holding a stash, not stashed
 * tasks. A thread takes one slot when it claims its first stash, keeps it
 * across refills, and gives it back only when it finds the small queue
 * empty. So the small worker limit is the number of files copied
 * concurrently, and a completed file needs no lock at all: finished tasks
 * stay in the stash and are recycled in bulk at the next refill. (Counting
 * tasks instead made a limit of 32 with stashes of 8 mean four busy threads,
 * and every completion a wake of a parked one.)
 */
#define SMALL_CLAIM_BATCH 8

/* Return the finished tasks in stash[0..head) and reset it. Called with the
 * queue lock held; directory refs are dropped by the caller beforehand. */
static void stash_recycle_locked(file_task_t **stash, int *stash_head, int *stash_count)
{
    for (int k = 0; k < *stash_head; k++) {
        stash[k]->next = g_task_freelist;
        g_task_freelist = stash[k];
    }
    *stash_head = 0;
    *stash_count = 0;
}

static work_claim_t dequeue_work(file_task_t **stash, int *stash_head,
                                 int *stash_count, int *small_slot)
{
    work_claim_t claim;
    memset(&claim, 0, sizeof(claim));

    if (*stash_head < *stash_count &&
        !atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
        claim.kind = WORK_SMALL_FILE;
        claim.file_task = stash[(*stash_head)++];
        return claim;
    }

    /* Drop directory refs of the finished tasks outside the queue lock;
     * the task structs themselves are recycled under it below. */
    for (int k = 0; k < *stash_head; k++) {
        dir_handle_release(stash[k]->dir);
    }

    pthread_mutex_lock(&g_queue_lock);

    if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
        /*
         * Ctrl+C: abandon instead of drain. Recycle the stash remainder and
         * wake any producer parked on a full queue so it can observe the
         * shutdown flag; tasks still in the queue/heap are freed by
         * workers_stop().
         */
        while (*stash_head < *stash_count) {
            dir_handle_release(stash[(*stash_head)++]->dir);
        }
        stash_recycle_locked(stash, stash_head, stash_count);
        if (*small_slot) {
            *small_slot = 0;
            if (g_small_workers_active > 0) {
                g_small_workers_active--;
            }
        }
        pthread_cond_broadcast(&g_space_cond);
        pthread_mutex_unlock(&g_queue_lock);
        return claim;
    }

    stash_recycle_locked(stash, stash_head, stash_count);

    for (;;) {
        int total_slots_used = total_worker_slots_used_locked();

        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
            /* Parked worker woken by workers_request_stop(): leave queued
             * work for workers_stop() to free and wake producers so they
             * can unwind too. */
            pthread_cond_broadcast(&g_space_cond);
            break;
        }


        if (g_large_heap.len > 0 &&
            (int)g_large_workers_active < g_max_active_large_files &&
            total_slots_used + g_large_worker_count <= g_worker_count) {
            claim.kind = WORK_LARGE_FILE_START;
            claim.file_task = heap_pop_max(&g_large_heap);
            g_large_workers_active++;
            wake_producer_if_room_locked();
            break;
        }

        if (g_small_q.len > 0 &&
            (*small_slot ||
             ((int)g_small_workers_active < g_small_worker_limit &&
              total_slots_used + 1 <= g_worker_count))) {
            dir_batch_t *b = smallq_pop_batch(&g_small_q);
            if (!*small_slot) {
                *small_slot = 1;
                g_small_workers_active++;
            }
            *stash_head = 0;
            *stash_count = 0;
            while (*stash_count < SMALL_CLAIM_BATCH && b->head < b->count) {
                stash[(*stash_count)++] = b->tasks[b->head++];
            }
            wake_producer_if_room_locked();
            /* Rotate a part-claimed directory behind the others so the next
             * claimant works in a different directory. */
            if (b->head < b->count) {
                smallq_push_batch(&g_small_q, b);
            } else {
                free(b);
            }
            /*
             * The stash is drained over the next several file copies, and
             * each task struct was last written by a traversal thread, i.e.
             * it currently lives in another core's cache. Pull the fixed
             * part (and the first tail line with the path strings) into this
             * core's cache now so the per-file loop does not stall on the
             * cross-core handoff. Seven lines cover the fixed part plus the
             * typical src+dst+name tail (~200-350 B). stash[0] is dispatched
             * immediately, so prefetching it would have no lead time.
             */
            for (int k = 1; k < *stash_count; k++) {
                const char *p = (const char *)stash[k];
                __builtin_prefetch(p, 0, 3);
                __builtin_prefetch(p + 64, 0, 3);
                __builtin_prefetch(p + 128, 0, 3);
                __builtin_prefetch(p + 192, 0, 3);
                __builtin_prefetch(p + 256, 0, 3);
                __builtin_prefetch(p + 320, 0, 3);
                __builtin_prefetch(p + 384, 0, 3);
            }
            claim.kind = WORK_SMALL_FILE;
            claim.file_task = stash[(*stash_head)++];
            break;
        }

        /* Nothing small to refill with: give the thread slot back. If that
         * was the last outstanding work after the producers finished, every
         * parked worker must see the termination condition. */
        if (*small_slot) {
            *small_slot = 0;
            if (g_small_workers_active > 0) {
                g_small_workers_active--;
            }
            if (g_queue_done && g_small_q.len == 0 && g_large_heap.len == 0 &&
                g_small_workers_active == 0 && g_large_workers_active == 0) {
                pthread_cond_broadcast(&g_queue_cond);
            }
        }

        if (g_small_q.len == 0 && g_large_heap.len == 0 &&
            g_queue_done && g_large_workers_active == 0 && g_small_workers_active == 0) {
            break;
        }

        {
            uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
            g_queue_waiters++;
            pthread_cond_wait(&g_queue_cond, &g_queue_lock);
            g_queue_waiters--;
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
        ecopy_close_nocancel(fd_in);
        return -1;
    }

    while (pos < size) {
        ssize_t r = pread_nocancel(fd_in, (char *)buf + pos, (size_t)(size - pos), pos);
        if (r < 0) { perror("pread"); ecopy_close_nocancel(fd_in); return -1; }
        if (r == 0) break; /* file shrank under us; send what we have */
        pos += r;
    }
    ecopy_close_nocancel(fd_in);

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
    int sparse = task->sparse;
    size_t chunk = (g_chunk_size > 0) ? (size_t)g_chunk_size : (size_t)(1024 * 1024);
    sshx_file_t *f = NULL;

    init_runtime_config();
    stats_set_current_file(task->src, task->src_len, (uint64_t)size, 0);

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
        while (pos < size &&
               !atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
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
        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
            goto out;
        }
        if (sshx_file_ftruncate(f, size) != 0) {
            goto out;
        }
    } else {
        off_t pos = 0;
        while (pos < size &&
               !atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
            /* Chunk-aligned count keeps O_DIRECT reads valid up to EOF. */
            ssize_t r = pread_nocancel(fd_in, buf, chunk, pos);
            if (r < 0) { perror("pread"); goto out; }
            if (r == 0) break;
            if (sshx_file_write(f, buf, (size_t)r, pos) != 0) goto out;
            pos += r;
            record_progress_bytes((uint64_t)r, 1);
            if (payload_bytes) *payload_bytes += (uint64_t)r;
        }
        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
            goto out;
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
        ecopy_close_nocancel(fd_in);
    }
    stats_clear_current_file(task->src);
    return rc;
}

/* -------------------- worker threads -------------------- */

static void *worker_main(void *arg)
{
    file_task_t *stash[SMALL_CLAIM_BATCH];
    int stash_head = 0;
    int stash_count = 0;
    int small_slot = 0;

    /* Bind this worker to one SSH connection of the pool so a streamed file's
     * OPEN/WRITE/COMMIT frames all land on the same server (a no-op locally). */
    sshx_bind_thread((int)(intptr_t)arg);

    for (;;) {
        work_claim_t claim = dequeue_work(stash, &stash_head, &stash_count, &small_slot);
        if (claim.kind == WORK_NONE) {
            break;
        }

        if (claim.kind == WORK_SMALL_FILE) {
            uint64_t service_start_ns = monotonic_ns();
            uint64_t payload_bytes = 0;
            int sparse = claim.file_task->sparse;
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

            /* The finished task stays in the stash and is recycled at the
             * next refill; no queue lock per completed file. */
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

    progress_flush_bytes();
    stats_flush_io_op_counts();
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
    g_small_q.head = g_small_q.tail = NULL;
    g_small_q.len = 0;
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

void workers_request_stop(void)
{
    pthread_mutex_lock(&g_queue_lock);
    g_queue_done = 1;
    pthread_cond_broadcast(&g_queue_cond);
    pthread_cond_broadcast(&g_large_done_cond);
    /*
     * Fail every in-flight large file: without this its parked reader/writer
     * threads would stay asleep on the per-file condvars (nothing else wakes
     * them) and workers_stop() would hang waiting for the slot release.
     */
    for (large_file_ctx_t *c = g_active_large; c; c = c->next_active) {
        pthread_mutex_lock(&c->lock);
        if (!c->failed) {
            mark_large_file_failed_locked(c);
        }
        pthread_mutex_unlock(&c->lock);
    }
    pthread_mutex_unlock(&g_queue_lock);
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
     * drains both queues to empty) and release the backing arrays.
     */
    while (g_small_q.head) {
        dir_batch_t *b = smallq_pop_batch(&g_small_q);
        for (uint32_t i = b->head; i < b->count; i++) {
            dir_handle_release(b->tasks[i]->dir);
            free(b->tasks[i]);
        }
        free(b);
    }
    for (size_t i = 0; i < g_large_heap.len; i++) {
        dir_handle_release(g_large_heap.items[i].task->dir);
        free(g_large_heap.items[i].task);
    }
    g_small_q.head = g_small_q.tail = NULL;
    g_small_q.len = 0;
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

/*
 * file_task_t path strings live in one exact-length flexible tail instead of
 * three PATH_MAX arrays, so a queued file costs ~300 B rather than ~12 KiB.
 * The join rule is parent + optional '/' + name (no double slash when the
 * parent already ends in '/').
 */
static size_t file_task_data_need(const dir_handle_t *dir, size_t name_len)
{
    size_t src_plen = strlen(dir->src);
    size_t dst_plen = strlen(dir->dst);
    size_t src_sep = (src_plen > 0 && dir->src[src_plen - 1] == '/') ? 0 : 1;
    size_t dst_sep = (dst_plen > 0 && dir->dst[dst_plen - 1] == '/') ? 0 : 1;

    return (src_plen + src_sep + name_len + 1) +
           (dst_plen + dst_sep + name_len + 1) +
           (name_len + 1);
}

static void file_task_fill_paths(file_task_t *t,
                                 const dir_handle_t *dir,
                                 const char *name,
                                 size_t name_len)
{
    size_t src_plen = strlen(dir->src);
    size_t dst_plen = strlen(dir->dst);
    size_t src_sep = (src_plen > 0 && dir->src[src_plen - 1] == '/') ? 0 : 1;
    size_t dst_sep = (dst_plen > 0 && dir->dst[dst_plen - 1] == '/') ? 0 : 1;
    char *p = t->data;

    t->src = p;
    memcpy(p, dir->src, src_plen);
    p += src_plen;
    if (src_sep) {
        *p++ = '/';
    }
    memcpy(p, name, name_len + 1);
    p += name_len + 1;
    t->src_len = (uint32_t)(src_plen + src_sep + name_len);

    t->dst = p;
    memcpy(p, dir->dst, dst_plen);
    p += dst_plen;
    if (dst_sep) {
        *p++ = '/';
    }
    memcpy(p, name, name_len + 1);
    p += name_len + 1;

    t->name = p;
    memcpy(p, name, name_len + 1);
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
        size_t name_len, need;
        if (!items[i].name || !items[i].src_st) {
            errno = EINVAL;
            goto fail;
        }
        name_len = strlen(items[i].name);
        need = file_task_data_need(dir, name_len);
        t = NULL;
        if (spares) {
            t = spares;
            spares = t->next;
            if (t->data_cap < need) {
                /* Right-sizing matters more than recycling a too-small node. */
                free(t);
                t = NULL;
            }
        }
        if (!t) {
            t = malloc(sizeof(*t) + need);
            if (!t) {
                errno = ENOMEM;
                goto fail;
            }
            t->data_cap = need;
        }

        file_task_fill_paths(t, dir, items[i].name, name_len);
        dir_handle_retain(dir);
        t->dir = dir;
        t->src_st = *items[i].src_st;
        t->sparse = (unsigned char)classify_sparse(dir, items[i].name, items[i].src_st);
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
        dir_batch_t *db;

        /* Sized for the whole remainder (>= this slice) so the fill under
         * the lock never allocates. */
        db = malloc(sizeof(*db) + built * sizeof(db->tasks[0]));
        if (!db) {
            errno = ENOMEM;
            goto fail;
        }
        db->head = 0;
        db->count = 0;

        pthread_mutex_lock(&g_queue_lock);
        {
            /* Park until the whole remainder fits, or at least a
             * space_wake_min() slice does (the consumer wakes us at that
             * point, never for a handful of slots). */
            int need = (int)built < space_wake_min() ? (int)built : space_wake_min();
            while (g_max_queued_files - (int)(g_small_q.len + g_large_heap.len) < need &&
                   !atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
                uint64_t wait_start_ns = g_collect_wait_timing ? monotonic_ns() : 0;
                g_space_waiters++;
                pthread_cond_wait(&g_space_cond, &g_queue_lock);
                g_space_waiters--;
                if (g_collect_wait_timing) {
                    stats_record_queue_wait_ns(monotonic_ns() - wait_start_ns);
                }
            }
        }
        if (atomic_load_explicit(&g_shutdown_requested, memory_order_relaxed)) {
            pthread_mutex_unlock(&g_queue_lock);
            free(db);
            errno = ECANCELED;
            goto fail;
        }
        room = (size_t)(g_max_queued_files -
                        (int)(g_small_q.len + g_large_heap.len));
        take = built < room ? built : room;

        /*
         * Reserve large-heap capacity for this slice before pushing so the
         * pushes are infallible (the small batch was sized above). On OOM
         * bail out (the fail path frees the remaining batch and
         * already-queued items stay valid).
         */
        {
            size_t large_add = 0;
            file_task_t *scan = batch_head;
            for (size_t i = 0; i < take; i++) {
                int use_large = task_uses_large_pipeline(scan);
                if (use_large) {
                    large_add++;
                }
                scan = scan->next;
            }
            if (heap_reserve(&g_large_heap, g_large_heap.len + large_add) != 0) {
                pthread_mutex_unlock(&g_queue_lock);
                free(db);
                errno = ENOMEM;
                goto fail;
            }
        }

        for (size_t i = 0; i < take; i++) {
            file_task_t *t = batch_head;
            int use_large;
            batch_head = t->next;
            t->next = NULL;

            use_large = task_uses_large_pipeline(t);
            /*
             * The large heap is keyed: allocated-bytes weight when size
             * priority is on (biggest data first), else a decreasing sequence
             * so it yields FIFO order. Small files go into this directory's
             * batch — size priority among sub-threshold files has no
             * load-balance value, so g_size_priority now only orders the
             * large heap.
             */
            if (use_large) {
                t->sched_key = g_size_priority ? task_weight(&t->src_st)
                                               : (UINT64_MAX - g_enqueue_seq++);
                heap_push(&g_large_heap, t);
            } else {
                db->tasks[db->count++] = t;
            }
        }
        if (db->count > 0) {
            smallq_push_batch(&g_small_q, db);
            db = NULL;
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

            if (g_small_q.len > 0 && free_slots > 0) {
                int stashes = ((int)g_small_q.len + SMALL_CLAIM_BATCH - 1) /
                              SMALL_CLAIM_BATCH;
                small_wake = g_small_worker_limit -
                             (int)g_small_workers_active;
                if (small_wake > stashes) {
                    small_wake = stashes;
                }
                if (small_wake > free_slots) {
                    small_wake = free_slots;
                }
            }

            /*
             * One wake per worker that can actually take a slot and a
             * stash; every signal that finds a waiter is a futex wake plus
             * the woken thread's lock reacquisition, so never over-wake.
             * Under-waking cannot strand work: a worker whose stash runs
             * dry refills from the queue before it would park.
             */
            int wakes = large_wake + small_wake;
            if (wakes > g_queue_waiters) {
                wakes = g_queue_waiters;
            }
            for (int i = 0; i < wakes; i++) {
                pthread_cond_signal(&g_queue_cond);
            }
        }
        pthread_mutex_unlock(&g_queue_lock);
        free(db); /* NULL if it was queued; otherwise this slice was all large */

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

uint64_t workers_small_queue_depth(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_queue_lock);
    v = g_small_q.len;
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
