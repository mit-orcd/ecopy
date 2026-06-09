/*
 * workers.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "workers.h"
#include "config.h"
#include "types.h"
#include "stats.h"
#include "fs_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <time.h>

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
static int g_small_inplace = 0;

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
static file_task_t    *g_small_queue_head = NULL;
static file_task_t    *g_small_queue_tail = NULL;
static file_task_t    *g_large_queue_head = NULL;
static file_task_t    *g_large_queue_tail = NULL;
static int             g_queue_done = 0;
static uint64_t        g_small_queue_depth = 0;
static uint64_t        g_large_queue_depth = 0;
static uint64_t        g_small_workers_active = 0;
static uint64_t        g_large_workers_active = 0;

static int g_workers_error = 0;
static pthread_mutex_t g_workers_error_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_explicit_large_readers = 0;
static int g_explicit_large_writers = 0;

static uint64_t monotonic_ns(void);

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

    g_small_worker_limit = env_int_or_default("DIRECT_COPY_SMALL_MAX_WORKERS",
                                             SMALL_WORKER_SLOTS,
                                             1,
                                             g_worker_count);

    {
        const char *s = getenv("DIRECT_COPY_SMALL_INPLACE");
        g_small_inplace = (s && *s && strcmp(s, "0") != 0) ? 1 : 0;
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

static int runtime_small_inplace(void)
{
    init_runtime_config();
    return g_small_inplace;
}

/* -------------------- queue helpers -------------------- */

static int enqueue_task(file_task_t **head,
                        file_task_t **tail,
                        uint64_t *depth,
                        dir_handle_t *dir,
                        const char *name,
                        const char *src,
                        const char *dst,
                        const struct stat *src_st)
{
    file_task_t *t = (file_task_t *)calloc(1, sizeof(*t));
    if (!t) {
        perror("calloc");
        return -1;
    }

    dir_handle_retain(dir);
    t->dir = dir;
    snprintf(t->name, sizeof(t->name), "%s", name);
    snprintf(t->src, sizeof(t->src), "%s", src);
    snprintf(t->dst, sizeof(t->dst), "%s", dst);
    t->src_st = *src_st;
    t->next = NULL;

    pthread_mutex_lock(&g_queue_lock);
    while ((int)(g_small_queue_depth + g_large_queue_depth) >= g_max_queued_files) {
        uint64_t wait_start_ns = monotonic_ns();
        pthread_cond_wait(&g_space_cond, &g_queue_lock);
        stats_record_queue_wait_ns(monotonic_ns() - wait_start_ns);
    }
    if (*tail) {
        (*tail)->next = t;
    } else {
        *head = t;
    }
    *tail = t;
    (*depth)++;
    pthread_cond_signal(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);

    return 0;
}

static void free_file_task(file_task_t *task)
{
    if (!task) {
        return;
    }
    dir_handle_release(task->dir);
    free(task);
}

static file_task_t *pop_file_task(file_task_t **head, file_task_t **tail)
{
    file_task_t *t = *head;
    *head = t->next;
    if (!*head) {
        *tail = NULL;
    }
    t->next = NULL;
    return t;
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

static void record_progress_bytes(uint64_t bytes, int use_current_file_stats)
{
    if (use_current_file_stats) {
        stats_advance_current_file(bytes);
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

static int copy_file_serial_small(file_task_t *task)
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
    int inplace = runtime_small_inplace();
    const char *write_name;
    char tmp_name[PATH_MAX] = "";

    init_runtime_config();
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

    if (posix_memalign(&buf, ALIGNMENT, (size_t)g_chunk_size) != 0) {
        fprintf(stderr, "posix_memalign failed\n");
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
            uint64_t read_start_ns = monotonic_ns();
            ssize_t r = read(fd_in, buf, len);
            stats_record_read_io(monotonic_ns() - read_start_ns);
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
                uint64_t write_start_ns = monotonic_ns();
                ssize_t w = write(fd_out, (char *)buf + done, len - done);
                stats_record_write_io(monotonic_ns() - write_start_ns);
                if (w < 0) {
                    perror("write");
                    goto out;
                }
                done += (size_t)w;
            }
        }

        advise_source_consumed(fd_in, pos, (off_t)len);
        pos += this_len_off;
        record_progress_bytes((uint64_t)len, 1);
    }

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

out:
    free(buf);
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
            uint64_t wait_start_ns = monotonic_ns();
            pthread_cond_wait(&ctx->free_cond, &ctx->lock);
            stats_record_reader_buffer_wait_ns(monotonic_ns() - wait_start_ns);
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
            uint64_t read_start_ns = monotonic_ns();
            ssize_t r = pread(ctx->fd_in, buf->data, len, offset);
            stats_record_read_io(monotonic_ns() - read_start_ns);
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
        advise_source_consumed(ctx->fd_in, offset, (off_t)len);

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
        stats_record_ready_queue_depth(ctx->ready_count);
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
            uint64_t wait_start_ns = monotonic_ns();
            pthread_cond_wait(&ctx->ready_cond, &ctx->lock);
            stats_record_writer_data_wait_ns(monotonic_ns() - wait_start_ns);
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
        stats_record_ready_queue_depth(ctx->ready_count);
        pthread_mutex_unlock(&ctx->lock);

        {
            size_t done = 0;
            int failed = 0;

            while (done < buf->len) {
                uint64_t write_start_ns = monotonic_ns();
                ssize_t w = pwrite(ctx->fd_out,
                                   (char *)buf->data + done,
                                   buf->len - done,
                                   buf->offset + (off_t)done);
                stats_record_write_io(monotonic_ns() - write_start_ns);
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

            pthread_mutex_lock(&ctx->lock);
            if (failed) {
                mark_large_file_failed_locked(ctx);
            } else {
                stats_add_bytes((uint64_t)buf->len);
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
            stats_inc_files_copied();
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

    if (ftruncate(ctx->fd_out, ctx->src_st.st_size) != 0) {
        perror("ftruncate");
        goto fail;
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

        if (g_large_queue_head &&
            (int)g_large_workers_active < g_max_active_large_files &&
            total_slots_used + g_large_worker_count <= g_worker_count) {
            claim.kind = WORK_LARGE_FILE_START;
            claim.file_task = pop_file_task(&g_large_queue_head, &g_large_queue_tail);
            if (g_large_queue_depth > 0) {
                g_large_queue_depth--;
            }
            g_large_workers_active++;
            pthread_cond_signal(&g_space_cond);
            break;
        }

        if (g_small_queue_head &&
            (int)g_small_workers_active < g_small_worker_limit &&
            total_slots_used + 1 <= g_worker_count) {
            claim.kind = WORK_SMALL_FILE;
            claim.file_task = pop_file_task(&g_small_queue_head, &g_small_queue_tail);
            if (g_small_queue_depth > 0) {
                g_small_queue_depth--;
            }
            g_small_workers_active++;
            pthread_cond_signal(&g_space_cond);
            break;
        }

        if (!g_small_queue_head && !g_large_queue_head &&
            g_queue_done && g_large_workers_active == 0 && g_small_workers_active == 0) {
            break;
        }

        {
            uint64_t wait_start_ns = monotonic_ns();
            pthread_cond_wait(&g_queue_cond, &g_queue_lock);
            stats_record_queue_wait_ns(monotonic_ns() - wait_start_ns);
        }
    }

    pthread_mutex_unlock(&g_queue_lock);
    return claim;
}

/* -------------------- worker threads -------------------- */

static void *worker_main(void *arg)
{
    (void)arg;

    for (;;) {
        work_claim_t claim = dequeue_work();
        if (claim.kind == WORK_NONE) {
            break;
        }

        if (claim.kind == WORK_SMALL_FILE) {
            if (copy_file_serial_small(claim.file_task) == 0) {
                stats_inc_files_copied();
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
            if (g_queue_done && !g_small_queue_head && !g_large_queue_head &&
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

    return NULL;
}

/* -------------------- public API -------------------- */

int workers_start(void)
{
    int i;

    init_runtime_config();
    validate_runtime_config();
    clear_worker_error();

    pthread_mutex_lock(&g_queue_lock);
    g_queue_done = 0;
    g_small_queue_head = NULL;
    g_small_queue_tail = NULL;
    g_large_queue_head = NULL;
    g_large_queue_tail = NULL;
    g_small_queue_depth = 0;
    g_large_queue_depth = 0;
    g_small_workers_active = 0;
    g_large_workers_active = 0;
    pthread_mutex_unlock(&g_queue_lock);

    g_workers = calloc((size_t)g_worker_count, sizeof(*g_workers));
    if (!g_workers) {
        perror("calloc");
        return -1;
    }

    for (i = 0; i < g_worker_count; i++) {
        if (pthread_create(&g_workers[i], NULL, worker_main, NULL) != 0) {
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
    pthread_mutex_unlock(&g_queue_lock);

    stats_set_file_work_drained();

    free(g_workers);
    g_workers = NULL;
}

int workers_enqueue_small_file(dir_handle_t *dir,
                               const char *name,
                               const char *src,
                               const char *dst,
                               const struct stat *src_st)
{
    init_runtime_config();

    if (src_st->st_size > runtime_large_threshold()) {
        return enqueue_task(&g_large_queue_head,
                            &g_large_queue_tail,
                            &g_large_queue_depth,
                            dir,
                            name,
                            src,
                            dst,
                            src_st);
    }

    return enqueue_task(&g_small_queue_head,
                        &g_small_queue_tail,
                        &g_small_queue_depth,
                        dir,
                        name,
                        src,
                        dst,
                        src_st);
}

int workers_enqueue_large_file(dir_handle_t *dir,
                               const char *name,
                               const char *src,
                               const char *dst,
                               const struct stat *src_st)
{
    return enqueue_task(&g_large_queue_head,
                        &g_large_queue_tail,
                        &g_large_queue_depth,
                        dir,
                        name,
                        src,
                        dst,
                        src_st);
}

uint64_t workers_small_queue_depth(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_queue_lock);
    v = g_small_queue_depth;
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
    v = g_large_queue_depth;
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
    printf("  small in-place writes       : %s\n", g_small_inplace ? "yes" : "no");
    printf("  read direct_io enabled      : %s\n", read_direct_io_enabled() ? "yes" : "no");
    printf("  write direct_io enabled     : %s\n", write_direct_io_enabled() ? "yes" : "no");
    printf("  copy_file_range enabled     : %s\n", copy_file_range_enabled() ? "yes" : "no");
    fflush(stdout);
}
