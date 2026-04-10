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
    struct stat src_st;
    off_t bulk_end;
    off_t next_read_offset;
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
static off_t g_chunk_size = 0;

/* -------------------- scheduler state -------------------- */

static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_queue_cond = PTHREAD_COND_INITIALIZER;
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

/* -------------------- runtime config -------------------- */

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

static void init_runtime_config(void)
{
    if (g_worker_count > 0 && g_large_worker_count > 0 && g_large_file_inflight > 0 && g_chunk_size > 0) {
        return;
    }

    g_worker_count = env_int_or_default("DIRECT_COPY_MAX_WORKERS",
                                        MAX_WORKER_SLOTS,
                                        1,
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

    if (g_explicit_large_readers > 0 && g_explicit_large_writers > 0) {
        g_large_worker_count = g_explicit_large_readers + g_explicit_large_writers;
    }

    g_large_file_inflight = env_int_or_default("DIRECT_COPY_LARGE_FILE_INFLIGHT",
                                               g_large_worker_count,
                                               1,
                                               1024);

    {
        int chunk_mb = env_int_or_default("DIRECT_COPY_CHUNK_MB",
                                          (int)(CHUNK_SIZE / (1024 * 1024)),
                                          1,
                                          4096);
        g_chunk_size = (off_t)chunk_mb * 1024 * 1024;
    }

    g_max_active_large_files = g_worker_count / g_large_worker_count;
    if (g_max_active_large_files < 1) {
        g_max_active_large_files = 1;
    }
}

static void get_pipeline_thread_counts(int *reader_count, int *writer_count)
{
    int readers;
    int writers;

    init_runtime_config();

    if (g_explicit_large_readers > 0 && g_explicit_large_writers > 0) {
        readers = g_explicit_large_readers;
        writers = g_explicit_large_writers;
    } else {
        readers = (g_large_worker_count + 1) / 2;
        writers = g_large_worker_count - readers;
        if (writers < 1) {
            writers = 1;
            readers = g_large_worker_count - 1;
        }
        if (readers < 1) {
            readers = 1;
        }
    }

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
    return g_chunk_size * 10;
}

/* -------------------- queue helpers -------------------- */

static int enqueue_task(file_task_t **head,
                        file_task_t **tail,
                        uint64_t *depth,
                        const char *src,
                        const char *dst,
                        const struct stat *src_st)
{
    file_task_t *t = (file_task_t *)calloc(1, sizeof(*t));
    if (!t) {
        perror("calloc");
        return -1;
    }

    snprintf(t->src, sizeof(t->src), "%s", src);
    snprintf(t->dst, sizeof(t->dst), "%s", dst);
    t->src_st = *src_st;
    t->next = NULL;

    pthread_mutex_lock(&g_queue_lock);
    if (*tail) {
        (*tail)->next = t;
    } else {
        *head = t;
    }
    *tail = t;
    (*depth)++;
    pthread_cond_broadcast(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);

    return 0;
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

static int direct_io_fallback_errno_local(int err)
{
    return err == EINVAL ||
           err == EOPNOTSUPP ||
           err == ENOTSUP ||
           err == ENOSYS;
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

static int open_write_existing_maybe_direct(const char *path, int *used_direct)
{
    int fd;

    if (used_direct) {
        *used_direct = 0;
    }

    if (write_direct_io_enabled()) {
        fd = open(path, O_WRONLY | O_DIRECT);
        if (fd >= 0) {
            if (used_direct) {
                *used_direct = 1;
            }
            stats_record_write_open(1);
            return fd;
        }

        if (!direct_io_fallback_errno_local(errno)) {
            perror(path);
            return -1;
        }
    }

    fd = open(path, O_WRONLY);
    if (fd < 0) {
        perror(path);
        return -1;
    }

    stats_record_write_open(0);
    return fd;
}

static int prepare_destination_file(const char *dst, const struct stat *src_st)
{
    int out_direct = 0;
    int fd = open_write_maybe_direct(dst, src_st->st_mode & 07777, &out_direct);
    if (fd < 0) {
        return -1;
    }

    if (ftruncate(fd, src_st->st_size) != 0) {
        perror("ftruncate");
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

static int copy_tail_buffered(const char *src,
                              const char *dst,
                              off_t start,
                              off_t end,
                              int use_current_file_stats)
{
    if (start >= end) {
        return 0;
    }

    int fd_in = open(src, O_RDONLY);
    if (fd_in < 0) {
        perror(src);
        return -1;
    }
    stats_record_read_open(0);

    int fd_out = open(dst, O_WRONLY);
    if (fd_out < 0) {
        perror(dst);
        close(fd_in);
        return -1;
    }
    stats_record_write_open(0);

    advise_source_streaming(fd_in);
    advise_dest_streaming(fd_out);

    if (lseek(fd_in, start, SEEK_SET) < 0) {
        perror("lseek src");
        close(fd_in);
        close(fd_out);
        return -1;
    }

    if (lseek(fd_out, start, SEEK_SET) < 0) {
        perror("lseek dst");
        close(fd_in);
        close(fd_out);
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
                close(fd_in);
                close(fd_out);
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
                        close(fd_in);
                        close(fd_out);
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

    close(fd_in);
    close(fd_out);
    return 0;
}

static int copy_file_serial_small(const char *src, const char *dst, const struct stat *src_st)
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

    init_runtime_config();
    copy_range_available = copy_file_range_enabled();
    size = src_st->st_size;
    bulk_end = (size / ALIGNMENT) * ALIGNMENT;

    fd_in = open_read_maybe_direct(src, &in_direct);
    if (fd_in < 0) {
        return -1;
    }

    fd_out = open_write_maybe_direct(dst, src_st->st_mode & 07777, &out_direct);
    if (fd_out < 0) {
        close(fd_in);
        return -1;
    }

    if (ftruncate(fd_out, size) != 0) {
        perror("ftruncate");
        close(fd_in);
        close(fd_out);
        return -1;
    }

    if (!in_direct) {
        advise_source_streaming(fd_in);
    }
    if (!out_direct) {
        advise_dest_streaming(fd_out);
    }

    if (posix_memalign(&buf, ALIGNMENT, (size_t)g_chunk_size) != 0) {
        fprintf(stderr, "posix_memalign failed\n");
        close(fd_in);
        close(fd_out);
        return -1;
    }

    while (pos < bulk_end) {
        off_t remain = bulk_end - pos;
        off_t this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
        size_t len = (size_t)this_len_off;

        if (!in_direct && !out_direct && copy_range_available) {
            off_t in_off = pos;
            off_t out_off = pos;
            int cfr_rc = copy_file_range_with_progress(fd_in, fd_out, &in_off, &out_off, len, 0);
            if (cfr_rc == 0) {
                pos += this_len_off;
                continue;
            }
            if (cfr_rc < 0) {
                free(buf);
                close(fd_in);
                close(fd_out);
                return -1;
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
                free(buf);
                close(fd_in);
                close(fd_out);
                return -1;
            }
            if ((size_t)r != len) {
                fprintf(stderr, "short read on %s: expected %zu got %zd\n", src, len, r);
                free(buf);
                close(fd_in);
                close(fd_out);
                return -1;
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
                    free(buf);
                    close(fd_in);
                    close(fd_out);
                    return -1;
                }
                done += (size_t)w;
            }
        }

        advise_source_consumed(fd_in, pos, (off_t)len);
        pos += this_len_off;
        stats_add_bytes((uint64_t)len);
    }

    free(buf);
    close(fd_in);
    close(fd_out);

    if (copy_tail_buffered(src, dst, bulk_end, size, 0) != 0) {
        return -1;
    }

    return finalize_copied_file(dst, src_st);
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
                ctx->failed = 1;
                enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
                ctx->free_count++;
                pthread_cond_broadcast(&ctx->ready_cond);
                pthread_cond_broadcast(&ctx->free_cond);
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
                ctx->failed = 1;
                enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
                ctx->free_count++;
                pthread_cond_broadcast(&ctx->ready_cond);
                pthread_cond_broadcast(&ctx->free_cond);
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
                ctx->failed = 1;
            } else {
                stats_add_bytes((uint64_t)buf->len);
            }
            enqueue_buffer(&ctx->free_head, &ctx->free_tail, buf);
            ctx->free_count++;
            pthread_cond_signal(&ctx->free_cond);
            if (ctx->failed) {
                pthread_cond_broadcast(&ctx->ready_cond);
            }
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
        if (copy_tail_buffered(ctx->src, ctx->dst, ctx->bulk_end, ctx->src_st.st_size, 0) != 0) {
            rc = -1;
        } else if (finalize_copied_file(ctx->dst, &ctx->src_st) != 0) {
            rc = -1;
        } else {
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
    ctx->src_st = task->src_st;
    ctx->bulk_end = (task->src_st.st_size / ALIGNMENT) * ALIGNMENT;
    ctx->next_read_offset = 0;
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

    if (prepare_destination_file(ctx->dst, &ctx->src_st) != 0) {
        goto fail;
    }

    ctx->fd_in = open_read_maybe_direct(ctx->src, &ctx->in_direct);
    if (ctx->fd_in < 0) {
        goto fail;
    }

    ctx->fd_out = open_write_existing_maybe_direct(ctx->dst, &ctx->out_direct);
    if (ctx->fd_out < 0) {
        goto fail;
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
    ctx->failed = 1;
    pthread_cond_broadcast(&ctx->free_cond);
    pthread_cond_broadcast(&ctx->ready_cond);
    pthread_mutex_unlock(&ctx->lock);
    finish_large_file_ctx(ctx);
    return -1;

fail:
    if (ctx->fd_in >= 0) {
        close(ctx->fd_in);
    }
    if (ctx->fd_out >= 0) {
        close(ctx->fd_out);
    }
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

static work_claim_t dequeue_work(void)
{
    work_claim_t claim;
    memset(&claim, 0, sizeof(claim));

    pthread_mutex_lock(&g_queue_lock);

    for (;;) {
        if (g_small_queue_head) {
            claim.kind = WORK_SMALL_FILE;
            claim.file_task = pop_file_task(&g_small_queue_head, &g_small_queue_tail);
            if (g_small_queue_depth > 0) {
                g_small_queue_depth--;
            }
            g_small_workers_active++;
            break;
        }

        if (g_large_queue_head && (int)g_large_workers_active < g_max_active_large_files) {
            claim.kind = WORK_LARGE_FILE_START;
            claim.file_task = pop_file_task(&g_large_queue_head, &g_large_queue_tail);
            if (g_large_queue_depth > 0) {
                g_large_queue_depth--;
            }
            g_large_workers_active++;
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
            if (copy_file_serial_small(claim.file_task->src, claim.file_task->dst, &claim.file_task->src_st) == 0) {
                stats_inc_files_copied();
            } else {
                mark_worker_error();
            }

            free(claim.file_task);

            pthread_mutex_lock(&g_queue_lock);
            if (g_small_workers_active > 0) {
                g_small_workers_active--;
            }
            pthread_cond_broadcast(&g_queue_cond);
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
            free(claim.file_task);
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

    free(g_workers);
    g_workers = NULL;
}

int workers_enqueue_small_file(const char *src, const char *dst, const struct stat *src_st)
{
    init_runtime_config();

    if (src_st->st_size > runtime_large_threshold()) {
        return enqueue_task(&g_large_queue_head,
                            &g_large_queue_tail,
                            &g_large_queue_depth,
                            src,
                            dst,
                            src_st);
    }

    return enqueue_task(&g_small_queue_head,
                        &g_small_queue_tail,
                        &g_small_queue_depth,
                        src,
                        dst,
                        src_st);
}

int workers_enqueue_large_file(const char *src, const char *dst, const struct stat *src_st)
{
    return enqueue_task(&g_large_queue_head,
                        &g_large_queue_tail,
                        &g_large_queue_depth,
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
    printf("  large workers total         : %d\n", g_large_worker_count);
    printf("  active large file limit     : %d\n", g_max_active_large_files);
    printf("  large file readers/file     : %d\n", reader_count);
    printf("  large file writers/file     : %d\n", writer_count);
    printf("  large readers explicit      : %s\n", g_explicit_large_readers > 0 ? "yes" : "no");
    printf("  large file inflight chunks  : %d\n", g_large_file_inflight);
    printf("  chunk size MiB              : %d\n", (int)(g_chunk_size / (1024 * 1024)));
}

void workers_print_startup_config(void)
{
    int reader_count;
    int writer_count;

    init_runtime_config();
    get_pipeline_thread_counts(&reader_count, &writer_count);

    printf("Resolved config:\n");
    printf("  max workers                 : %d\n", g_worker_count);
    printf("  large workers total         : %d\n", g_large_worker_count);
    printf("  active large file limit     : %d\n", g_max_active_large_files);
    printf("  large file readers/file     : %d\n", reader_count);
    printf("  large file writers/file     : %d\n", writer_count);
    printf("  large readers explicit      : %s\n", g_explicit_large_readers > 0 ? "yes" : "no");
    printf("  large file inflight chunks  : %d\n", g_large_file_inflight);
    printf("  chunk size MiB              : %d\n", (int)(g_chunk_size / (1024 * 1024)));
    printf("  read direct_io enabled      : %s\n", read_direct_io_enabled() ? "yes" : "no");
    printf("  write direct_io enabled     : %s\n", write_direct_io_enabled() ? "yes" : "no");
    printf("  copy_file_range enabled     : %s\n", copy_file_range_enabled() ? "yes" : "no");
    fflush(stdout);
}
