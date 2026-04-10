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
 */

typedef enum {
    WORK_NONE = 0,
    WORK_SMALL_FILE,
    WORK_LARGE_FILE_START,
    WORK_LARGE_CHUNK
} work_kind_t;

typedef struct large_file_ctx {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    struct stat src_st;
    off_t bulk_end;
    off_t next_offset;
    int fd_in;
    int fd_out;
    int in_direct;
    int out_direct;
    int copy_range_enabled;
    int inflight_chunks;
    int failed;
    struct large_file_ctx *next;
} large_file_ctx_t;

typedef struct chunk_task {
    large_file_ctx_t *ctx;
    off_t start;
    off_t end;
    struct chunk_task *next;
} chunk_task_t;

typedef struct {
    work_kind_t kind;
    file_task_t *file_task;
    chunk_task_t *chunk_task;
} work_claim_t;

static __thread void *g_large_chunk_buf = NULL;
static __thread size_t g_large_chunk_buf_sz = 0;

static pthread_t *g_workers = NULL;
static int g_worker_count = 0;
static int g_large_worker_count = 0;
static int g_large_file_inflight = 0;
static int g_max_active_large_files = 0;
static off_t g_chunk_size = 0;

/* -------------------- scheduler state -------------------- */

static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_queue_cond = PTHREAD_COND_INITIALIZER;
static file_task_t    *g_small_queue_head = NULL;
static file_task_t    *g_small_queue_tail = NULL;
static file_task_t    *g_large_queue_head = NULL;
static file_task_t    *g_large_queue_tail = NULL;
static chunk_task_t   *g_chunk_queue_head = NULL;
static chunk_task_t   *g_chunk_queue_tail = NULL;
static int             g_queue_done = 0;
static uint64_t        g_small_queue_depth = 0;
static uint64_t        g_large_queue_depth = 0;
static uint64_t        g_chunk_queue_depth = 0;
static uint64_t        g_small_workers_active = 0;
static uint64_t        g_large_workers_active = 0;

static int g_workers_error = 0;
static pthread_mutex_t g_workers_error_lock = PTHREAD_MUTEX_INITIALIZER;

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
                                              1,
                                              g_worker_count);

    g_large_file_inflight = env_int_or_default("DIRECT_COPY_LARGE_FILE_INFLIGHT",
                                               g_large_worker_count,
                                               1,
                                               1024);

    int chunk_mb = env_int_or_default("DIRECT_COPY_CHUNK_MB",
                                      (int)(CHUNK_SIZE / (1024 * 1024)),
                                      1,
                                      4096);

    g_chunk_size = (off_t)chunk_mb * 1024 * 1024;
    g_max_active_large_files = g_worker_count / g_large_worker_count;
    if (g_max_active_large_files < 1) {
        g_max_active_large_files = 1;
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

static chunk_task_t *pop_chunk_task_locked(void)
{
    chunk_task_t *t = g_chunk_queue_head;
    g_chunk_queue_head = t->next;
    if (!g_chunk_queue_head) {
        g_chunk_queue_tail = NULL;
    }
    t->next = NULL;
    if (g_chunk_queue_depth > 0) {
        g_chunk_queue_depth--;
    }
    return t;
}

static void push_chunk_task_locked(chunk_task_t *task)
{
    task->next = NULL;
    if (g_chunk_queue_tail) {
        g_chunk_queue_tail->next = task;
    } else {
        g_chunk_queue_head = task;
    }
    g_chunk_queue_tail = task;
    g_chunk_queue_depth++;
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

    return 0;
}

static int open_write_existing_maybe_direct(const char *path, int *used_direct)
{
    int fd;

    if (used_direct) {
        *used_direct = 0;
    }

    if (direct_io_enabled()) {
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

    int fd_out = open(dst, O_WRONLY);
    if (fd_out < 0) {
        perror(dst);
        close(fd_in);
        return -1;
    }

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

        pos += r;
        record_progress_bytes((uint64_t)r, use_current_file_stats);
        advise_source_consumed(fd_in, chunk_start, r);
    }

    close(fd_in);
    close(fd_out);
    return 0;
}

static int copy_file_serial_small(const char *src, const char *dst, const struct stat *src_st)
{
    init_runtime_config();

    int fd_in = -1;
    int fd_out = -1;
    int in_direct = 0;
    int out_direct = 0;
    void *buf = NULL;
    int copy_range_available = copy_file_range_enabled();

    off_t size = src_st->st_size;
    off_t bulk_end = (size / ALIGNMENT) * ALIGNMENT;
    off_t pos = 0;

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

        ssize_t r = read(fd_in, buf, len);
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

        advise_source_consumed(fd_in, pos, r);
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

/* -------------------- large-file chunk scheduler -------------------- */

static int schedule_more_chunks_locked(large_file_ctx_t *ctx)
{
    while (!ctx->failed &&
           ctx->inflight_chunks < g_large_file_inflight &&
           ctx->next_offset < ctx->bulk_end) {
        off_t start = ctx->next_offset;
        off_t remain = ctx->bulk_end - start;
        off_t span = (remain >= g_chunk_size) ? g_chunk_size : remain;
        off_t end = start + span;

        chunk_task_t *task = calloc(1, sizeof(*task));
        if (!task) {
            perror("calloc");
            ctx->failed = 1;
            return -1;
        }

        task->ctx = ctx;
        task->start = start;
        task->end = end;
        push_chunk_task_locked(task);
        ctx->next_offset = end;
        ctx->inflight_chunks++;
    }

    return 0;
}

static void cleanup_large_file_ctx(large_file_ctx_t *ctx)
{
    if (!ctx) {
        return;
    }

    if (ctx->fd_in >= 0) {
        close(ctx->fd_in);
    }
    if (ctx->fd_out >= 0) {
        close(ctx->fd_out);
    }
    free(ctx);
}

static int finalize_large_file_ctx(large_file_ctx_t *ctx)
{
    int rc = 0;

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

    cleanup_large_file_ctx(ctx);
    return rc;
}

static int start_large_file_copy(file_task_t *task)
{
    large_file_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        perror("calloc");
        return -1;
    }

    snprintf(ctx->src, sizeof(ctx->src), "%s", task->src);
    snprintf(ctx->dst, sizeof(ctx->dst), "%s", task->dst);
    ctx->src_st = task->src_st;
    ctx->bulk_end = (task->src_st.st_size / ALIGNMENT) * ALIGNMENT;
    ctx->next_offset = 0;
    ctx->fd_in = -1;
    ctx->fd_out = -1;
    ctx->copy_range_enabled = copy_file_range_enabled();

    if (prepare_destination_file(ctx->dst, &ctx->src_st) != 0) {
        cleanup_large_file_ctx(ctx);
        return -1;
    }

    ctx->fd_in = open_read_maybe_direct(ctx->src, &ctx->in_direct);
    if (ctx->fd_in < 0) {
        cleanup_large_file_ctx(ctx);
        return -1;
    }

    ctx->fd_out = open_write_existing_maybe_direct(ctx->dst, &ctx->out_direct);
    if (ctx->fd_out < 0) {
        cleanup_large_file_ctx(ctx);
        return -1;
    }

    if (!ctx->in_direct) {
        advise_source_streaming(ctx->fd_in);
    }
    if (!ctx->out_direct) {
        advise_dest_streaming(ctx->fd_out);
    }

    pthread_mutex_lock(&g_queue_lock);
    if (schedule_more_chunks_locked(ctx) != 0) {
        pthread_mutex_unlock(&g_queue_lock);
        cleanup_large_file_ctx(ctx);
        return -1;
    }
    pthread_cond_broadcast(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);

    return 0;
}

static int copy_large_chunk(chunk_task_t *task)
{
    large_file_ctx_t *ctx = task->ctx;
    void *buf = NULL;
    int rc = 0;

    if (!ctx) {
        free(task);
        return -1;
    }

    pthread_mutex_lock(&g_queue_lock);
    int skip = ctx->failed;
    pthread_mutex_unlock(&g_queue_lock);

    if (!skip) {
        if (!g_large_chunk_buf || g_large_chunk_buf_sz < (size_t)g_chunk_size) {
            free(g_large_chunk_buf);
            g_large_chunk_buf = NULL;
            g_large_chunk_buf_sz = 0;
            if (posix_memalign(&g_large_chunk_buf, ALIGNMENT, (size_t)g_chunk_size) != 0) {
                fprintf(stderr, "posix_memalign failed\n");
                rc = -1;
                skip = 1;
            } else {
                g_large_chunk_buf_sz = (size_t)g_chunk_size;
                stats_record_large_chunk_buffer_alloc();
            }
        }
        buf = g_large_chunk_buf;
    }

    if (!skip) {
        off_t pos = task->start;
        int copy_range_available = ctx->copy_range_enabled && !ctx->in_direct && !ctx->out_direct;

        while (pos < task->end) {
            off_t remain = task->end - pos;
            off_t this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
            size_t len = (size_t)this_len_off;

            if (copy_range_available) {
                off_t in_off = pos;
                off_t out_off = pos;
                int cfr_rc = copy_file_range_with_progress(ctx->fd_in, ctx->fd_out,
                                                           &in_off, &out_off,
                                                           len, 0);
                if (cfr_rc == 0) {
                    pos += this_len_off;
                    continue;
                }
                if (cfr_rc < 0) {
                    rc = -1;
                    break;
                }

                pthread_mutex_lock(&g_queue_lock);
                ctx->copy_range_enabled = 0;
                pthread_mutex_unlock(&g_queue_lock);
                stats_record_copy_file_range_fallback();
                copy_range_available = 0;
            }

            uint64_t read_start_ns = monotonic_ns();
            ssize_t r = pread(ctx->fd_in, buf, len, pos);
            stats_record_read_io(monotonic_ns() - read_start_ns);
            if (r < 0) {
                perror("pread");
                rc = -1;
                break;
            }
            if ((size_t)r != len) {
                fprintf(stderr, "short pread at off %lld: expected %zu got %zd\n",
                        (long long)pos, len, r);
                rc = -1;
                break;
            }

            size_t done = 0;
            while (done < len) {
                uint64_t write_start_ns = monotonic_ns();
                ssize_t w = pwrite(ctx->fd_out,
                                   (char *)buf + done,
                                   len - done,
                                   pos + (off_t)done);
                stats_record_write_io(monotonic_ns() - write_start_ns);
                if (w < 0) {
                    perror("pwrite");
                    rc = -1;
                    break;
                }
                if (w == 0) {
                    fprintf(stderr, "zero pwrite at off %lld\n",
                            (long long)(pos + (off_t)done));
                    rc = -1;
                    break;
                }
                done += (size_t)w;
            }
            if (rc != 0) {
                break;
            }

            advise_source_consumed(ctx->fd_in, pos, len);
            pos += this_len_off;
            stats_add_bytes((uint64_t)len);
        }
    }

    int finalize_now = 0;
    pthread_mutex_lock(&g_queue_lock);
    if (rc != 0) {
        ctx->failed = 1;
    }
    if (ctx->inflight_chunks > 0) {
        ctx->inflight_chunks--;
    }
    if (!ctx->failed) {
        (void)schedule_more_chunks_locked(ctx);
    }
    if ((ctx->failed || ctx->next_offset >= ctx->bulk_end) && ctx->inflight_chunks == 0) {
        if (g_large_workers_active > 0) {
            g_large_workers_active--;
        }
        finalize_now = 1;
    }
    pthread_cond_broadcast(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);

    free(task);

    if (finalize_now) {
        if (finalize_large_file_ctx(ctx) != 0) {
            return -1;
        }
    }

    return rc;
}

/* -------------------- scheduler -------------------- */

static work_claim_t dequeue_work(void)
{
    work_claim_t claim;
    memset(&claim, 0, sizeof(claim));

    pthread_mutex_lock(&g_queue_lock);

    for (;;) {
        if (g_chunk_queue_head) {
            claim.kind = WORK_LARGE_CHUNK;
            claim.chunk_task = pop_chunk_task_locked();
            break;
        }

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

        if (!g_small_queue_head && !g_large_queue_head && !g_chunk_queue_head &&
            g_queue_done && g_large_workers_active == 0 && g_small_workers_active == 0) {
            break;
        }

        uint64_t wait_start_ns = monotonic_ns();
        pthread_cond_wait(&g_queue_cond, &g_queue_lock);
        stats_record_queue_wait_ns(monotonic_ns() - wait_start_ns);
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
                pthread_mutex_unlock(&g_queue_lock);
            }
            free(claim.file_task);
            continue;
        }

        if (claim.kind == WORK_LARGE_CHUNK) {
            if (copy_large_chunk(claim.chunk_task) != 0) {
                mark_worker_error();
            }
            continue;
        }
    }

    free(g_large_chunk_buf);
    g_large_chunk_buf = NULL;
    g_large_chunk_buf_sz = 0;
    return NULL;
}

/* -------------------- public API -------------------- */

int workers_start(void)
{
    init_runtime_config();
    clear_worker_error();

    pthread_mutex_lock(&g_queue_lock);
    g_queue_done = 0;
    g_small_queue_head = NULL;
    g_small_queue_tail = NULL;
    g_large_queue_head = NULL;
    g_large_queue_tail = NULL;
    g_chunk_queue_head = NULL;
    g_chunk_queue_tail = NULL;
    g_small_queue_depth = 0;
    g_large_queue_depth = 0;
    g_chunk_queue_depth = 0;
    g_small_workers_active = 0;
    g_large_workers_active = 0;
    pthread_mutex_unlock(&g_queue_lock);

    g_workers = calloc((size_t)g_worker_count, sizeof(*g_workers));
    if (!g_workers) {
        perror("calloc");
        return -1;
    }

    for (int i = 0; i < g_worker_count; i++) {
        if (pthread_create(&g_workers[i], NULL, worker_main, NULL) != 0) {
            perror("pthread_create");
            return -1;
        }
    }

    return 0;
}

void workers_stop(void)
{
    pthread_mutex_lock(&g_queue_lock);
    g_queue_done = 1;
    pthread_cond_broadcast(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);

    for (int i = 0; i < g_worker_count; i++) {
        pthread_join(g_workers[i], NULL);
    }

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
    init_runtime_config();
    printf("Options used:\n");
    printf("  direct_io enabled           : %s\n", direct_io_enabled() ? "yes" : "no");
    printf("  copy_file_range enabled     : %s\n", copy_file_range_enabled() ? "yes" : "no");
    printf("  max workers                 : %d\n", g_worker_count);
    printf("  large workers divisor       : %d\n", g_large_worker_count);
    printf("  active large file limit     : %d\n", g_max_active_large_files);
    printf("  large file inflight chunks  : %d\n", g_large_file_inflight);
    printf("  chunk size MiB              : %d\n", (int)(g_chunk_size / (1024 * 1024)));
}
