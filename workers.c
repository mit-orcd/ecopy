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

/* -------------------- small-file queue -------------------- */

static pthread_t g_small_workers[SMALL_FILE_WORKERS];
static pthread_mutex_t g_small_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_small_queue_cond = PTHREAD_COND_INITIALIZER;
static file_task_t    *g_small_queue_head = NULL;
static file_task_t    *g_small_queue_tail = NULL;
static int             g_small_queue_done = 0;
static uint64_t        g_small_queue_depth = 0;
static uint64_t        g_small_workers_active = 0;

/* -------------------- large-file queue -------------------- */

static pthread_t g_large_dispatcher_thread;
static pthread_mutex_t g_large_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_large_queue_cond = PTHREAD_COND_INITIALIZER;
static file_task_t    *g_large_queue_head = NULL;
static file_task_t    *g_large_queue_tail = NULL;
static int             g_large_queue_done = 0;
static uint64_t        g_large_queue_depth = 0;
static uint64_t        g_large_workers_active = 0;

/* -------------------- generic queue helpers -------------------- */

static int enqueue_task(file_task_t **head,
                        file_task_t **tail,
                        pthread_mutex_t *lock,
                        pthread_cond_t *cond,
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

    pthread_mutex_lock(lock);
    if (*tail) {
        (*tail)->next = t;
    } else {
        *head = t;
    }
    *tail = t;
    (*depth)++;
    pthread_cond_signal(cond);
    pthread_mutex_unlock(lock);

    return 0;
}

static file_task_t *dequeue_task(file_task_t **head,
                                 file_task_t **tail,
                                 pthread_mutex_t *lock,
                                 pthread_cond_t *cond,
                                 int *done_flag,
                                 uint64_t *depth,
                                 uint64_t *active)
{
    pthread_mutex_lock(lock);

    while (!*head && !*done_flag) {
        pthread_cond_wait(cond, lock);
    }

    if (!*head && *done_flag) {
        pthread_mutex_unlock(lock);
        return NULL;
    }

    file_task_t *t = *head;
    *head = t->next;
    if (!*head) {
        *tail = NULL;
    }

    (*depth)--;
    (*active)++;
    pthread_mutex_unlock(lock);

    return t;
}

static void finish_task(pthread_mutex_t *lock, uint64_t *active)
{
    pthread_mutex_lock(lock);
    if (*active > 0) {
        (*active)--;
    }
    pthread_mutex_unlock(lock);
}

/* -------------------- copy helpers -------------------- */

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

        ssize_t r = read(fd_in, buf, len);
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
            ssize_t w = write(fd_out, buf + done, (size_t)r - done);
            if (w < 0) {
                perror("write tail");
                close(fd_in);
                close(fd_out);
                return -1;
            }
            done += (size_t)w;
        }

        pos += r;
        if (use_current_file_stats) {
            stats_advance_current_file((uint64_t)r);
        } else {
            stats_add_bytes((uint64_t)r);
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

    if (!out_direct) {
        if (ftruncate(fd_out, size) != 0) {
            perror("ftruncate");
        }
    }

    if (posix_memalign(&buf, ALIGNMENT, (size_t)CHUNK_SIZE) != 0) {
        fprintf(stderr, "posix_memalign failed\n");
        close(fd_in);
        close(fd_out);
        return -1;
    }

    while (pos < bulk_end) {
        off_t remain = bulk_end - pos;
        off_t this_len_off = (remain >= CHUNK_SIZE) ? CHUNK_SIZE : remain;
        size_t len = (size_t)this_len_off;

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
            ssize_t w = write(fd_out, (char *)buf + done, len - done);
            if (w < 0) {
                perror("write");
                free(buf);
                close(fd_in);
                close(fd_out);
                return -1;
            }
            done += (size_t)w;
        }

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

/* -------------------- large-file parallel copy -------------------- */

static int ctx_has_error(parallel_ctx_t *ctx)
{
    int err;
    pthread_mutex_lock(&ctx->error_lock);
    err = ctx->error;
    pthread_mutex_unlock(&ctx->error_lock);
    return err;
}

static void ctx_set_error(parallel_ctx_t *ctx)
{
    pthread_mutex_lock(&ctx->error_lock);
    ctx->error = 1;
    pthread_mutex_unlock(&ctx->error_lock);
}

static void chunk_worker_set_state(parallel_ctx_t *ctx, int id, int active)
{
    ctx->workers[id].active = active;
}

static void chunk_worker_add_done(parallel_ctx_t *ctx, int id, uint64_t bytes)
{
    ctx->workers[id].bytes_done += bytes;
    ctx->workers[id].chunks_done++;
}

static void *large_chunk_worker_main(void *arg)
{
    chunk_worker_arg_t *wa = (chunk_worker_arg_t *)arg;
    parallel_ctx_t *ctx = wa->ctx;
    int id = wa->worker_id;
    void *buf = NULL;

    if (posix_memalign(&buf, ALIGNMENT, (size_t)CHUNK_SIZE) != 0) {
        fprintf(stderr, "posix_memalign failed\n");
        ctx_set_error(ctx);
        return NULL;
    }

    while (!ctx_has_error(ctx)) {
        off_t off;
        off_t this_len_off;
        size_t len;

        pthread_mutex_lock(&ctx->offset_lock);
        off = ctx->next_offset;
        if (off >= ctx->bulk_end) {
            pthread_mutex_unlock(&ctx->offset_lock);
            break;
        }

        off_t remain = ctx->bulk_end - off;
        this_len_off = (remain >= CHUNK_SIZE) ? CHUNK_SIZE : remain;
        len = (size_t)this_len_off;
        ctx->next_offset += this_len_off;
        pthread_mutex_unlock(&ctx->offset_lock);

        chunk_worker_set_state(ctx, id, 1);

        ssize_t r = pread(ctx->fd_in, buf, len, off);
        if (r < 0) {
            perror("pread");
            ctx_set_error(ctx);
            chunk_worker_set_state(ctx, id, 0);
            break;
        }
        if ((size_t)r != len) {
            fprintf(stderr, "short pread at off %lld: expected %zu got %zd\n",
                    (long long)off, len, r);
            ctx_set_error(ctx);
            chunk_worker_set_state(ctx, id, 0);
            break;
        }

        size_t done = 0;
        while (done < len) {
            ssize_t w = pwrite(ctx->fd_out,
                               (char *)buf + done,
                               len - done,
                               off + (off_t)done);
            if (w < 0) {
                perror("pwrite");
                ctx_set_error(ctx);
                break;
            }
            if (w == 0) {
                fprintf(stderr, "zero pwrite at off %lld\n",
                        (long long)(off + (off_t)done));
                ctx_set_error(ctx);
                break;
            }
            done += (size_t)w;
        }

        if (ctx_has_error(ctx)) {
            chunk_worker_set_state(ctx, id, 0);
            break;
        }

        chunk_worker_add_done(ctx, id, (uint64_t)len);
        stats_advance_current_file((uint64_t)len);
        chunk_worker_set_state(ctx, id, 0);
    }

    free(buf);
    return NULL;
}

static int copy_file_parallel_large(const char *src, const char *dst, const struct stat *src_st)
{
    parallel_ctx_t ctx;
    chunk_worker_arg_t args[LARGE_FILE_WORKERS];
    pthread_t tids[LARGE_FILE_WORKERS];

    int fd_in = -1;
    int fd_out = -1;
    int in_direct = 0;
    int out_direct = 0;
    int rc = -1;

    memset(&ctx, 0, sizeof(ctx));
    ctx.file_size = src_st->st_size;
    ctx.bulk_end = (src_st->st_size / ALIGNMENT) * ALIGNMENT;
    ctx.next_offset = 0;
    ctx.worker_count = LARGE_FILE_WORKERS;
    ctx.workers = calloc((size_t)LARGE_FILE_WORKERS, sizeof(*ctx.workers));
    if (!ctx.workers) {
        perror("calloc");
        return -1;
    }

    pthread_mutex_init(&ctx.offset_lock, NULL);
    pthread_mutex_init(&ctx.error_lock, NULL);

    fd_in = open_read_maybe_direct(src, &in_direct);
    if (fd_in < 0) {
        goto out;
    }

    fd_out = open_write_maybe_direct(dst, src_st->st_mode & 07777, &out_direct);
    if (fd_out < 0) {
        goto out;
    }

    if (!out_direct) {
        if (ftruncate(fd_out, src_st->st_size) != 0) {
            perror("ftruncate");
        }
    }

    ctx.fd_in = fd_in;
    ctx.fd_out = fd_out;

    stats_begin_current_file(src, (uint64_t)src_st->st_size, 1);
    stats_set_active_parallel_ctx(&ctx);

    for (int i = 0; i < ctx.worker_count; i++) {
        args[i].ctx = &ctx;
        args[i].worker_id = i;
        if (pthread_create(&tids[i], NULL, large_chunk_worker_main, &args[i]) != 0) {
            perror("pthread_create");
            ctx_set_error(&ctx);
            ctx.worker_count = i;
            break;
        }
    }

    for (int i = 0; i < ctx.worker_count; i++) {
        pthread_join(tids[i], NULL);
    }

    if (ctx_has_error(&ctx)) {
        goto out_with_current;
    }

    close(fd_in);
    fd_in = -1;
    close(fd_out);
    fd_out = -1;

    if (copy_tail_buffered(src, dst, ctx.bulk_end, src_st->st_size, 1) != 0) {
        goto out_with_current;
    }

    if (finalize_copied_file(dst, src_st) != 0) {
        goto out_with_current;
    }

    stats_inc_files_copied();
    rc = 0;

out_with_current:
    stats_clear_active_parallel_ctx();
    stats_end_current_file();

out:
    if (fd_in >= 0) {
        close(fd_in);
    }
    if (fd_out >= 0) {
        close(fd_out);
    }

    pthread_mutex_destroy(&ctx.offset_lock);
    pthread_mutex_destroy(&ctx.error_lock);
    free(ctx.workers);
    return rc;
}

/* -------------------- worker threads -------------------- */

static void *small_file_worker_main(void *arg)
{
    (void)arg;

    for (;;) {
        file_task_t *t = dequeue_task(&g_small_queue_head,
                                      &g_small_queue_tail,
                                      &g_small_queue_lock,
                                      &g_small_queue_cond,
                                      &g_small_queue_done,
                                      &g_small_queue_depth,
                                      &g_small_workers_active);
        if (!t) {
            break;
        }

        if (copy_file_serial_small(t->src, t->dst, &t->src_st) == 0) {
            stats_inc_files_copied();
        }

        free(t);
        finish_task(&g_small_queue_lock, &g_small_workers_active);
    }

    return NULL;
}

static void *large_file_dispatcher_main(void *arg)
{
    (void)arg;

    for (;;) {
        file_task_t *t = dequeue_task(&g_large_queue_head,
                                      &g_large_queue_tail,
                                      &g_large_queue_lock,
                                      &g_large_queue_cond,
                                      &g_large_queue_done,
                                      &g_large_queue_depth,
                                      &g_large_workers_active);
        if (!t) {
            break;
        }

        (void)copy_file_parallel_large(t->src, t->dst, &t->src_st);

        free(t);
        finish_task(&g_large_queue_lock, &g_large_workers_active);
    }

    return NULL;
}

/* -------------------- public API -------------------- */

int workers_start(void)
{
    pthread_mutex_lock(&g_small_queue_lock);
    g_small_queue_done = 0;
    g_small_queue_head = NULL;
    g_small_queue_tail = NULL;
    g_small_queue_depth = 0;
    g_small_workers_active = 0;
    pthread_mutex_unlock(&g_small_queue_lock);

    pthread_mutex_lock(&g_large_queue_lock);
    g_large_queue_done = 0;
    g_large_queue_head = NULL;
    g_large_queue_tail = NULL;
    g_large_queue_depth = 0;
    g_large_workers_active = 0;
    pthread_mutex_unlock(&g_large_queue_lock);

    for (int i = 0; i < SMALL_FILE_WORKERS; i++) {
        if (pthread_create(&g_small_workers[i], NULL, small_file_worker_main, NULL) != 0) {
            perror("pthread_create");
            return -1;
        }
    }

    if (pthread_create(&g_large_dispatcher_thread, NULL, large_file_dispatcher_main, NULL) != 0) {
        perror("pthread_create");
        return -1;
    }

    return 0;
}

void workers_stop(void)
{
    pthread_mutex_lock(&g_small_queue_lock);
    g_small_queue_done = 1;
    pthread_cond_broadcast(&g_small_queue_cond);
    pthread_mutex_unlock(&g_small_queue_lock);

    for (int i = 0; i < SMALL_FILE_WORKERS; i++) {
        pthread_join(g_small_workers[i], NULL);
    }

    pthread_mutex_lock(&g_large_queue_lock);
    g_large_queue_done = 1;
    pthread_cond_broadcast(&g_large_queue_cond);
    pthread_mutex_unlock(&g_large_queue_lock);

    pthread_join(g_large_dispatcher_thread, NULL);
}

int workers_enqueue_small_file(const char *src, const char *dst, const struct stat *src_st)
{
    return enqueue_task(&g_small_queue_head,
                        &g_small_queue_tail,
                        &g_small_queue_lock,
                        &g_small_queue_cond,
                        &g_small_queue_depth,
                        src,
                        dst,
                        src_st);
}

int workers_enqueue_large_file(const char *src, const char *dst, const struct stat *src_st)
{
    return enqueue_task(&g_large_queue_head,
                        &g_large_queue_tail,
                        &g_large_queue_lock,
                        &g_large_queue_cond,
                        &g_large_queue_depth,
                        src,
                        dst,
                        src_st);
}

uint64_t workers_small_queue_depth(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_small_queue_lock);
    v = g_small_queue_depth;
    pthread_mutex_unlock(&g_small_queue_lock);
    return v;
}

uint64_t workers_small_active_count(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_small_queue_lock);
    v = g_small_workers_active;
    pthread_mutex_unlock(&g_small_queue_lock);
    return v;
}

uint64_t workers_large_queue_depth(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_large_queue_lock);
    v = g_large_queue_depth;
    pthread_mutex_unlock(&g_large_queue_lock);
    return v;
}

uint64_t workers_large_active_count(void)
{
    uint64_t v;
    pthread_mutex_lock(&g_large_queue_lock);
    v = g_large_workers_active;
    pthread_mutex_unlock(&g_large_queue_lock);
    return v;
}