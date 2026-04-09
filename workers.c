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
#include <sys/wait.h>
#include <sys/mman.h>
#include <signal.h>
#include <limits.h>

/*
 * Runtime overrides:
 *
 *   DIRECT_COPY_MAX_WORKERS
 *   DIRECT_COPY_LARGE_WORKERS
 *   DIRECT_COPY_CHUNK_MB
 */

typedef struct {
    file_task_t *task;
    int is_large;
    int slots_used;
} task_claim_t;

static pthread_t *g_workers = NULL;
static int g_worker_count = 0;
static int g_large_worker_count = 0;
static off_t g_chunk_size = 0;

/* -------------------- scheduler state -------------------- */

static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_queue_cond = PTHREAD_COND_INITIALIZER;
static file_task_t    *g_small_queue_head = NULL;
static file_task_t    *g_small_queue_tail = NULL;
static file_task_t    *g_large_queue_head = NULL;
static file_task_t    *g_large_queue_tail = NULL;
static int             g_queue_done = 0;
static uint64_t        g_small_queue_depth = 0;
static uint64_t        g_large_queue_depth = 0;
static uint64_t        g_small_workers_active = 0;
static uint64_t        g_large_workers_active = 0;
static int             g_capacity_in_use = 0;

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
    if (g_worker_count > 0 && g_large_worker_count > 0 && g_chunk_size > 0) {
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

    int chunk_mb = env_int_or_default("DIRECT_COPY_CHUNK_MB",
                                      (int)(CHUNK_SIZE / (1024 * 1024)),
                                      1,
                                      4096);

    g_chunk_size = (off_t)chunk_mb * 1024 * 1024;
}

static off_t runtime_large_threshold(void)
{
    init_runtime_config();
    return g_chunk_size * 10;
}

/* -------------------- scheduler helpers -------------------- */

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

static file_task_t *pop_task(file_task_t **head, file_task_t **tail)
{
    file_task_t *t = *head;
    *head = t->next;
    if (!*head) {
        *tail = NULL;
    }
    t->next = NULL;
    return t;
}

static task_claim_t dequeue_schedulable_task(void)
{
    task_claim_t claim;
    memset(&claim, 0, sizeof(claim));

    pthread_mutex_lock(&g_queue_lock);

    for (;;) {
        if (g_large_queue_head && g_capacity_in_use + g_large_worker_count <= g_worker_count) {
            claim.task = pop_task(&g_large_queue_head, &g_large_queue_tail);
            g_large_queue_depth--;
            g_large_workers_active++;
            g_capacity_in_use += g_large_worker_count;
            claim.is_large = 1;
            claim.slots_used = g_large_worker_count;
            break;
        }

        if (g_small_queue_head && g_capacity_in_use < g_worker_count) {
            claim.task = pop_task(&g_small_queue_head, &g_small_queue_tail);
            g_small_queue_depth--;
            g_small_workers_active++;
            g_capacity_in_use += 1;
            claim.is_large = 0;
            claim.slots_used = 1;
            break;
        }

        if (!g_small_queue_head && !g_large_queue_head && g_queue_done) {
            break;
        }

        pthread_cond_wait(&g_queue_cond, &g_queue_lock);
    }

    pthread_mutex_unlock(&g_queue_lock);
    return claim;
}

static void finish_task(int is_large, int slots_used)
{
    pthread_mutex_lock(&g_queue_lock);

    g_capacity_in_use -= slots_used;
    if (g_capacity_in_use < 0) {
        g_capacity_in_use = 0;
    }

    if (is_large) {
        if (g_large_workers_active > 0) {
            g_large_workers_active--;
        }
    } else {
        if (g_small_workers_active > 0) {
            g_small_workers_active--;
        }
    }

    pthread_cond_broadcast(&g_queue_cond);
    pthread_mutex_unlock(&g_queue_lock);
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

static int direct_io_fallback_errno_local(int err)
{
    return err == EINVAL ||
           err == EOPNOTSUPP ||
           err == ENOTSUP ||
           err == ENOSYS;
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
    init_runtime_config();

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

    if (ftruncate(fd_out, size) != 0) {
        perror("ftruncate");
        close(fd_in);
        close(fd_out);
        return -1;
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

/* -------------------- large-file forked copy -------------------- */

static uint64_t sum_worker_bytes(chunk_worker_stat_t *workers, int count)
{
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        total += workers[i].bytes_done;
    }
    return total;
}

static void kill_remaining_children(pid_t *pids, int count, int *done)
{
    for (int i = 0; i < count; i++) {
        if (!done[i] && pids[i] > 0) {
            kill(pids[i], SIGTERM);
        }
    }
}

static int child_copy_range(const char *src,
                            const char *dst,
                            off_t start,
                            off_t end,
                            chunk_worker_stat_t *ws)
{
    int fd_in = -1;
    int fd_out = -1;
    int in_direct = 0;
    int out_direct = 0;
    void *buf = NULL;
    int rc = 1;

    fd_in = open_read_maybe_direct(src, &in_direct);
    if (fd_in < 0) {
        return 1;
    }

    fd_out = open_write_existing_maybe_direct(dst, &out_direct);
    if (fd_out < 0) {
        close(fd_in);
        return 1;
    }

    if (posix_memalign(&buf, ALIGNMENT, (size_t)g_chunk_size) != 0) {
        fprintf(stderr, "posix_memalign failed\n");
        close(fd_in);
        close(fd_out);
        return 1;
    }

    ws->active = 1;
    ws->bytes_done = 0;
    ws->chunks_done = 0;

    off_t pos = start;

    while (pos < end) {
        off_t remain = end - pos;
        off_t this_len_off = (remain >= g_chunk_size) ? g_chunk_size : remain;
        size_t len = (size_t)this_len_off;

        ssize_t r = pread(fd_in, buf, len, pos);
        if (r < 0) {
            perror("pread");
            goto out;
        }
        if ((size_t)r != len) {
            fprintf(stderr, "short pread at off %lld: expected %zu got %zd\n",
                    (long long)pos, len, r);
            goto out;
        }

        size_t done = 0;
        while (done < len) {
            ssize_t w = pwrite(fd_out,
                               (char *)buf + done,
                               len - done,
                               pos + (off_t)done);
            if (w < 0) {
                perror("pwrite");
                goto out;
            }
            if (w == 0) {
                fprintf(stderr, "zero pwrite at off %lld\n",
                        (long long)(pos + (off_t)done));
                goto out;
            }
            done += (size_t)w;
        }

        pos += this_len_off;
        ws->bytes_done += (uint64_t)len;
        ws->chunks_done++;
    }

    rc = 0;

out:
    ws->active = 0;

    free(buf);
    if (fd_in >= 0) close(fd_in);
    if (fd_out >= 0) close(fd_out);
    return rc;
}

static int copy_file_parallel_large(const char *src, const char *dst, const struct stat *src_st)
{
    init_runtime_config();

    parallel_ctx_t ctx;
    pid_t *pids = NULL;
    int *done = NULL;
    int rc = -1;
    uint64_t accounted = 0;
    int failed = 0;

    memset(&ctx, 0, sizeof(ctx));
    ctx.file_size = src_st->st_size;
    ctx.bulk_end = (src_st->st_size / ALIGNMENT) * ALIGNMENT;
    ctx.worker_count = g_large_worker_count;

    ctx.workers = mmap(NULL,
                       (size_t)ctx.worker_count * sizeof(*ctx.workers),
                       PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_ANONYMOUS,
                       -1,
                       0);
    if (ctx.workers == MAP_FAILED) {
        perror("mmap");
        ctx.workers = NULL;
        return -1;
    }
    memset(ctx.workers, 0, (size_t)ctx.worker_count * sizeof(*ctx.workers));

    pids = calloc((size_t)ctx.worker_count, sizeof(*pids));
    done = calloc((size_t)ctx.worker_count, sizeof(*done));
    if (!pids || !done) {
        perror("calloc");
        goto out;
    }

    if (prepare_destination_file(dst, src_st) != 0) {
        goto out;
    }

    off_t base = 0;
    off_t per = (ctx.worker_count > 0) ? (ctx.bulk_end / ctx.worker_count) : ctx.bulk_end;
    per = (per / ALIGNMENT) * ALIGNMENT;

    for (int i = 0; i < ctx.worker_count; i++) {
        off_t start = base;
        off_t end;

        if (i == ctx.worker_count - 1) {
            end = ctx.bulk_end;
        } else {
            end = start + per;
        }
        base = end;

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            failed = 1;
            kill_remaining_children(pids, i, done);
            break;
        }

        if (pid == 0) {
            int child_rc = child_copy_range(src, dst, start, end, &ctx.workers[i]);
            _exit(child_rc == 0 ? 0 : 1);
        }

        pids[i] = pid;
    }

    while (1) {
        int completed = 0;

        for (int i = 0; i < ctx.worker_count; i++) {
            if (done[i] || pids[i] <= 0) {
                if (done[i]) {
                    completed++;
                }
                continue;
            }

            int status = 0;
            pid_t r = waitpid(pids[i], &status, WNOHANG);
            if (r == 0) {
                /* still running */
            } else if (r < 0) {
                perror("waitpid");
                failed = 1;
                done[i] = 1;
                completed++;
            } else {
                done[i] = 1;
                completed++;
                if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                    failed = 1;
                }
            }
        }

        uint64_t total_done = sum_worker_bytes(ctx.workers, ctx.worker_count);
        if (total_done > accounted) {
            stats_add_bytes(total_done - accounted);
            accounted = total_done;
        }

        if (failed) {
            kill_remaining_children(pids, ctx.worker_count, done);

            for (int i = 0; i < ctx.worker_count; i++) {
                if (!done[i] && pids[i] > 0) {
                    int status = 0;
                    (void)waitpid(pids[i], &status, 0);
                    done[i] = 1;
                }
            }
            break;
        }

        if (completed == ctx.worker_count) {
            break;
        }

        usleep(MONITOR_INTERVAL_MS * 1000);
    }

    {
        uint64_t total_done = sum_worker_bytes(ctx.workers, ctx.worker_count);
        if (total_done > accounted) {
            stats_add_bytes(total_done - accounted);
            accounted = total_done;
        }
    }

    if (failed) {
        goto out;
    }

    if (copy_tail_buffered(src, dst, ctx.bulk_end, src_st->st_size, 0) != 0) {
        goto out;
    }

    if (finalize_copied_file(dst, src_st) != 0) {
        goto out;
    }

    stats_inc_files_copied();
    rc = 0;

out:
    if (ctx.workers) {
        munmap(ctx.workers, (size_t)ctx.worker_count * sizeof(*ctx.workers));
    }
    free(pids);
    free(done);
    return rc;
}

/* -------------------- worker threads -------------------- */

static void *worker_main(void *arg)
{
    (void)arg;

    for (;;) {
        task_claim_t claim = dequeue_schedulable_task();
        if (!claim.task) {
            break;
        }

        if (claim.is_large) {
            if (copy_file_parallel_large(claim.task->src, claim.task->dst, &claim.task->src_st) != 0) {
                mark_worker_error();
            }
        } else {
            if (copy_file_serial_small(claim.task->src, claim.task->dst, &claim.task->src_st) == 0) {
                stats_inc_files_copied();
            } else {
                mark_worker_error();
            }
        }

        free(claim.task);
        finish_task(claim.is_large, claim.slots_used);
    }

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
    g_small_queue_depth = 0;
    g_large_queue_depth = 0;
    g_small_workers_active = 0;
    g_large_workers_active = 0;
    g_capacity_in_use = 0;
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
