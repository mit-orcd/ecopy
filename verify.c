/*
 * Transfer verification implementation.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */
#define _GNU_SOURCE
#include "verify.h"

#include "copy_policy.h"
#include "fs_util.h"
#include "progress.h"
#include "ssh_transport.h"
#include "stats.h"
#include "third_party/blake3/blake3.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/random.h>
#endif

typedef struct verify_item {
    char *src;
    char *dst;
    struct stat src_st;
    int skipped;
    struct verify_item *next;
} verify_item_t;

typedef struct {
    int metadata;
    int data;
    int include_skipped;
    double percent;
    uint64_t seed;
} verify_config_t;

static verify_config_t g_cfg;
static verify_item_t *g_head;
static verify_item_t *g_tail;
static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;

static uint64_t mix64(uint64_t x)
{
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}

static uint64_t path_hash(const char *s)
{
    uint64_t h = UINT64_C(1469598103934665603);
    while (s && *s) {
        h ^= (unsigned char)*s++;
        h *= UINT64_C(1099511628211);
    }
    return h;
}

static uint64_t random_seed(void)
{
    uint64_t seed = 0;
#ifdef __linux__
    if (getrandom(&seed, sizeof(seed), 0) == (ssize_t)sizeof(seed)) {
        return seed;
    }
#endif
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return mix64((uint64_t)ts.tv_sec ^ ((uint64_t)ts.tv_nsec << 32) ^
                 (uint64_t)getpid());
}

void verify_configure(int metadata, int data, double percent,
                      int include_skipped, uint64_t seed, int seed_set)
{
    g_cfg.metadata = metadata ? 1 : 0;
    g_cfg.data = data ? 1 : 0;
    g_cfg.include_skipped = include_skipped ? 1 : 0;
    if (percent < 0.0) percent = 0.0;
    if (percent > 100.0) percent = 100.0;
    g_cfg.percent = percent;
    g_cfg.seed = seed_set ? seed : random_seed();
    stats_set_verify_config(g_cfg.metadata, g_cfg.data, g_cfg.percent, g_cfg.seed);
}

int verify_enabled(void) { return g_cfg.metadata || g_cfg.data; }
int verify_metadata_enabled(void) { return g_cfg.metadata; }
int verify_data_enabled(void) { return g_cfg.data; }
int verify_include_skipped(void) { return g_cfg.include_skipped; }
double verify_percent(void) { return g_cfg.percent; }
uint64_t verify_seed(void) { return g_cfg.seed; }

int verify_queue_file(const char *src, const char *dst,
                      const struct stat *src_st, int skipped)
{
    verify_item_t *item;
    if (!verify_enabled() || (skipped && !g_cfg.include_skipped)) return 0;
    item = calloc(1, sizeof(*item));
    if (!item) return -1;
    item->src = strdup(src);
    item->dst = strdup(dst);
    if (!item->src || !item->dst) {
        free(item->src);
        free(item->dst);
        free(item);
        return -1;
    }
    item->src_st = *src_st;
    item->skipped = skipped;

    pthread_mutex_lock(&g_queue_lock);
    if (g_tail) g_tail->next = item;
    else g_head = item;
    g_tail = item;
    pthread_mutex_unlock(&g_queue_lock);
    return 0;
}

int verify_retarget_path(const char *old_dst, const char *new_dst)
{
    int rc = 0;
    pthread_mutex_lock(&g_queue_lock);
    for (verify_item_t *item = g_head; item; item = item->next) {
        if (strcmp(item->dst, old_dst) == 0) {
            char *replacement = strdup(new_dst);
            if (!replacement) {
                rc = -1;
                break;
            }
            free(item->dst);
            item->dst = replacement;
        }
    }
    pthread_mutex_unlock(&g_queue_lock);
    return rc;
}

static int timestamp_equal(const struct timespec *a, const struct timespec *b)
{
    return a->tv_sec == b->tv_sec && a->tv_nsec == b->tv_nsec;
}

int verify_metadata_stat(const struct stat *expected,
                         const struct stat *actual,
                         int is_dir,
                         const char *path)
{
    const char *field = NULL;
    if ((actual->st_mode & S_IFMT) != (expected->st_mode & S_IFMT)) field = "type";
    else if (!is_dir && actual->st_size != expected->st_size) field = "size";
    else if ((actual->st_mode & 07777) != (expected->st_mode & 07777)) field = "mode";
    else if (actual->st_uid != expected->st_uid) field = "uid";
    else if (actual->st_gid != expected->st_gid) field = "gid";
    else if (copy_policy_preserve_times() &&
             !timestamp_equal(&actual->st_atim, &expected->st_atim)) field = "atime";
    else if (copy_policy_preserve_times() &&
             !timestamp_equal(&actual->st_mtim, &expected->st_mtim)) field = "mtime";

    if (!field) return 0;
    progress_interrupt();
    fprintf(stderr, "ecopy: verification metadata mismatch: %s (%s)\n",
            path, field);
    return -1;
}

int verify_metadata_path(const char *path, const struct stat *expected,
                         int is_dir)
{
    struct stat actual;
    if (!g_cfg.metadata) return 0;
    if (lstat(path, &actual) != 0) {
        progress_interrupt();
        perror(path);
        stats_record_verify(0, 0, 0, 1, 0, 0, 1);
        return -1;
    }
    int rc = verify_metadata_stat(expected, &actual, is_dir, path);
    stats_record_verify(0, 0, 0, 1, 0, rc != 0, rc != 0);
    return rc;
}

static ssize_t pread_full(int fd, void *buf, size_t len, off_t off)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = pread(fd, (char *)buf + done, len - done, off + (off_t)done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

typedef int (*sample_cb)(uint64_t index, off_t offset, size_t length, void *arg);

static int for_each_sample(const verify_item_t *item, sample_cb cb, void *arg)
{
    uint64_t n, target, emitted = 0;
    uint64_t seed, a, b, domain = 1;
    off_t size = item->src_st.st_size;
    if (size <= 0 || !g_cfg.data) return 0;
    n = ((uint64_t)size + VERIFY_BLOCK_SIZE - 1) / VERIFY_BLOCK_SIZE;
    target = (uint64_t)((g_cfg.percent * (double)n + 99.999999) / 100.0);
    if (target < (n == 1 ? 1 : 2)) target = n == 1 ? 1 : 2;
    if (target > n) target = n;

    if (cb(0, 0, (size_t)(size < VERIFY_BLOCK_SIZE ? size : VERIFY_BLOCK_SIZE), arg) != 0) {
        return -1;
    }
    emitted++;
    if (n > 1) {
        off_t off = (off_t)((n - 1) * VERIFY_BLOCK_SIZE);
        size_t len = (size_t)(size - off);
        if (cb(n - 1, off, len, arg) != 0) return -1;
        emitted++;
    }
    if (emitted >= target || n <= 2) return 0;

    seed = mix64(g_cfg.seed ^ path_hash(item->src) ^ (uint64_t)size);
    while (domain < n) domain <<= 1;
    a = mix64(seed) | 1u; /* odd multiplication permutes a power-of-two domain */
    b = mix64(seed ^ UINT64_C(0x9e3779b97f4a7c15)) & (domain - 1);
    for (uint64_t j = 0; emitted < target && j < domain; j++) {
        uint64_t idx = (a * j + b) & (domain - 1);
        if (idx >= n) continue;
        if (idx == 0 || idx == n - 1) continue;
        off_t off = (off_t)(idx * VERIFY_BLOCK_SIZE);
        size_t len = (size_t)(size - off);
        if (len > VERIFY_BLOCK_SIZE) len = VERIFY_BLOCK_SIZE;
        if (cb(idx, off, len, arg) != 0) return -1;
        emitted++;
    }
    return 0;
}

typedef struct {
    int src_fd;
    int dst_fd;
    const char *path;
    uint8_t src_buf[VERIFY_BLOCK_SIZE];
    uint8_t dst_buf[VERIFY_BLOCK_SIZE];
    uint64_t bytes;
    uint64_t blocks;
    int mismatch;
} local_ctx_t;

static int compare_sample(uint64_t index, off_t offset, size_t length, void *arg)
{
    local_ctx_t *ctx = arg;
    (void)index;
    ssize_t sr = pread_full(ctx->src_fd, ctx->src_buf, length, offset);
    ssize_t dr = pread_full(ctx->dst_fd, ctx->dst_buf, length, offset);
    ctx->blocks++;
    ctx->bytes += length;
    if (sr != (ssize_t)length || dr != (ssize_t)length ||
        memcmp(ctx->src_buf, ctx->dst_buf, length) != 0) {
        if (!ctx->mismatch) {
            progress_interrupt();
            fprintf(stderr,
                    "ecopy: verification data mismatch: %s at offset %" PRId64 "\n",
                    ctx->path, (int64_t)offset);
        }
        ctx->mismatch = 1;
    }
    return 0;
}

typedef struct {
    const verify_item_t *item;
    int src_fd;
    verify_digest_t batch[VERIFY_BATCH_MAX];
    size_t count;
    uint64_t bytes;
    uint64_t blocks;
    int failed;
} remote_ctx_t;

static int flush_remote_batch(remote_ctx_t *ctx, int final)
{
    if (sshx_verify_batch(ctx->item->dst, &ctx->item->src_st,
                          ctx->batch, ctx->count, final) != 0) {
        ctx->failed = 1;
        return -1;
    }
    ctx->count = 0;
    return 0;
}

static int hash_sample(uint64_t index, off_t offset, size_t length, void *arg)
{
    remote_ctx_t *ctx = arg;
    uint8_t buf[VERIFY_BLOCK_SIZE];
    (void)index;
    if (pread_full(ctx->src_fd, buf, length, offset) != (ssize_t)length) {
        ctx->failed = 1;
        return -1;
    }
    verify_digest_t *d = &ctx->batch[ctx->count++];
    d->offset = (int64_t)offset;
    d->length = (uint32_t)length;
    blake3_hash(buf, length, d->digest);
    ctx->blocks++;
    ctx->bytes += length;
    if (ctx->count == VERIFY_BATCH_MAX) return flush_remote_batch(ctx, 0);
    return 0;
}

static int verify_local_item(const verify_item_t *item)
{
    local_ctx_t ctx = { .src_fd = -1, .dst_fd = -1, .path = item->dst };
    int failed = 0;
    if (g_cfg.data && item->src_st.st_size > 0) {
        ctx.src_fd = open(item->src, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        ctx.dst_fd = open(item->dst, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        if (ctx.src_fd < 0 || ctx.dst_fd < 0 ||
            for_each_sample(item, compare_sample, &ctx) != 0) {
            progress_interrupt();
            if (ctx.src_fd < 0) perror(item->src);
            else if (ctx.dst_fd < 0) perror(item->dst);
            failed = 1;
        }
        if (ctx.src_fd >= 0) close(ctx.src_fd);
        if (ctx.dst_fd >= 0) close(ctx.dst_fd);
        if (ctx.mismatch) failed = 1;
    }

    /* Verification reads must not become the final destination atime. */
    if (copy_policy_preserve_times() && preserve_path_metadata(item->dst, &item->src_st) != 0) {
        failed = 1;
    }
    int meta_bad = 0;
    if (g_cfg.metadata) {
        struct stat actual;
        if (lstat(item->dst, &actual) != 0 ||
            verify_metadata_stat(&item->src_st, &actual, 0, item->dst) != 0) {
            meta_bad = 1;
            failed = 1;
        }
    }
    stats_record_verify(ctx.bytes, (uint64_t)item->src_st.st_size,
                        ctx.blocks, g_cfg.metadata,
                        ctx.mismatch, meta_bad, failed);
    return failed ? -1 : 0;
}

static int verify_remote_item(const verify_item_t *item)
{
    remote_ctx_t ctx = { .item = item, .src_fd = -1 };
    if (g_cfg.data && item->src_st.st_size > 0) {
        ctx.src_fd = open(item->src, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        if (ctx.src_fd < 0) {
            perror(item->src);
            ctx.failed = 1;
        } else if (for_each_sample(item, hash_sample, &ctx) != 0) {
            ctx.failed = 1;
        }
        if (ctx.src_fd >= 0) close(ctx.src_fd);
    }
    if (!ctx.failed && (ctx.count || g_cfg.data || g_cfg.metadata)) {
        if (flush_remote_batch(&ctx, 1) != 0) ctx.failed = 1;
    }
    stats_record_verify(ctx.bytes, (uint64_t)item->src_st.st_size,
                        ctx.blocks, g_cfg.metadata,
                        0, 0, ctx.failed);
    return ctx.failed ? -1 : 0;
}

int verify_run_queued(int remote)
{
    verify_item_t *item;
    int failed = 0;
    struct timespec start, finish;
    clock_gettime(CLOCK_MONOTONIC, &start);
    pthread_mutex_lock(&g_queue_lock);
    item = g_head;
    g_head = g_tail = NULL;
    pthread_mutex_unlock(&g_queue_lock);

    while (item) {
        verify_item_t *next = item->next;
        if ((remote ? verify_remote_item(item) : verify_local_item(item)) != 0) {
            failed = 1;
        }
        free(item->src);
        free(item->dst);
        free(item);
        item = next;
    }
    if (remote && verify_enabled() && sshx_barrier(0) != 0) {
        stats_mark_verify_failure();
        failed = 1;
    }
    clock_gettime(CLOCK_MONOTONIC, &finish);
    int64_t elapsed_ns = (int64_t)(finish.tv_sec - start.tv_sec) * INT64_C(1000000000) +
                         (int64_t)finish.tv_nsec - (int64_t)start.tv_nsec;
    stats_add_verify_ns(elapsed_ns > 0 ? (uint64_t)elapsed_ns : 0);
    return failed ? -1 : 0;
}

void verify_queue_clear(void)
{
    verify_item_t *item;
    pthread_mutex_lock(&g_queue_lock);
    item = g_head;
    g_head = g_tail = NULL;
    pthread_mutex_unlock(&g_queue_lock);
    while (item) {
        verify_item_t *next = item->next;
        free(item->src);
        free(item->dst);
        free(item);
        item = next;
    }
}
