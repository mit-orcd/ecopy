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
#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
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
    int is_dir;
    struct verify_item *next;
} verify_item_t;

typedef struct {
    int metadata;
    int data;
    int include_skipped;
    double percent;
    uint64_t seed;
    int workers;
} verify_config_t;

static verify_config_t g_cfg;
static verify_item_t *g_head;
static verify_item_t *g_tail;
static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static _Atomic uint64_t g_run_queue_depth;
static _Atomic uint64_t g_run_active;
static atomic_flag g_atime_warning = ATOMIC_FLAG_INIT;

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
                      int include_skipped, uint64_t seed, int seed_set,
                      int workers)
{
    g_cfg.metadata = metadata ? 1 : 0;
    g_cfg.data = data ? 1 : 0;
    g_cfg.include_skipped = include_skipped ? 1 : 0;
    if (percent < 0.0) percent = 0.0;
    if (percent > 100.0) percent = 100.0;
    g_cfg.percent = percent;
    g_cfg.seed = seed_set ? seed : random_seed();
    if (workers < 1) workers = 1;
    if (workers > 128) workers = 128;
    g_cfg.workers = workers;
    stats_set_verify_config(g_cfg.metadata, g_cfg.data, g_cfg.percent, g_cfg.seed);
}

int verify_enabled(void) { return g_cfg.metadata || g_cfg.data; }
int verify_metadata_enabled(void) { return g_cfg.metadata; }
int verify_data_enabled(void) { return g_cfg.data; }
int verify_include_skipped(void) { return g_cfg.include_skipped; }
double verify_percent(void) { return g_cfg.percent; }
uint64_t verify_seed(void) { return g_cfg.seed; }
int verify_worker_count(void) { return g_cfg.workers; }
uint64_t verify_queue_depth(void) { return atomic_load(&g_run_queue_depth); }
uint64_t verify_active_count(void) { return atomic_load(&g_run_active); }

static verify_item_t *make_item(const char *src, const char *dst,
                                const struct stat *src_st, int skipped,
                                int is_dir)
{
    verify_item_t *item;
    item = calloc(1, sizeof(*item));
    if (!item) return NULL;
    item->src = strdup(src);
    item->dst = strdup(dst);
    if (!item->src || !item->dst) {
        free(item->src);
        free(item->dst);
        free(item);
        return NULL;
    }
    item->src_st = *src_st;
    item->skipped = skipped;
    item->is_dir = is_dir;
    return item;
}

int verify_queue_file(const char *src, const char *dst,
                      const struct stat *src_st, int skipped)
{
    verify_item_t *item;
    if (!verify_enabled() || (skipped && !g_cfg.include_skipped)) return 0;
    item = make_item(src, dst, src_st, skipped, 0);
    if (!item) return -1;

    pthread_mutex_lock(&g_queue_lock);
    if (g_tail) g_tail->next = item;
    else g_head = item;
    g_tail = item;
    pthread_mutex_unlock(&g_queue_lock);
    return 0;
}

int verify_queue_directory(const char *src, const char *dst,
                           const struct stat *src_st)
{
    verify_item_t *item;
    if (!g_cfg.metadata) return 0;
    item = make_item(src, dst, src_st, 0, 1);
    if (!item) return -1;
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
        stats_record_verify(0, 0, 0, 1, 0, 0, 0, 1, 1);
        return -1;
    }
    int rc = verify_metadata_stat(expected, &actual, is_dir, path);
    stats_record_verify(0, 0, 0, 1, 0, rc != 0, 0, 0, rc != 0);
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

typedef struct {
    off_t start;
    off_t end;
} data_extent_t;

typedef struct {
    data_extent_t *extents;
    size_t count;
    size_t cap;
    int supported;
} extent_map_t;

static void extent_map_destroy(extent_map_t *map)
{
    free(map->extents);
    memset(map, 0, sizeof(*map));
}

static int extent_map_build(int fd, off_t size, extent_map_t *map)
{
    memset(map, 0, sizeof(*map));
    map->supported = 1;
    for (off_t pos = 0; pos < size;) {
        off_t data = lseek(fd, pos, SEEK_DATA);
        if (data < 0) {
            if (errno == ENXIO) break;
            if (errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
                extent_map_destroy(map);
                return 0;
            }
            return -1;
        }
        off_t hole = lseek(fd, data, SEEK_HOLE);
        if (hole < 0) {
            if (errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
                extent_map_destroy(map);
                return 0;
            }
            return -1;
        }
        if (hole > size) hole = size;
        if (map->count == map->cap) {
            size_t cap = map->cap ? map->cap * 2 : 16;
            data_extent_t *p = realloc(map->extents, cap * sizeof(*p));
            if (!p) return -1;
            map->extents = p;
            map->cap = cap;
        }
        map->extents[map->count++] = (data_extent_t){ data, hole };
        pos = hole > pos ? hole : pos + 1;
    }
    return 0;
}

static int extent_map_range_is_hole(const extent_map_t *map,
                                    off_t offset, size_t length)
{
    if (!map->supported) return 0;
    off_t end = offset + (off_t)length;
    size_t lo = 0, hi = map->count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (map->extents[mid].end <= offset) lo = mid + 1;
        else hi = mid;
    }
    return lo == map->count || map->extents[lo].start >= end;
}

typedef int (*sample_cb)(uint64_t index, off_t offset, size_t length, void *arg);

typedef struct {
    uint64_t index;
    off_t offset;
    size_t length;
} sample_desc_t;

static int compare_sample_offset(const void *a, const void *b)
{
    const sample_desc_t *sa = a;
    const sample_desc_t *sb = b;
    return sa->offset < sb->offset ? -1 : sa->offset > sb->offset;
}

static int emit_sample_batch(sample_desc_t *batch, size_t *count,
                             sample_cb cb, void *arg)
{
    qsort(batch, *count, sizeof(*batch), compare_sample_offset);
    for (size_t i = 0; i < *count; i++) {
        if (cb(batch[i].index, batch[i].offset, batch[i].length, arg) != 0) {
            return -1;
        }
    }
    *count = 0;
    return 0;
}

static int for_each_sample(const verify_item_t *item, sample_cb cb, void *arg)
{
    uint64_t n, target, emitted = 0;
    uint64_t seed, a, b, domain = 1;
    sample_desc_t batch[VERIFY_BATCH_MAX];
    size_t batch_count = 0;
    off_t size = item->src_st.st_size;
    if (size <= 0 || !g_cfg.data) return 0;
    n = ((uint64_t)size + VERIFY_BLOCK_SIZE - 1) / VERIFY_BLOCK_SIZE;
    target = (uint64_t)((g_cfg.percent * (double)n + 99.999999) / 100.0);
    if (target < (n == 1 ? 1 : 2)) target = n == 1 ? 1 : 2;
    if (target > n) target = n;

    /* Full coverage is an ordinary sequential scan, not a random permutation. */
    if (target == n) {
        for (uint64_t idx = 0; idx < n; idx++) {
            off_t off = (off_t)(idx * VERIFY_BLOCK_SIZE);
            size_t len = (size_t)(size - off);
            if (len > VERIFY_BLOCK_SIZE) len = VERIFY_BLOCK_SIZE;
            if (cb(idx, off, len, arg) != 0) return -1;
        }
        return 0;
    }

#define APPEND_SAMPLE(idx_, off_, len_) do {                                  \
        batch[batch_count++] = (sample_desc_t){ (idx_), (off_), (len_) };      \
        emitted++;                                                             \
        if (batch_count == VERIFY_BATCH_MAX &&                                 \
            emit_sample_batch(batch, &batch_count, cb, arg) != 0) return -1;  \
    } while (0)

    APPEND_SAMPLE(0, 0,
                  (size_t)(size < VERIFY_BLOCK_SIZE ? size : VERIFY_BLOCK_SIZE));
    if (n > 1) {
        off_t off = (off_t)((n - 1) * VERIFY_BLOCK_SIZE);
        APPEND_SAMPLE(n - 1, off, (size_t)(size - off));
    }
    if (emitted >= target || n <= 2) {
        return emit_sample_batch(batch, &batch_count, cb, arg);
    }

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
        APPEND_SAMPLE(idx, off, len);
    }
    if (emit_sample_batch(batch, &batch_count, cb, arg) != 0) return -1;
#undef APPEND_SAMPLE
    return 0;
}

typedef struct {
    int src_fd;
    int dst_fd;
    const char *path;
    uint8_t src_buf[VERIFY_BLOCK_SIZE] __attribute__((aligned(VERIFY_BLOCK_SIZE)));
    uint8_t dst_buf[VERIFY_BLOCK_SIZE] __attribute__((aligned(VERIFY_BLOCK_SIZE)));
    uint64_t bytes;
    uint64_t blocks;
    uint64_t hole_bytes;
    uint64_t hole_blocks;
    extent_map_t extents;
    int mismatch;
    int data_mismatch;
    int zero_mismatch;
} local_ctx_t;

static int compare_sample(uint64_t index, off_t offset, size_t length, void *arg)
{
    local_ctx_t *ctx = arg;
    (void)index;
    int expected_zero = extent_map_range_is_hole(&ctx->extents, offset, length);
    ssize_t sr = expected_zero
                     ? (ssize_t)length
                     : pread_full(ctx->src_fd, ctx->src_buf, length, offset);
    ssize_t dr = pread_full(ctx->dst_fd, ctx->dst_buf, length, offset);
    ctx->blocks++;
    ctx->bytes += length;
    if (expected_zero) {
        memset(ctx->src_buf, 0, length);
        ctx->hole_blocks++;
        ctx->hole_bytes += length;
    }
    if (sr != (ssize_t)length || dr != (ssize_t)length ||
        memcmp(ctx->src_buf, ctx->dst_buf, length) != 0) {
        if (!ctx->mismatch) {
            progress_interrupt();
            fprintf(stderr,
                    "ecopy: verification data mismatch: %s at offset %" PRId64 "\n",
                    ctx->path, (int64_t)offset);
        }
        ctx->mismatch = 1;
        if (expected_zero) ctx->zero_mismatch = 1;
        else ctx->data_mismatch = 1;
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
    uint64_t hole_bytes;
    uint64_t hole_blocks;
    extent_map_t extents;
    int failed;
} remote_ctx_t;

static int flush_remote_batch(remote_ctx_t *ctx, int check_metadata)
{
    if (sshx_verify_batch(ctx->item->dst, &ctx->item->src_st,
                          ctx->batch, ctx->count, ctx->item->is_dir,
                          check_metadata && g_cfg.metadata) != 0) {
        ctx->failed = 1;
        return -1;
    }
    ctx->count = 0;
    return 0;
}

static int hash_sample(uint64_t index, off_t offset, size_t length, void *arg)
{
    remote_ctx_t *ctx = arg;
    uint8_t buf[VERIFY_BLOCK_SIZE] __attribute__((aligned(VERIFY_BLOCK_SIZE)));
    (void)index;
    int expected_zero = extent_map_range_is_hole(&ctx->extents, offset, length);
    if (!expected_zero &&
        pread_full(ctx->src_fd, buf, length, offset) != (ssize_t)length) {
        ctx->failed = 1;
        return -1;
    }
    verify_digest_t *d = &ctx->batch[ctx->count++];
    d->offset = (int64_t)offset;
    d->length = (uint32_t)length;
    d->flags = expected_zero ? VERIFY_SAMPLE_EXPECT_ZERO : 0;
    if (expected_zero) {
        memset(d->digest, 0, sizeof(d->digest));
        ctx->hole_blocks++;
        ctx->hole_bytes += length;
    } else {
        blake3_hash(buf, length, d->digest);
    }
    ctx->blocks++;
    ctx->bytes += length;
    if (ctx->count == VERIFY_BATCH_MAX) return flush_remote_batch(ctx, 0);
    return 0;
}

static int open_verify_data(const char *path)
{
    int flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW;
#ifdef O_NOATIME
    if (g_cfg.metadata && copy_policy_preserve_times()) {
        int fd = open(path, flags | O_NOATIME);
        if (fd >= 0) return fd;
        progress_interrupt();
        fprintf(stderr,
                "ecopy: verification cannot open %s with O_NOATIME: %s; "
                "use --verify-metadata or --no-preserve-times\n",
                path, strerror(errno));
        return -1;
    }
#endif
    if (!atomic_flag_test_and_set(&g_atime_warning)) {
        progress_interrupt();
        fprintf(stderr,
                "ecopy: verification data reads may update atime because "
                "timestamps are not being checked\n");
    }
    return open(path, flags);
}

static int source_still_matches(const verify_item_t *item, int fd)
{
    struct stat now;
    if (fstat(fd, &now) != 0 ||
        !S_ISREG(now.st_mode) ||
        now.st_dev != item->src_st.st_dev ||
        now.st_ino != item->src_st.st_ino ||
        now.st_size != item->src_st.st_size ||
        !timestamp_equal(&now.st_mtim, &item->src_st.st_mtim)) {
        progress_interrupt();
        fprintf(stderr, "ecopy: source changed during verification: %s\n",
                item->src);
        return -1;
    }
    return 0;
}

static int source_is_sparse(const struct stat *st)
{
    return st->st_size > 0 &&
           (uint64_t)st->st_blocks * UINT64_C(512) < (uint64_t)st->st_size;
}

static void advise_verification_access(int fd)
{
    int advice = g_cfg.percent >= 100.0 ? POSIX_FADV_SEQUENTIAL
                                        : POSIX_FADV_RANDOM;
    (void)posix_fadvise(fd, 0, 0, advice);
}

static int verify_local_item(const verify_item_t *item)
{
    local_ctx_t ctx = { .src_fd = -1, .dst_fd = -1, .path = item->dst };
    int failed = 0;
    int meta_bad = 0;
    int structural_bad = 0;

    /* Compare metadata before reads so verification can never hide a mismatch. */
    if (g_cfg.metadata) {
        struct stat actual;
        if (lstat(item->dst, &actual) != 0) {
            progress_interrupt();
            fprintf(stderr, "ecopy: verification cannot stat %s: %s\n",
                    item->dst, strerror(errno));
            meta_bad = 1;
            failed = 1;
            structural_bad = 1;
        } else if (verify_metadata_stat(&item->src_st, &actual, item->is_dir,
                                        item->dst) != 0) {
            meta_bad = 1;
            failed = 1;
            if ((actual.st_mode & S_IFMT) != (item->src_st.st_mode & S_IFMT) ||
                (!item->is_dir && actual.st_size != item->src_st.st_size)) {
                structural_bad = 1;
            }
        }
    } else {
        struct stat actual;
        if (lstat(item->dst, &actual) != 0 ||
            (item->is_dir ? !S_ISDIR(actual.st_mode) :
                            (!S_ISREG(actual.st_mode) ||
                             actual.st_size != item->src_st.st_size))) {
            progress_interrupt();
            fprintf(stderr,
                    "ecopy: verification target missing or wrong type/size: %s\n",
                    item->dst);
            failed = 1;
            structural_bad = 1;
        }
    }

    if (!item->is_dir && !structural_bad &&
        g_cfg.data && item->src_st.st_size > 0) {
        ctx.src_fd = open_verify_data(item->src);
        ctx.dst_fd = open_verify_data(item->dst);
        if (ctx.src_fd >= 0) advise_verification_access(ctx.src_fd);
        if (ctx.dst_fd >= 0) advise_verification_access(ctx.dst_fd);
        if (ctx.src_fd >= 0 && source_is_sparse(&item->src_st) &&
            extent_map_build(ctx.src_fd, item->src_st.st_size,
                             &ctx.extents) != 0) {
            failed = 1;
        }
        if (ctx.src_fd < 0 || ctx.dst_fd < 0 ||
            source_still_matches(item, ctx.src_fd) != 0 ||
            for_each_sample(item, compare_sample, &ctx) != 0 ||
            source_still_matches(item, ctx.src_fd) != 0) {
            progress_interrupt();
            if (ctx.src_fd < 0) perror(item->src);
            else if (ctx.dst_fd < 0) perror(item->dst);
            failed = 1;
        }
        if (ctx.src_fd >= 0) close(ctx.src_fd);
        if (ctx.dst_fd >= 0) close(ctx.dst_fd);
        extent_map_destroy(&ctx.extents);
        if (ctx.mismatch) failed = 1;
    }

    stats_record_verify(ctx.bytes,
                        item->is_dir ? 0 : (uint64_t)item->src_st.st_size,
                        ctx.blocks, g_cfg.metadata,
                        ctx.data_mismatch, meta_bad, ctx.zero_mismatch,
                        failed && !ctx.mismatch && !meta_bad, failed);
    stats_record_verify_holes(ctx.hole_blocks, ctx.hole_bytes);
    return failed ? -1 : 0;
}

static int verify_remote_item(const verify_item_t *item)
{
    remote_ctx_t ctx = { .item = item, .src_fd = -1 };
    if (!item->is_dir && g_cfg.data && item->src_st.st_size > 0) {
        ctx.src_fd = open_verify_data(item->src);
        if (ctx.src_fd < 0) {
            perror(item->src);
            ctx.failed = 1;
        } else {
            advise_verification_access(ctx.src_fd);
            if (source_is_sparse(&item->src_st) &&
                extent_map_build(ctx.src_fd, item->src_st.st_size,
                                 &ctx.extents) != 0) {
                ctx.failed = 1;
            }
        }
        if (!ctx.failed && ctx.src_fd >= 0 &&
            (source_still_matches(item, ctx.src_fd) != 0 ||
                   for_each_sample(item, hash_sample, &ctx) != 0 ||
             source_still_matches(item, ctx.src_fd) != 0)) {
            ctx.failed = 1;
        }
        if (ctx.src_fd >= 0) close(ctx.src_fd);
        extent_map_destroy(&ctx.extents);
    }
    if (!ctx.failed && (ctx.count || g_cfg.data || g_cfg.metadata || item->is_dir)) {
        if (flush_remote_batch(&ctx, 1) != 0) ctx.failed = 1;
    }
    stats_record_verify(ctx.bytes,
                        item->is_dir ? 0 : (uint64_t)item->src_st.st_size,
                        ctx.blocks, g_cfg.metadata,
                        0, 0, 0, 0, ctx.failed);
    stats_record_verify_holes(ctx.hole_blocks, ctx.hole_bytes);
    return ctx.failed ? -1 : 0;
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t work_cv;
    pthread_cond_t space_cv;
    verify_item_t *head;
    verify_item_t *tail;
    size_t queued;
    size_t limit;
    uint64_t queue_peak;
    uint64_t active_peak;
    int stop;
    int failed;
    int remote;
    int nthreads;
    pthread_t *threads;
} verify_pool_t;

static void free_item(verify_item_t *item)
{
    if (!item) return;
    free(item->src);
    free(item->dst);
    free(item);
}

static void *verify_pool_worker(void *arg)
{
    verify_pool_t *pool = arg;
    for (;;) {
        pthread_mutex_lock(&pool->lock);
        while (!pool->head && !pool->stop) {
            pthread_cond_wait(&pool->work_cv, &pool->lock);
        }
        if (!pool->head) {
            pthread_mutex_unlock(&pool->lock);
            break;
        }
        verify_item_t *item = pool->head;
        pool->head = item->next;
        if (!pool->head) pool->tail = NULL;
        pool->queued--;
        atomic_store(&g_run_queue_depth, pool->queued);
        uint64_t active = atomic_fetch_add(&g_run_active, 1) + 1;
        if (active > pool->active_peak) pool->active_peak = active;
        pthread_cond_signal(&pool->space_cv);
        pthread_mutex_unlock(&pool->lock);

        item->next = NULL;
        if ((pool->remote ? verify_remote_item(item) :
                            verify_local_item(item)) != 0) {
            pthread_mutex_lock(&pool->lock);
            pool->failed = 1;
            pthread_mutex_unlock(&pool->lock);
        }
        free_item(item);
        atomic_fetch_sub(&g_run_active, 1);
    }
    return NULL;
}

static int verify_pool_start(verify_pool_t *pool, int remote)
{
    memset(pool, 0, sizeof(*pool));
    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->work_cv, NULL);
    pthread_cond_init(&pool->space_cv, NULL);
    pool->remote = remote;
    pool->nthreads = g_cfg.workers;
    pool->limit = (size_t)pool->nthreads * 4;
    if (pool->limit < 16) pool->limit = 16;
    pool->threads = calloc((size_t)pool->nthreads, sizeof(*pool->threads));
    if (!pool->threads) return -1;
    for (int i = 0; i < pool->nthreads; i++) {
        if (pthread_create(&pool->threads[i], NULL, verify_pool_worker, pool) != 0) {
            pthread_mutex_lock(&pool->lock);
            pool->stop = 1;
            pthread_cond_broadcast(&pool->work_cv);
            pthread_mutex_unlock(&pool->lock);
            for (int j = 0; j < i; j++) pthread_join(pool->threads[j], NULL);
            free(pool->threads);
            pool->threads = NULL;
            return -1;
        }
    }
    return 0;
}

static int verify_pool_submit(verify_pool_t *pool, verify_item_t *item)
{
    pthread_mutex_lock(&pool->lock);
    while (pool->queued >= pool->limit && !pool->stop) {
        pthread_cond_wait(&pool->space_cv, &pool->lock);
    }
    if (pool->stop) {
        pthread_mutex_unlock(&pool->lock);
        return -1;
    }
    item->next = NULL;
    if (pool->tail) pool->tail->next = item;
    else pool->head = item;
    pool->tail = item;
    pool->queued++;
    if (pool->queued > pool->queue_peak) pool->queue_peak = pool->queued;
    atomic_store(&g_run_queue_depth, pool->queued);
    pthread_cond_signal(&pool->work_cv);
    pthread_mutex_unlock(&pool->lock);
    return 0;
}

static int verify_pool_finish(verify_pool_t *pool)
{
    pthread_mutex_lock(&pool->lock);
    pool->stop = 1;
    pthread_cond_broadcast(&pool->work_cv);
    pthread_mutex_unlock(&pool->lock);
    for (int i = 0; i < pool->nthreads; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    int failed = pool->failed;
    stats_set_verify_runtime(-1, pool->nthreads,
                             pool->queue_peak, pool->active_peak);
    free(pool->threads);
    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->work_cv);
    pthread_cond_destroy(&pool->space_cv);
    atomic_store(&g_run_queue_depth, 0);
    atomic_store(&g_run_active, 0);
    return failed ? -1 : 0;
}

static int run_pool_list(verify_item_t *list, int remote)
{
    verify_pool_t pool;
    int failed = 0;
    if (verify_pool_start(&pool, remote) != 0) {
        fprintf(stderr, "ecopy: unable to start verification workers\n");
        while (list) {
            verify_item_t *next = list->next;
            free_item(list);
            list = next;
        }
        return -1;
    }
    while (list) {
        verify_item_t *next = list->next;
        if (verify_pool_submit(&pool, list) != 0) {
            free_item(list);
            failed = 1;
        }
        list = next;
    }
    if (verify_pool_finish(&pool) != 0) failed = 1;
    return failed ? -1 : 0;
}

int verify_run_queued(int remote)
{
    verify_item_t *item;
    int failed;
    pthread_mutex_lock(&g_queue_lock);
    item = g_head;
    g_head = g_tail = NULL;
    pthread_mutex_unlock(&g_queue_lock);

    failed = run_pool_list(item, remote) != 0;
    if (remote && verify_enabled() && sshx_barrier(0) != 0) {
        failed = 1;
    }
    return failed ? -1 : 0;
}

static int join_child_path(const char *parent, const char *name,
                           char *out, size_t out_sz)
{
    int n = snprintf(out, out_sz, "%s%s%s", parent,
                     strcmp(parent, "/") == 0 ? "" : "/", name);
    if (n < 0 || (size_t)n >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int walk_submit(verify_pool_t *pool, const char *src, const char *dst,
                       const struct stat *known)
{
    struct stat st;
    if (known) st = *known;
    else if (lstat(src, &st) != 0) {
        progress_interrupt();
        fprintf(stderr, "ecopy: verification cannot stat %s: %s\n",
                src, strerror(errno));
        return -1;
    }

    if (!S_ISREG(st.st_mode) && !S_ISDIR(st.st_mode)) return 0;
    verify_item_t *item = make_item(src, dst, &st, 0, S_ISDIR(st.st_mode));
    if (!item || verify_pool_submit(pool, item) != 0) {
        free_item(item);
        return -1;
    }
    if (S_ISREG(st.st_mode)) {
        stats_inc_files_seen();
        return 0;
    }

    stats_inc_dirs_seen();
    int dir_flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW;
#ifdef O_NOATIME
    if (g_cfg.metadata && copy_policy_preserve_times()) dir_flags |= O_NOATIME;
#endif
    int dir_fd = open(src, dir_flags);
    DIR *dir = dir_fd >= 0 ? fdopendir(dir_fd) : NULL;
    if (!dir) {
        if (dir_fd >= 0) close(dir_fd);
        progress_interrupt();
        fprintf(stderr,
                "ecopy: verification cannot open directory %s without changing "
                "timestamps: %s; use --no-preserve-times\n",
                src, strerror(errno));
        return -1;
    }
    int failed = 0;
    for (;;) {
        errno = 0;
        struct dirent *de = readdir(dir);
        if (!de) {
            if (errno != 0) failed = 1;
            break;
        }
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
            continue;
        }
        char child_src[PATH_MAX], child_dst[PATH_MAX];
        if (join_child_path(src, de->d_name, child_src, sizeof(child_src)) != 0 ||
            join_child_path(dst, de->d_name, child_dst, sizeof(child_dst)) != 0) {
            progress_interrupt();
            fprintf(stderr, "ecopy: verification path too long below %s\n", src);
            failed = 1;
            continue;
        }
        struct stat child_st;
        if (lstat(child_src, &child_st) != 0) {
            failed = 1;
            continue;
        }
        if ((S_ISREG(child_st.st_mode) || S_ISDIR(child_st.st_mode)) &&
            walk_submit(pool, child_src, child_dst, &child_st) != 0) {
            failed = 1;
        }
    }
    closedir(dir);
    return failed ? -1 : 0;
}

int verify_run_tree(const char *src, const char *dst, int remote,
                    int source_is_dir)
{
    verify_pool_t pool;
    struct stat st;
    int failed = 0;
    (void)source_is_dir;
    if (lstat(src, &st) != 0) {
        perror(src);
        return -1;
    }
    if (verify_pool_start(&pool, remote) != 0) {
        fprintf(stderr, "ecopy: unable to start verification workers\n");
        return -1;
    }
    if (walk_submit(&pool, src, dst, &st) != 0) failed = 1;
    if (verify_pool_finish(&pool) != 0) failed = 1;
    stats_set_traversal_done();
    if (remote && sshx_barrier(0) != 0) {
        failed = 1;
    }
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
        free_item(item);
        item = next;
    }
}
