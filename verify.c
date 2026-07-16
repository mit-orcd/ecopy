/*
 * Transfer verification implementation.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */
#define _GNU_SOURCE
#include "verify.h"

#include "config.h"
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
    int durable;
    int arena_owned;
    int dst_heap;
    struct verify_item *next;
} verify_item_t;

#define VERIFY_ARENA_CHUNK_SIZE (1024u * 1024u)
typedef struct verify_arena_chunk {
    struct verify_arena_chunk *next;
    size_t used;
    max_align_t align;
    unsigned char data[VERIFY_ARENA_CHUNK_SIZE];
} verify_arena_chunk_t;

typedef struct {
    int metadata;
    int data;
    int include_skipped;
    double percent;
    uint64_t seed;
    int workers;
} verify_config_t;

static verify_config_t g_cfg;
static verify_item_t *g_head;          /* file intake list (fed to the pool) */
static verify_item_t *g_tail;
static verify_item_t *g_dir_head;      /* directory items, held until finish */
static verify_item_t *g_dir_tail;
static verify_arena_chunk_t *g_arena_head;
static verify_arena_chunk_t *g_arena_tail;
static uint64_t g_pending_count;
static uint64_t g_pending_peak;
static pthread_mutex_t g_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_queue_cv = PTHREAD_COND_INITIALIZER;
static _Atomic uint64_t g_run_queue_depth;
static _Atomic uint64_t g_run_active;
static atomic_flag g_atime_warning = ATOMIC_FLAG_INIT;

/* Verify pipeline (feeder) state; g_pipe_pool is declared after verify_pool_t. */
static pthread_t g_feeder_thread;
static int g_feeder_started;
static int g_feeder_stop;              /* set under g_queue_lock */
static int g_feeder_failed;
static int g_pipe_remote;
static uint64_t g_pipeline_ops = VERIFY_PIPELINE_OPS_DEFAULT; /* gen size */

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

static verify_item_t *make_queued_item_locked(const char *src, const char *dst,
                                              const struct stat *src_st,
                                              int skipped, int is_dir)
{
    size_t src_len = strlen(src) + 1;
    size_t dst_len = strlen(dst) + 1;
    size_t align = _Alignof(verify_item_t);
    size_t item_size = sizeof(verify_item_t) + src_len + dst_len;
    size_t offset;

    if (item_size > VERIFY_ARENA_CHUNK_SIZE) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    if (!g_arena_tail) {
        g_arena_tail = calloc(1, sizeof(*g_arena_tail));
        if (!g_arena_tail) return NULL;
        g_arena_head = g_arena_tail;
    }
    offset = (g_arena_tail->used + align - 1) & ~(align - 1);
    if (offset + item_size > VERIFY_ARENA_CHUNK_SIZE) {
        verify_arena_chunk_t *chunk = calloc(1, sizeof(*chunk));
        if (!chunk) return NULL;
        g_arena_tail->next = chunk;
        g_arena_tail = chunk;
        offset = 0;
    }

    verify_item_t *item = (verify_item_t *)(void *)(g_arena_tail->data + offset);
    memset(item, 0, sizeof(*item));
    item->src = (char *)(item + 1);
    item->dst = item->src + src_len;
    memcpy(item->src, src, src_len);
    memcpy(item->dst, dst, dst_len);
    item->src_st = *src_st;
    item->skipped = skipped;
    item->is_dir = is_dir;
    item->arena_owned = 1;
    g_arena_tail->used = offset + item_size;
    return item;
}

static void append_queued_item_locked(verify_item_t *item)
{
    if (g_tail) g_tail->next = item;
    else g_head = item;
    g_tail = item;
    g_pending_count++;
    if (g_pending_count > g_pending_peak) g_pending_peak = g_pending_count;
}

int verify_queue_file(const char *src, const char *dst,
                      const struct stat *src_st, int skipped, int durable)
{
    verify_item_t *item;
    if (!verify_enabled() || (skipped && !g_cfg.include_skipped)) return 0;
    pthread_mutex_lock(&g_queue_lock);
    item = make_queued_item_locked(src, dst, src_st, skipped, 0);
    if (item) {
        item->durable = durable ? 1 : 0;
        append_queued_item_locked(item);
        /* Wake the feeder (a no-op when the pipeline is not running). */
        pthread_cond_signal(&g_queue_cv);
    }
    pthread_mutex_unlock(&g_queue_lock);
    return item ? 0 : -1;
}

int verify_queue_directory(const char *src, const char *dst,
                           const struct stat *src_st)
{
    verify_item_t *item;
    if (!g_cfg.metadata) return 0;
    pthread_mutex_lock(&g_queue_lock);
    item = make_queued_item_locked(src, dst, src_st, 0, 1);
    if (item) {
        /* Directory metadata is finalized deepest-first at the end of the run,
         * so these items are held on a separate list and only released in
         * verify_pipeline_finish() / verify_run_queued(). */
        if (g_dir_tail) g_dir_tail->next = item;
        else g_dir_head = item;
        g_dir_tail = item;
    }
    pthread_mutex_unlock(&g_queue_lock);
    return item ? 0 : -1;
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
            if (!item->arena_owned || item->dst_heap) free(item->dst);
            item->dst = replacement;
            item->dst_heap = 1;
        }
    }
    pthread_mutex_unlock(&g_queue_lock);
    return rc;
}

static int timestamp_equal(const struct timespec *a, const struct timespec *b)
{
    return a->tv_sec == b->tv_sec && a->tv_nsec == b->tv_nsec;
}

int verify_ownership_unpreservable(void)
{
    static int cached = -1;
    if (cached < 0) {
        /* Benign race: every thread computes the same value. The test hook lets
         * the single-user harness exercise the downgrade path deterministically. */
        cached = (getenv("ECOPY_TEST_FORCE_OWNERSHIP_MISMATCH") != NULL ||
                  geteuid() != 0) ? 1 : 0;
    }
    return cached;
}

static int force_ownership_mismatch(void)
{
    static int cached = -1;
    if (cached < 0)
        cached = getenv("ECOPY_TEST_FORCE_OWNERSHIP_MISMATCH") != NULL ? 1 : 0;
    return cached;
}

static _Atomic int g_ownership_warned;

/*
 * Compare source and target metadata. Genuine fields (type/size/mode/times) are
 * checked first, so a pure uid/gid difference is only classified as ownership
 * when everything else matches. When ownership cannot be preserved (unprivileged
 * process) that difference is downgraded to a one-time warning and reported as a
 * distinct category rather than a failure.
 */
verify_meta_class_t verify_metadata_stat(const struct stat *expected,
                                         const struct stat *actual,
                                         int is_dir,
                                         const char *path)
{
    const char *field = NULL;
    if ((actual->st_mode & S_IFMT) != (expected->st_mode & S_IFMT)) field = "type";
    else if (!is_dir && actual->st_size != expected->st_size) field = "size";
    else if ((actual->st_mode & 07777) != (expected->st_mode & 07777)) field = "mode";
    else if (copy_policy_preserve_times() &&
             !timestamp_equal(&actual->st_atim, &expected->st_atim)) field = "atime";
    else if (copy_policy_preserve_times() &&
             !timestamp_equal(&actual->st_mtim, &expected->st_mtim)) field = "mtime";

    if (field) {
        progress_interrupt();
        fprintf(stderr, "ecopy: verification metadata mismatch: %s (%s)\n",
                path, field);
        return VERIFY_META_MISMATCH;
    }

    const char *owner_field = NULL;
    if (actual->st_uid != expected->st_uid) owner_field = "uid";
    else if (actual->st_gid != expected->st_gid) owner_field = "gid";
    else if (force_ownership_mismatch()) owner_field = "uid";
    if (!owner_field) return VERIFY_META_OK;

    if (verify_ownership_unpreservable()) {
        if (atomic_exchange(&g_ownership_warned, 1) == 0) {
            progress_interrupt();
            fprintf(stderr,
                    "ecopy: verification: source ownership could not be preserved "
                    "without privilege; reporting uid/gid differences as warnings, "
                    "not failures\n");
        }
        return VERIFY_META_OWNERSHIP;
    }
    progress_interrupt();
    fprintf(stderr, "ecopy: verification metadata mismatch: %s (%s)\n",
            path, owner_field);
    return VERIFY_META_MISMATCH;
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
    verify_meta_class_t cls = verify_metadata_stat(expected, &actual, is_dir, path);
    if (cls == VERIFY_META_OWNERSHIP) stats_record_verify_ownership(1);
    int meta_bad = (cls == VERIFY_META_MISMATCH);
    stats_record_verify(0, 0, 0, 1, 0, meta_bad, 0, 0, meta_bad);
    return meta_bad ? -1 : 0;
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

/*
 * The domain of sampleable blocks for one file. For a dense file (no usable
 * extent map) this is the whole logical size, exactly as before. For a sparse
 * file it is only the blocks that overlap an allocated data extent, so
 * coverage is relative to real data: holes are never selected, hashed, or (over
 * SSH) sent as digests. Blocks are ordered by "ordinal" 0..data_n-1; each
 * ordinal maps to a file offset via the extent list (cum[] holds the running
 * block count before each extent). Data/hole boundaries from SEEK_DATA/SEEK_HOLE
 * are filesystem-block aligned, so per-extent block sets do not overlap.
 */
typedef struct {
    const extent_map_t *map;  /* NULL/unsupported => dense (logical) domain */
    off_t    size;
    uint64_t *cum;            /* cum[i] = data blocks before extent i */
    uint64_t data_n;          /* total sampleable blocks */
    uint64_t data_bytes;      /* allocated bytes in domain (== size when dense) */
} sample_domain_t;

static int sample_domain_init(const extent_map_t *map, off_t size,
                              sample_domain_t *d)
{
    memset(d, 0, sizeof(*d));
    d->size = size;
    if (!map || !map->supported) {
        d->data_n = ((uint64_t)size + VERIFY_BLOCK_SIZE - 1) / VERIFY_BLOCK_SIZE;
        d->data_bytes = (uint64_t)size;
        return 0;
    }
    d->map = map;
    if (map->count > 0) {
        d->cum = malloc(map->count * sizeof(*d->cum));
        if (!d->cum) return -1;
    }
    uint64_t total = 0, bytes = 0;
    for (size_t i = 0; i < map->count; i++) {
        d->cum[i] = total;
        uint64_t fb = (uint64_t)map->extents[i].start / VERIFY_BLOCK_SIZE;
        uint64_t lb = ((uint64_t)map->extents[i].end + VERIFY_BLOCK_SIZE - 1) /
                      VERIFY_BLOCK_SIZE;
        total += lb - fb;
        bytes += (uint64_t)(map->extents[i].end - map->extents[i].start);
    }
    d->data_n = total;
    d->data_bytes = bytes;
    return 0;
}

static void sample_domain_offset(const sample_domain_t *d, uint64_t ordinal,
                                 off_t *off, size_t *len)
{
    off_t o;
    if (!d->map) {
        o = (off_t)(ordinal * VERIFY_BLOCK_SIZE);
    } else {
        size_t lo = 0, hi = d->map->count;
        while (hi - lo > 1) {  /* find extent i: cum[i] <= ordinal < cum[i+1] */
            size_t mid = lo + (hi - lo) / 2;
            if (d->cum[mid] <= ordinal) lo = mid;
            else hi = mid;
        }
        uint64_t fb = (uint64_t)d->map->extents[lo].start / VERIFY_BLOCK_SIZE;
        uint64_t block = fb + (ordinal - d->cum[lo]);
        o = (off_t)(block * VERIFY_BLOCK_SIZE);
    }
    size_t l = (size_t)(d->size - o);
    if (l > VERIFY_BLOCK_SIZE) l = VERIFY_BLOCK_SIZE;
    *off = o;
    *len = l;
}

static void sample_domain_destroy(sample_domain_t *d)
{
    free(d->cum);
    d->cum = NULL;
}

static int for_each_sample(const verify_item_t *item, const sample_domain_t *dom,
                           sample_cb cb, void *arg)
{
    uint64_t n, target, emitted = 0;
    uint64_t seed, a, b, domain = 1;
    sample_desc_t batch[VERIFY_BATCH_MAX];
    size_t batch_count = 0;
    off_t size = item->src_st.st_size;
    if (size <= 0 || !g_cfg.data) return 0;
    n = dom->data_n;
    if (n == 0) return 0;  /* fully-hole file: no real data to sample */
    target = (uint64_t)((g_cfg.percent * (double)n + 99.999999) / 100.0);
    if (target < (n == 1 ? 1 : 2)) target = n == 1 ? 1 : 2;
    if (target > n) target = n;

    /* Full coverage is an ordinary sequential scan, not a random permutation. */
    if (target == n) {
        for (uint64_t idx = 0; idx < n; idx++) {
            off_t off; size_t len;
            sample_domain_offset(dom, idx, &off, &len);
            if (cb(idx, off, len, arg) != 0) return -1;
        }
        return 0;
    }

#define APPEND_SAMPLE(idx_) do {                                              \
        off_t off__; size_t len__;                                            \
        sample_domain_offset(dom, (idx_), &off__, &len__);                    \
        batch[batch_count++] = (sample_desc_t){ (idx_), off__, len__ };       \
        emitted++;                                                            \
        if (batch_count == VERIFY_BATCH_MAX &&                               \
            emit_sample_batch(batch, &batch_count, cb, arg) != 0) return -1; \
    } while (0)

    APPEND_SAMPLE(0);                       /* first data block */
    if (n > 1) APPEND_SAMPLE(n - 1);        /* last data block */
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
        APPEND_SAMPLE(idx);
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
        } else {
            verify_meta_class_t cls = verify_metadata_stat(
                &item->src_st, &actual, item->is_dir, item->dst);
            if (cls == VERIFY_META_MISMATCH) {
                meta_bad = 1;
                failed = 1;
                if ((actual.st_mode & S_IFMT) != (item->src_st.st_mode & S_IFMT) ||
                    (!item->is_dir && actual.st_size != item->src_st.st_size)) {
                    structural_bad = 1;
                }
            } else if (cls == VERIFY_META_OWNERSHIP) {
                stats_record_verify_ownership(1);
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

    uint64_t scope_bytes = item->is_dir ? 0 : (uint64_t)item->src_st.st_size;
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
        sample_domain_t dom;
        if (sample_domain_init(ctx.extents.supported ? &ctx.extents : NULL,
                               item->src_st.st_size, &dom) != 0) {
            failed = 1;
        } else {
            scope_bytes = dom.data_bytes;  /* coverage relative to real data */
            if (ctx.src_fd < 0 || ctx.dst_fd < 0 ||
                source_still_matches(item, ctx.src_fd) != 0 ||
                for_each_sample(item, &dom, compare_sample, &ctx) != 0 ||
                source_still_matches(item, ctx.src_fd) != 0) {
                progress_interrupt();
                if (ctx.src_fd < 0) perror(item->src);
                else if (ctx.dst_fd < 0) perror(item->dst);
                failed = 1;
            }
            sample_domain_destroy(&dom);
        }
        if (ctx.src_fd >= 0) close(ctx.src_fd);
        if (ctx.dst_fd >= 0) close(ctx.dst_fd);
        extent_map_destroy(&ctx.extents);
        if (ctx.mismatch) failed = 1;
    }

    stats_record_verify(ctx.bytes, scope_bytes,
                        ctx.blocks, g_cfg.metadata,
                        ctx.data_mismatch, meta_bad, ctx.zero_mismatch,
                        failed && !ctx.mismatch && !meta_bad, failed);
    stats_record_verify_holes(ctx.hole_blocks, ctx.hole_bytes);
    return failed ? -1 : 0;
}

static int verify_remote_item(const verify_item_t *item)
{
    remote_ctx_t ctx = { .item = item, .src_fd = -1 };
    uint64_t scope_bytes = item->is_dir ? 0 : (uint64_t)item->src_st.st_size;
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
        sample_domain_t dom = {0};
        if (!ctx.failed &&
            sample_domain_init(ctx.extents.supported ? &ctx.extents : NULL,
                               item->src_st.st_size, &dom) != 0) {
            ctx.failed = 1;
        } else if (!ctx.failed) {
            scope_bytes = dom.data_bytes;  /* coverage relative to real data */
            if (ctx.src_fd >= 0 &&
                (source_still_matches(item, ctx.src_fd) != 0 ||
                       for_each_sample(item, &dom, hash_sample, &ctx) != 0 ||
                 source_still_matches(item, ctx.src_fd) != 0)) {
                ctx.failed = 1;
            }
            sample_domain_destroy(&dom);
        }
        if (ctx.src_fd >= 0) close(ctx.src_fd);
        extent_map_destroy(&ctx.extents);
    }
    if (!ctx.failed && (ctx.count || g_cfg.data || g_cfg.metadata || item->is_dir)) {
        if (flush_remote_batch(&ctx, 1) != 0) ctx.failed = 1;
    }
    stats_record_verify(ctx.bytes, scope_bytes,
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
    int bind_seq;
    pthread_t *threads;
} verify_pool_t;

/* The long-lived pool used by the copy/verify pipeline. */
static verify_pool_t g_pipe_pool;

static void free_item(verify_item_t *item)
{
    if (!item) return;
    if (item->arena_owned) {
        if (item->dst_heap) free(item->dst);
        return;
    }
    free(item->src);
    free(item->dst);
    free(item);
}

static void free_arena(verify_arena_chunk_t *arena)
{
    while (arena) {
        verify_arena_chunk_t *next = arena->next;
        free(arena);
        arena = next;
    }
}

static void *verify_pool_worker(void *arg)
{
    verify_pool_t *pool = arg;

    /*
     * Spread remote verify batches across the SSH pool's verify role: when a
     * verify partition is active (pipelined dir copy) this pins verify frames to
     * the reserved verify connection(s) so they do not interleave with bulk copy
     * data; otherwise it falls back to the whole pool (verify-only, post-copy).
     */
    pthread_mutex_lock(&pool->lock);
    int bind_idx = pool->bind_seq++;
    pthread_mutex_unlock(&pool->lock);
    sshx_bind_thread_verify(bind_idx);

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
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        int item_failed = (pool->remote ? verify_remote_item(item) :
                                          verify_local_item(item)) != 0;
        clock_gettime(CLOCK_MONOTONIC, &t1);
        stats_add_verify_busy_ns(
            (uint64_t)((t1.tv_sec - t0.tv_sec) * 1000000000LL +
                       (t1.tv_nsec - t0.tv_nsec)));
        if (item_failed) {
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
    verify_item_t *tail;
    verify_arena_chunk_t *arena;
    int failed;
    pthread_mutex_lock(&g_queue_lock);
    item = g_head;
    tail = g_tail;
    /* Directory items were held on a separate list; append them so the single
     * post-copy drain verifies both files and directories. */
    if (g_dir_head) {
        if (tail) tail->next = g_dir_head;
        else item = g_dir_head;
    }
    arena = g_arena_head;
    g_head = g_tail = NULL;
    g_dir_head = g_dir_tail = NULL;
    g_arena_head = g_arena_tail = NULL;
    stats_set_verify_pending_peak(g_pending_peak);
    g_pending_count = 0;
    g_pending_peak = 0;
    pthread_mutex_unlock(&g_queue_lock);

    failed = run_pool_list(item, remote) != 0;
    free_arena(arena);
    if (remote && verify_enabled() && sshx_barrier_all(0) != 0) {
        failed = 1;
    }
    return failed ? -1 : 0;
}

/* Barrier-gate then submit a generation of non-durable (remote fire-and-forget)
 * items. The barrier drains every SSH connection, materializing the PUTFILE
 * targets before verification opens them. Local items are always durable, so
 * this list stays empty and no barrier is issued for local runs. */
static int pipeline_flush_gen(verify_item_t **head, verify_item_t **tail,
                              uint64_t *count)
{
    int rc = 0;
    verify_item_t *it = *head;
    if (!it) return 0;
    if (g_pipe_remote && sshx_barrier_all(0) != 0) rc = -1;
    while (it) {
        verify_item_t *next = it->next;
        it->next = NULL;
        if (verify_pool_submit(&g_pipe_pool, it) != 0) rc = -1;
        it = next;
    }
    *head = *tail = NULL;
    *count = 0;
    return rc;
}

static void *verify_feeder_main(void *arg)
{
    (void)arg;
    verify_item_t *gen_head = NULL, *gen_tail = NULL;
    uint64_t gen_count = 0;

    /* Bind the feeder to a connection so its barriers use a valid stream. */
    sshx_bind_thread(0);

    for (;;) {
        int timed_out = 0;
        pthread_mutex_lock(&g_queue_lock);
        while (!g_head && !g_feeder_stop) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 250L * 1000L * 1000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec += 1;
                ts.tv_nsec -= 1000000000L;
            }
            if (pthread_cond_timedwait(&g_queue_cv, &g_queue_lock, &ts) ==
                ETIMEDOUT) {
                timed_out = 1;
                break;
            }
        }
        verify_item_t *batch = g_head;
        g_head = g_tail = NULL;
        int stop = g_feeder_stop;
        pthread_mutex_unlock(&g_queue_lock);

        uint64_t stolen = 0;
        while (batch) {
            verify_item_t *next = batch->next;
            batch->next = NULL;
            if (batch->durable) {
                if (verify_pool_submit(&g_pipe_pool, batch) != 0) {
                    g_feeder_failed = 1;
                }
            } else {
                if (gen_tail) gen_tail->next = batch;
                else gen_head = batch;
                gen_tail = batch;
                gen_count++;
            }
            stolen++;
            batch = next;
        }
        if (stolen) {
            pthread_mutex_lock(&g_queue_lock);
            g_pending_count -= stolen;
            pthread_mutex_unlock(&g_queue_lock);
        }

        /* Release a generation when it is full or when copy has gone idle. */
        if (gen_count &&
            (gen_count >= g_pipeline_ops || timed_out || stop)) {
            if (pipeline_flush_gen(&gen_head, &gen_tail, &gen_count) != 0) {
                g_feeder_failed = 1;
            }
        }
        if (stop) break;
    }
    return NULL;
}

int verify_pipeline_start(int remote)
{
    const char *env;
    if (!verify_enabled()) return 0;

    env = getenv("DIRECT_COPY_VERIFY_PIPELINE_OPS");
    if (env && *env) {
        long v = strtol(env, NULL, 10);
        if (v >= 1 && v <= 1000000) g_pipeline_ops = (uint64_t)v;
    }
    g_pipe_remote = remote;
    g_feeder_stop = 0;
    g_feeder_failed = 0;

    if (verify_pool_start(&g_pipe_pool, remote) != 0) {
        fprintf(stderr, "ecopy: unable to start verification workers\n");
        return -1;
    }
    if (pthread_create(&g_feeder_thread, NULL, verify_feeder_main, NULL) != 0) {
        pthread_mutex_lock(&g_pipe_pool.lock);
        g_pipe_pool.stop = 1;
        pthread_cond_broadcast(&g_pipe_pool.work_cv);
        pthread_mutex_unlock(&g_pipe_pool.lock);
        verify_pool_finish(&g_pipe_pool);
        return -1;
    }
    g_feeder_started = 1;
    return 0;
}

int verify_pipeline_finish(int remote, int include_dirs)
{
    verify_item_t *dirs;
    verify_arena_chunk_t *arena;
    int failed;

    if (!g_feeder_started) return 0;

    /* Stop the feeder: it drains any remaining file items and flushes its final
     * generation (materialized already by the sshx_flush() in main). */
    pthread_mutex_lock(&g_queue_lock);
    g_feeder_stop = 1;
    pthread_cond_signal(&g_queue_cv);
    pthread_mutex_unlock(&g_queue_lock);
    pthread_join(g_feeder_thread, NULL);
    g_feeder_started = 0;

    failed = g_feeder_failed;

    pthread_mutex_lock(&g_queue_lock);
    dirs = g_dir_head;
    g_dir_head = g_dir_tail = NULL;
    stats_set_verify_pending_peak(g_pending_peak);
    pthread_mutex_unlock(&g_queue_lock);

    /* Directory metadata is now finalized and flushed; release those items. */
    while (dirs) {
        verify_item_t *next = dirs->next;
        dirs->next = NULL;
        if (include_dirs) {
            if (verify_pool_submit(&g_pipe_pool, dirs) != 0) failed = 1;
        }
        /* Arena-owned items need no free; the arena is released below. */
        dirs = next;
    }

    if (verify_pool_finish(&g_pipe_pool) != 0) failed = 1;
    if (remote && sshx_barrier_all(0) != 0) failed = 1;

    pthread_mutex_lock(&g_queue_lock);
    arena = g_arena_head;
    g_head = g_tail = NULL;
    g_arena_head = g_arena_tail = NULL;
    g_pending_count = 0;
    g_pending_peak = 0;
    pthread_mutex_unlock(&g_queue_lock);
    free_arena(arena);

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
    if (remote && sshx_barrier_all(0) != 0) {
        failed = 1;
    }
    return failed ? -1 : 0;
}

void verify_queue_clear(void)
{
    verify_item_t *item;
    verify_arena_chunk_t *arena;
    pthread_mutex_lock(&g_queue_lock);
    item = g_head;
    arena = g_arena_head;
    g_head = g_tail = NULL;
    g_arena_head = g_arena_tail = NULL;
    g_pending_count = 0;
    g_pending_peak = 0;
    pthread_mutex_unlock(&g_queue_lock);
    while (item) {
        verify_item_t *next = item->next;
        free_item(item);
        item = next;
    }
    free_arena(arena);
}
