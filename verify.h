/*
 * Transfer verification configuration and queue.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */
#ifndef VERIFY_H
#define VERIFY_H

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>

#define VERIFY_BLOCK_SIZE 4096u
#define VERIFY_DIGEST_SIZE 32u
#define VERIFY_BATCH_MAX 256u

typedef struct {
    int64_t offset;
    uint32_t length;
    uint8_t flags;
    uint8_t digest[VERIFY_DIGEST_SIZE];
} verify_digest_t;

#define VERIFY_SAMPLE_EXPECT_ZERO 0x1u

void verify_configure(int metadata, int data, double percent,
                      int include_skipped, uint64_t seed, int seed_set,
                      int workers);
int verify_enabled(void);
int verify_metadata_enabled(void);
int verify_data_enabled(void);
int verify_include_skipped(void);
double verify_percent(void);
uint64_t verify_seed(void);

int verify_queue_file(const char *src, const char *dst,
                      const struct stat *src_st, int skipped, int durable);
int verify_queue_directory(const char *src, const char *dst,
                           const struct stat *src_st);
int verify_retarget_path(const char *old_dst, const char *new_dst);
int verify_run_queued(int remote);

/*
 * Pipelined verification: overlap the verify phase with copy. Start the verify
 * worker pool and a feeder thread at copy start; copy workers enqueue files via
 * verify_queue_file() as they finish. Durable items (local temp+rename, remote
 * streamed COMMIT, skipped) are submitted immediately; non-durable remote
 * fire-and-forget PUTFILE items are held and released in barrier-gated
 * generations once materialized. Directory-metadata items are always released
 * in verify_pipeline_finish() (after traversal_finalize_metadata).
 */
int verify_pipeline_start(int remote);
int verify_pipeline_finish(int remote, int include_dirs);
int verify_run_tree(const char *src, const char *dst, int remote,
                    int source_is_dir);
void verify_queue_clear(void);
int verify_worker_count(void);
uint64_t verify_queue_depth(void);
uint64_t verify_active_count(void);

typedef enum {
    VERIFY_META_OK = 0,        /* source and target metadata match */
    VERIFY_META_MISMATCH = 1,  /* genuine mismatch: type/size/mode/times, or
                                  ownership while privileged */
    VERIFY_META_OWNERSHIP = 2  /* only uid/gid differ and this process cannot
                                  chown; a warning, never a failure */
} verify_meta_class_t;

/*
 * True when the running process cannot change file ownership, so a uid/gid
 * difference could never have been preserved and must not be treated as a
 * verification failure.
 */
int verify_ownership_unpreservable(void);

verify_meta_class_t verify_metadata_stat(const struct stat *expected,
                                         const struct stat *actual,
                                         int is_dir,
                                         const char *path);
int verify_metadata_path(const char *path, const struct stat *expected,
                         int is_dir);

#endif
