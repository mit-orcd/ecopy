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
    uint8_t digest[VERIFY_DIGEST_SIZE];
} verify_digest_t;

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
                      const struct stat *src_st, int skipped);
int verify_queue_directory(const char *src, const char *dst,
                           const struct stat *src_st);
int verify_retarget_path(const char *old_dst, const char *new_dst);
int verify_run_queued(int remote);
int verify_run_tree(const char *src, const char *dst, int remote,
                    int source_is_dir);
void verify_queue_clear(void);
int verify_worker_count(void);
uint64_t verify_queue_depth(void);
uint64_t verify_active_count(void);

int verify_metadata_stat(const struct stat *expected,
                         const struct stat *actual,
                         int is_dir,
                         const char *path);
int verify_metadata_path(const char *path, const struct stat *expected,
                         int is_dir);

#endif
