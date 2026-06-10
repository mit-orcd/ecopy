/*
 * workers.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef WORKERS_H
#define WORKERS_H

#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "types.h"

int workers_start(void);
/*
 * Enable collection of the blocking-wait timing counters (reader buffer waits,
 * writer data waits, queue waits). These feed verbose-only diagnostics, and the
 * clock_gettime() calls around every park/wake are costly under contention, so
 * they default to off and main enables them only for verbose runs.
 */
void workers_set_collect_wait_timing(int on);
int workers_max_workers(void);
int workers_large_workers(void);
int workers_large_file_inflight(void);
int workers_max_active_large_files(void);
int workers_chunk_mb(void);
int workers_large_threshold_mb(void);
int workers_traversal_workers(void);
int workers_max_queued_files(void);
int workers_small_worker_limit(void);
int workers_file_is_large(off_t size);
int workers_file_is_sparse(const struct stat *st);
void workers_print_runtime_summary(void);
void workers_print_startup_config(void);
void workers_stop(void);
int workers_status(void);
int workers_enqueue_small_file(dir_handle_t *dir,
                               const char *name,
                               const char *src,
                               const char *dst,
                               const struct stat *src_st);
int workers_enqueue_large_file(dir_handle_t *dir,
                               const char *name,
                               const char *src,
                               const char *dst,
                               const struct stat *src_st);
uint64_t workers_small_queue_depth(void);
uint64_t workers_small_active_count(void);
uint64_t workers_large_queue_depth(void);
uint64_t workers_large_active_count(void);

#endif
