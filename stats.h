/*
 * stats.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef STATS_H
#define STATS_H

#include <stdint.h>
#include "types.h"

void stats_init(void);
void stats_add_bytes(uint64_t bytes);
void stats_add_skipped_bytes(uint64_t bytes);
void stats_add_planned_copy_bytes(uint64_t bytes);
void stats_set_traversal_done(void);
void stats_set_file_work_drained(void);
void stats_set_finalize_done(void);
void stats_set_shutdown_done(void);
void stats_record_read_open(int used_direct);
void stats_record_write_open(int used_direct);
void stats_record_queue_wait_ns(uint64_t ns);
void stats_record_read_io(uint64_t ns);
void stats_record_write_io(uint64_t ns);
/*
 * Split variants for the hot large-file path, where I/O timing is sampled:
 * the _op functions count every syscall, while the _time functions fold in an
 * (already extrapolated) duration only on sampled iterations. This keeps
 * clock_gettime() off most iterations without losing the syscall count.
 */
void stats_record_read_op(void);
void stats_record_write_op(void);
void stats_record_read_time(uint64_t ns);
void stats_record_write_time(uint64_t ns);
void stats_record_copy_file_range_io(uint64_t ns);
void stats_record_large_chunk_buffer_alloc(void);
void stats_record_reader_buffer_wait_ns(uint64_t ns);
void stats_record_writer_data_wait_ns(uint64_t ns);
void stats_record_ready_queue_depth(uint64_t depth);
void stats_inc_files_seen(void);
void stats_inc_files_copied(void);
void stats_inc_files_skipped(void);
void stats_inc_dirs_seen(void);
void stats_inc_dirs_created(void);
void stats_record_copy_file_range_call(uint64_t bytes);
void stats_record_copy_file_range_fallback(void);
void stats_add_copy_file_range_usage(uint64_t calls, uint64_t bytes, uint64_t fallbacks);
void stats_inc_metadata_warning(void);
void stats_inc_metadata_error(void);
void stats_set_verify_config(int metadata, int data, double percent, uint64_t seed);
void stats_record_verify(uint64_t bytes, uint64_t scope_bytes, uint64_t blocks,
                         int metadata_checked,
                         int data_mismatch, int metadata_mismatch, int failed);
void stats_mark_verify_failure(void);
void stats_add_verify_ns(uint64_t ns);

void stats_set_current_file(const char *path, uint64_t total, int parallel);
void stats_advance_current_file(uint64_t bytes);
void stats_clear_current_file(const char *path);

void stats_record_speed_sample(void);
double stats_elapsed_sec(void);
double stats_traversal_elapsed_sec(void);
double stats_file_work_drained_elapsed_sec(void);
double stats_finalize_elapsed_sec(void);
double stats_shutdown_elapsed_sec(void);
double stats_avg_gibs(void);
double stats_rolling_gibs(void);
void stats_get_progress_snapshot(progress_snapshot_t *snap);
void stats_get_final(stats_t *out);
void stats_print_final(int verbose);
double stats_bytes_to_gib(uint64_t bytes);

#endif
