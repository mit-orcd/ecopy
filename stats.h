#ifndef STATS_H
#define STATS_H

#include <stdint.h>
#include "types.h"

void stats_init(void);
void stats_add_bytes(uint64_t bytes);
void stats_add_planned_copy_bytes(uint64_t bytes);
void stats_set_traversal_done(void);
void stats_inc_files_seen(void);
void stats_inc_files_copied(void);
void stats_inc_files_skipped(void);
void stats_inc_dirs_seen(void);
void stats_inc_dirs_created(void);
void stats_record_copy_file_range_call(uint64_t bytes);
void stats_record_copy_file_range_fallback(void);
void stats_add_copy_file_range_usage(uint64_t calls, uint64_t bytes, uint64_t fallbacks);

void stats_begin_current_file(const char *path, uint64_t total_bytes, int is_parallel);
void stats_advance_current_file(uint64_t bytes);
void stats_end_current_file(void);

void stats_set_active_parallel_ctx(parallel_ctx_t *ctx);
void stats_clear_active_parallel_ctx(void);
int stats_copy_active_parallel_workers(chunk_worker_stat_t *out, int max_workers);

void stats_record_speed_sample(void);
double stats_elapsed_sec(void);
double stats_avg_gibs(void);
double stats_rolling_gibs(void);
void stats_get_progress_snapshot(progress_snapshot_t *snap);
void stats_print_final(void);
double stats_bytes_to_gib(uint64_t bytes);

#endif
