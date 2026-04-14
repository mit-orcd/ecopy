#ifndef WORKERS_H
#define WORKERS_H

#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

int workers_start(void);
int workers_max_workers(void);
int workers_large_workers(void);
int workers_large_file_inflight(void);
int workers_max_active_large_files(void);
int workers_chunk_mb(void);
int workers_large_threshold_mb(void);
int workers_file_is_large(off_t size);
void workers_print_runtime_summary(void);
void workers_print_startup_config(void);
void workers_stop(void);
int workers_status(void);
int workers_enqueue_small_file(const char *src, const char *dst, const struct stat *src_st);
int workers_enqueue_large_file(const char *src, const char *dst, const struct stat *src_st);
uint64_t workers_small_queue_depth(void);
uint64_t workers_small_active_count(void);
uint64_t workers_large_queue_depth(void);
uint64_t workers_large_active_count(void);

#endif
