#ifndef WORKERS_H
#define WORKERS_H

#include <stdint.h>
#include <sys/stat.h>

int workers_start(void);
void workers_stop(void);
int workers_status(void);
int workers_enqueue_small_file(const char *src, const char *dst, const struct stat *src_st);
int workers_enqueue_large_file(const char *src, const char *dst, const struct stat *src_st);
uint64_t workers_small_queue_depth(void);
uint64_t workers_small_active_count(void);
uint64_t workers_large_queue_depth(void);
uint64_t workers_large_active_count(void);

#endif
