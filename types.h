#ifndef TYPES_H
#define TYPES_H

#include <stdint.h>
#include <limits.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
    uint64_t files_seen;
    uint64_t files_copied;
    uint64_t files_skipped;
    uint64_t dirs_seen;
    uint64_t dirs_created;
    uint64_t bytes_copied;
    uint64_t copy_file_range_calls;
    uint64_t copy_file_range_bytes;
    uint64_t copy_file_range_fallbacks;
    uint64_t planned_copy_bytes;
    int traversal_done;
    struct timespec start_ts;
} stats_t;

typedef struct {
    struct timespec ts;
    uint64_t bytes_copied;
    int valid;
} speed_sample_t;

typedef struct {
    uint64_t bytes_done;
    uint64_t chunks_done;
    uint64_t copy_file_range_calls;
    uint64_t copy_file_range_bytes;
    uint64_t copy_file_range_fallbacks;
    int active;
} chunk_worker_stat_t;

typedef struct parallel_ctx {
    int fd_in;
    int fd_out;
    off_t file_size;
    off_t bulk_end;
    off_t next_offset;
    pthread_mutex_t offset_lock;
    pthread_mutex_t error_lock;
    int error;
    int worker_count;
    chunk_worker_stat_t *workers;
} parallel_ctx_t;


typedef struct {
    parallel_ctx_t *ctx;
    int worker_id;
} chunk_worker_arg_t;

typedef struct file_task {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    struct stat src_st;
    struct file_task *next;
} file_task_t;

typedef struct {
    uint64_t bytes_copied;
    uint64_t files_seen;
    uint64_t files_copied;
    uint64_t files_skipped;
    uint64_t dirs_seen;
    uint64_t dirs_created;
    uint64_t planned_copy_bytes;
    int traversal_done;
    uint64_t current_file_done;
    uint64_t current_file_total;
    int current_file_parallel;
    char current_file[PATH_MAX];
    double rolling_gibs;
    double elapsed_sec;
} progress_snapshot_t;

#endif
