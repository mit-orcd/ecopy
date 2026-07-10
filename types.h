/*
 * types.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

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

typedef struct dir_handle {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    int src_fd;
    int dst_fd;
    mode_t src_mode;       /* source directory mode; used by remote lazy mkdir */
    unsigned int refs;
    pthread_mutex_t lock;
} dir_handle_t;

typedef struct {
    uint64_t files_seen;
    uint64_t files_copied;
    uint64_t files_skipped;
    uint64_t dirs_seen;
    uint64_t dirs_created;
    uint64_t bytes_copied;
    uint64_t bytes_skipped;
    uint64_t copy_file_range_calls;
    uint64_t copy_file_range_bytes;
    uint64_t copy_file_range_fallbacks;
    uint64_t planned_copy_bytes;
    uint64_t read_direct_opens;
    uint64_t read_buffered_opens;
    uint64_t write_direct_opens;
    uint64_t write_buffered_opens;
    uint64_t queue_wait_ns;
    uint64_t read_syscalls;
    uint64_t read_ns;
    uint64_t write_syscalls;
    uint64_t write_ns;
    uint64_t copy_file_range_syscalls;
    uint64_t copy_file_range_ns;
    uint64_t large_chunk_buffer_allocs;
    uint64_t reader_buffer_wait_ns;
    uint64_t reader_buffer_waits;
    uint64_t writer_data_wait_ns;
    uint64_t writer_data_waits;
    uint64_t ready_queue_peak;
    uint64_t ready_queue_samples;
    uint64_t ready_queue_total;
    uint64_t metadata_warnings;
    uint64_t metadata_errors;
    int traversal_done;
    int file_work_drained;
    int finalize_done;
    int shutdown_done;
    struct timespec start_ts;
    struct timespec traversal_done_ts;
    struct timespec file_work_drained_ts;
    struct timespec finalize_done_ts;
    struct timespec shutdown_done_ts;
} stats_t;

typedef struct {
    struct timespec ts;
    uint64_t bytes_copied;
    uint64_t bytes_completed;
    uint64_t files_completed;
    int valid;
} speed_sample_t;

typedef struct file_task {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    char name[PATH_MAX];
    dir_handle_t *dir;
    struct stat src_st;
    struct file_task *next;
} file_task_t;

typedef struct {
    uint64_t bytes_copied;
    uint64_t bytes_completed;
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
    double rolling_completed_gibs;
    double rolling_files_per_sec;
    double elapsed_sec;
} progress_snapshot_t;

#endif
