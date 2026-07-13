/*
 * stats.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "stats.h"
#include "config.h"

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <inttypes.h>
#include <stdatomic.h>

static stats_t g_stats;
static speed_sample_t g_speed_ring[SPEED_SLOTS];
static int g_speed_index = 0;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_current_file[PATH_MAX];
static uint64_t g_current_file_total = 0;
static int g_current_file_parallel = 0;

/*
 * Hot per-chunk counters live outside g_stats as lock-free atomics so that
 * reader/writer threads never serialize on g_lock for every I/O. They are
 * materialized back into a stats_t (via stats_load_hot) only when a snapshot
 * or the final report is taken.
 */
static _Atomic uint64_t a_bytes_copied;
static _Atomic uint64_t a_current_file_done;
static _Atomic uint64_t a_read_syscalls;
static _Atomic uint64_t a_read_ns;
static _Atomic uint64_t a_write_syscalls;
static _Atomic uint64_t a_write_ns;
static _Atomic uint64_t a_reader_buffer_waits;
static _Atomic uint64_t a_reader_buffer_wait_ns;
static _Atomic uint64_t a_writer_data_waits;
static _Atomic uint64_t a_writer_data_wait_ns;
static _Atomic uint64_t a_ready_queue_peak;
static _Atomic uint64_t a_ready_queue_total;
static _Atomic uint64_t a_ready_queue_samples;

static uint64_t hot_load(_Atomic uint64_t *p) {
    return atomic_load_explicit(p, memory_order_relaxed);
}

static void hot_add(_Atomic uint64_t *p, uint64_t v) {
    atomic_fetch_add_explicit(p, v, memory_order_relaxed);
}

/* Copy the lock-free hot counters into a stats_t snapshot. */
static void stats_load_hot(stats_t *s) {
    s->bytes_copied = hot_load(&a_bytes_copied);
    s->read_syscalls = hot_load(&a_read_syscalls);
    s->read_ns = hot_load(&a_read_ns);
    s->write_syscalls = hot_load(&a_write_syscalls);
    s->write_ns = hot_load(&a_write_ns);
    s->reader_buffer_waits = hot_load(&a_reader_buffer_waits);
    s->reader_buffer_wait_ns = hot_load(&a_reader_buffer_wait_ns);
    s->writer_data_waits = hot_load(&a_writer_data_waits);
    s->writer_data_wait_ns = hot_load(&a_writer_data_wait_ns);
    s->ready_queue_peak = hot_load(&a_ready_queue_peak);
    s->ready_queue_total = hot_load(&a_ready_queue_total);
    s->ready_queue_samples = hot_load(&a_ready_queue_samples);
}

static double ts_to_sec(const struct timespec *ts) {
    return (double)ts->tv_sec + (double)ts->tv_nsec / 1e9;
}

static double diff_sec(const struct timespec *a, const struct timespec *b) {
    return ts_to_sec(a) - ts_to_sec(b);
}

double stats_bytes_to_gib(uint64_t bytes) {
    return (double)bytes / (1024.0 * 1024.0 * 1024.0);
}

void stats_init(void) {
    pthread_mutex_lock(&g_lock);
    memset(&g_stats, 0, sizeof(g_stats));
    memset(g_speed_ring, 0, sizeof(g_speed_ring));
    memset(g_current_file, 0, sizeof(g_current_file));
    g_current_file_total = 0;
    g_current_file_parallel = 0;
    g_speed_index = 0;
    atomic_store_explicit(&a_bytes_copied, 0, memory_order_relaxed);
    atomic_store_explicit(&a_current_file_done, 0, memory_order_relaxed);
    atomic_store_explicit(&a_read_syscalls, 0, memory_order_relaxed);
    atomic_store_explicit(&a_read_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&a_write_syscalls, 0, memory_order_relaxed);
    atomic_store_explicit(&a_write_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&a_reader_buffer_waits, 0, memory_order_relaxed);
    atomic_store_explicit(&a_reader_buffer_wait_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&a_writer_data_waits, 0, memory_order_relaxed);
    atomic_store_explicit(&a_writer_data_wait_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&a_ready_queue_peak, 0, memory_order_relaxed);
    atomic_store_explicit(&a_ready_queue_total, 0, memory_order_relaxed);
    atomic_store_explicit(&a_ready_queue_samples, 0, memory_order_relaxed);
    clock_gettime(CLOCK_MONOTONIC, &g_stats.start_ts);
    pthread_mutex_unlock(&g_lock);
}

void stats_add_bytes(uint64_t bytes) {
    hot_add(&a_bytes_copied, bytes);
}

void stats_add_skipped_bytes(uint64_t bytes) {
    pthread_mutex_lock(&g_lock);
    g_stats.bytes_skipped += bytes;
    pthread_mutex_unlock(&g_lock);
}

void stats_add_planned_copy_bytes(uint64_t bytes) {
    pthread_mutex_lock(&g_lock);
    g_stats.planned_copy_bytes += bytes;
    pthread_mutex_unlock(&g_lock);
}

void stats_set_traversal_done(void) {
    pthread_mutex_lock(&g_lock);
    g_stats.traversal_done = 1;
    clock_gettime(CLOCK_MONOTONIC, &g_stats.traversal_done_ts);
    pthread_mutex_unlock(&g_lock);
}

void stats_set_file_work_drained(void) {
    pthread_mutex_lock(&g_lock);
    g_stats.file_work_drained = 1;
    clock_gettime(CLOCK_MONOTONIC, &g_stats.file_work_drained_ts);
    pthread_mutex_unlock(&g_lock);
}

void stats_set_finalize_done(void) {
    pthread_mutex_lock(&g_lock);
    g_stats.finalize_done = 1;
    clock_gettime(CLOCK_MONOTONIC, &g_stats.finalize_done_ts);
    pthread_mutex_unlock(&g_lock);
}

void stats_set_shutdown_done(void) {
    pthread_mutex_lock(&g_lock);
    g_stats.shutdown_done = 1;
    clock_gettime(CLOCK_MONOTONIC, &g_stats.shutdown_done_ts);
    pthread_mutex_unlock(&g_lock);
}

void stats_record_read_open(int used_direct) {
    pthread_mutex_lock(&g_lock);
    if (used_direct) {
        g_stats.read_direct_opens++;
    } else {
        g_stats.read_buffered_opens++;
    }
    pthread_mutex_unlock(&g_lock);
}

void stats_record_write_open(int used_direct) {
    pthread_mutex_lock(&g_lock);
    if (used_direct) {
        g_stats.write_direct_opens++;
    } else {
        g_stats.write_buffered_opens++;
    }
    pthread_mutex_unlock(&g_lock);
}

void stats_record_queue_wait_ns(uint64_t ns) { pthread_mutex_lock(&g_lock); g_stats.queue_wait_ns += ns; pthread_mutex_unlock(&g_lock); }
void stats_record_read_io(uint64_t ns) { hot_add(&a_read_syscalls, 1); hot_add(&a_read_ns, ns); }
void stats_record_write_io(uint64_t ns) { hot_add(&a_write_syscalls, 1); hot_add(&a_write_ns, ns); }
void stats_record_read_op(void) { hot_add(&a_read_syscalls, 1); }
void stats_record_write_op(void) { hot_add(&a_write_syscalls, 1); }
void stats_record_read_time(uint64_t ns) { hot_add(&a_read_ns, ns); }
void stats_record_write_time(uint64_t ns) { hot_add(&a_write_ns, ns); }
void stats_record_copy_file_range_io(uint64_t ns) { pthread_mutex_lock(&g_lock); g_stats.copy_file_range_syscalls++; g_stats.copy_file_range_ns += ns; pthread_mutex_unlock(&g_lock); }
void stats_record_large_chunk_buffer_alloc(void) { pthread_mutex_lock(&g_lock); g_stats.large_chunk_buffer_allocs++; pthread_mutex_unlock(&g_lock); }
void stats_record_reader_buffer_wait_ns(uint64_t ns) { hot_add(&a_reader_buffer_wait_ns, ns); hot_add(&a_reader_buffer_waits, 1); }
void stats_record_writer_data_wait_ns(uint64_t ns) { hot_add(&a_writer_data_wait_ns, ns); hot_add(&a_writer_data_waits, 1); }
void stats_record_ready_queue_depth(uint64_t depth) {
    hot_add(&a_ready_queue_total, depth);
    hot_add(&a_ready_queue_samples, 1);
    uint64_t cur = atomic_load_explicit(&a_ready_queue_peak, memory_order_relaxed);
    while (depth > cur &&
           !atomic_compare_exchange_weak_explicit(&a_ready_queue_peak, &cur, depth,
                                                  memory_order_relaxed, memory_order_relaxed)) {
        /* cur reloaded with the current peak on failure; retry. */
    }
}
void stats_inc_files_seen(void){ pthread_mutex_lock(&g_lock); g_stats.files_seen++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_files_copied(void){ pthread_mutex_lock(&g_lock); g_stats.files_copied++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_files_skipped(void){ pthread_mutex_lock(&g_lock); g_stats.files_skipped++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_dirs_seen(void){ pthread_mutex_lock(&g_lock); g_stats.dirs_seen++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_dirs_created(void){ pthread_mutex_lock(&g_lock); g_stats.dirs_created++; pthread_mutex_unlock(&g_lock);} 
void stats_record_copy_file_range_call(uint64_t bytes) { pthread_mutex_lock(&g_lock); g_stats.copy_file_range_calls++; g_stats.copy_file_range_bytes += bytes; pthread_mutex_unlock(&g_lock); }
void stats_record_copy_file_range_fallback(void) { pthread_mutex_lock(&g_lock); g_stats.copy_file_range_fallbacks++; pthread_mutex_unlock(&g_lock); }
void stats_add_copy_file_range_usage(uint64_t calls, uint64_t bytes, uint64_t fallbacks) { pthread_mutex_lock(&g_lock); g_stats.copy_file_range_calls += calls; g_stats.copy_file_range_bytes += bytes; g_stats.copy_file_range_fallbacks += fallbacks; pthread_mutex_unlock(&g_lock); } 
void stats_inc_metadata_warning(void) { pthread_mutex_lock(&g_lock); g_stats.metadata_warnings++; pthread_mutex_unlock(&g_lock); }
void stats_inc_metadata_error(void) { pthread_mutex_lock(&g_lock); g_stats.metadata_errors++; pthread_mutex_unlock(&g_lock); }
void stats_set_verify_config(int metadata, int data, double percent, uint64_t seed) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_metadata_enabled = metadata;
    g_stats.verify_data_enabled = data;
    g_stats.verify_requested_percent = percent;
    g_stats.verify_seed = seed;
    pthread_mutex_unlock(&g_lock);
}
void stats_record_verify(uint64_t bytes, uint64_t scope_bytes, uint64_t blocks,
                         int metadata_checked,
                         int data_mismatch, int metadata_mismatch, int failed) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_objects++;
    g_stats.verify_bytes += bytes;
    g_stats.verify_scope_bytes += scope_bytes;
    g_stats.verify_blocks += blocks;
    if (metadata_checked) g_stats.verify_metadata_objects++;
    if (data_mismatch) g_stats.verify_data_mismatches++;
    if (metadata_mismatch) g_stats.verify_metadata_mismatches++;
    if (failed) g_stats.verify_failures++;
    pthread_mutex_unlock(&g_lock);
}
void stats_mark_verify_failure(void) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_failures++;
    pthread_mutex_unlock(&g_lock);
}
void stats_add_verify_ns(uint64_t ns) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_ns += ns;
    pthread_mutex_unlock(&g_lock);
}

void stats_set_current_file(const char *path, uint64_t total, int parallel) {
    /* Plain bounded copy: snprintf("%s") here pulled in the vfprintf machinery
     * on every file, which was a visible cost on small-file trees. */
    size_t n = path ? strlen(path) : 0;
    pthread_mutex_lock(&g_lock);
    if (n >= sizeof(g_current_file)) {
        n = sizeof(g_current_file) - 1;
    }
    if (n > 0) {
        memcpy(g_current_file, path, n);
    }
    g_current_file[n] = '\0';
    atomic_store_explicit(&a_current_file_done, 0, memory_order_relaxed);
    g_current_file_total = total;
    g_current_file_parallel = parallel;
    pthread_mutex_unlock(&g_lock);
}

void stats_advance_current_file(uint64_t bytes) {
    hot_add(&a_current_file_done, bytes);
    hot_add(&a_bytes_copied, bytes);
}

void stats_clear_current_file(const char *path) {
    pthread_mutex_lock(&g_lock);
    if (!path || strcmp(g_current_file, path) == 0) {
        memset(g_current_file, 0, sizeof(g_current_file));
        atomic_store_explicit(&a_current_file_done, 0, memory_order_relaxed);
        g_current_file_total = 0;
        g_current_file_parallel = 0;
    }
    pthread_mutex_unlock(&g_lock);
}

void stats_record_speed_sample(void) {
    uint64_t bytes_copied = hot_load(&a_bytes_copied);
    pthread_mutex_lock(&g_lock);
    clock_gettime(CLOCK_MONOTONIC, &g_speed_ring[g_speed_index].ts);
    g_speed_ring[g_speed_index].bytes_copied = bytes_copied;
    g_speed_ring[g_speed_index].bytes_completed = bytes_copied + g_stats.bytes_skipped;
    g_speed_ring[g_speed_index].files_completed = g_stats.files_copied + g_stats.files_skipped;
    g_speed_ring[g_speed_index].valid = 1;
    g_speed_index = (g_speed_index + 1) % SPEED_SLOTS;
    pthread_mutex_unlock(&g_lock);
}

double stats_elapsed_sec(void) {
    struct timespec now, start;
    clock_gettime(CLOCK_MONOTONIC, &now);
    pthread_mutex_lock(&g_lock);
    start = g_stats.start_ts;
    pthread_mutex_unlock(&g_lock);
    return ts_to_sec(&now) - ts_to_sec(&start);
}

double stats_traversal_elapsed_sec(void) {
    struct timespec start, done;
    int traversal_done;

    pthread_mutex_lock(&g_lock);
    start = g_stats.start_ts;
    done = g_stats.traversal_done_ts;
    traversal_done = g_stats.traversal_done;
    pthread_mutex_unlock(&g_lock);

    if (!traversal_done) {
        return 0.0;
    }
    return ts_to_sec(&done) - ts_to_sec(&start);
}

double stats_file_work_drained_elapsed_sec(void) {
    struct timespec start, done;
    int file_work_drained;

    pthread_mutex_lock(&g_lock);
    start = g_stats.start_ts;
    done = g_stats.file_work_drained_ts;
    file_work_drained = g_stats.file_work_drained;
    pthread_mutex_unlock(&g_lock);

    if (!file_work_drained) {
        return 0.0;
    }
    return ts_to_sec(&done) - ts_to_sec(&start);
}

double stats_finalize_elapsed_sec(void) {
    struct timespec start, done;
    int file_work_drained;

    pthread_mutex_lock(&g_lock);
    start = g_stats.file_work_drained_ts;
    done = g_stats.finalize_done_ts;
    file_work_drained = g_stats.file_work_drained;
    pthread_mutex_unlock(&g_lock);

    if (!file_work_drained || done.tv_sec == 0) {
        return 0.0;
    }
    return ts_to_sec(&done) - ts_to_sec(&start);
}

double stats_shutdown_elapsed_sec(void) {
    struct timespec start, done;
    int finalize_done;

    pthread_mutex_lock(&g_lock);
    start = g_stats.finalize_done_ts;
    done = g_stats.shutdown_done_ts;
    finalize_done = g_stats.finalize_done;
    pthread_mutex_unlock(&g_lock);

    if (!finalize_done || done.tv_sec == 0) {
        return 0.0;
    }
    return ts_to_sec(&done) - ts_to_sec(&start);
}

double stats_avg_gibs(void) {
    double elapsed = stats_elapsed_sec();
    uint64_t bytes = hot_load(&a_bytes_copied);
    return elapsed > 0.0 ? stats_bytes_to_gib(bytes) / elapsed : 0.0;
}

double stats_rolling_gibs(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    pthread_mutex_lock(&g_lock);
    int found = 0;
    double best_age = 0.0;
    uint64_t old_bytes = 0;
    struct timespec old_ts = now;
    for (int i = 0; i < SPEED_SLOTS; i++) {
        if (!g_speed_ring[i].valid) continue;
        double age = diff_sec(&now, &g_speed_ring[i].ts);
        if (age < 0.0 || age > SPEED_WINDOW_SEC) continue;
        if (!found || age > best_age) {
            found = 1; best_age = age; old_bytes = g_speed_ring[i].bytes_copied; old_ts = g_speed_ring[i].ts;
        }
    }
    uint64_t cur_bytes = hot_load(&a_bytes_copied);
    pthread_mutex_unlock(&g_lock);
    if (!found) return 0.0;
    double dt = diff_sec(&now, &old_ts);
    if (dt <= 0.0) return 0.0;
    return stats_bytes_to_gib(cur_bytes - old_bytes) / dt;
}

static double stats_rolling_completed_gibs(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    pthread_mutex_lock(&g_lock);
    int found = 0;
    double best_age = 0.0;
    uint64_t old_bytes = 0;
    struct timespec old_ts = now;
    for (int i = 0; i < SPEED_SLOTS; i++) {
        if (!g_speed_ring[i].valid) continue;
        double age = diff_sec(&now, &g_speed_ring[i].ts);
        if (age < 0.0 || age > SPEED_WINDOW_SEC) continue;
        if (!found || age > best_age) {
            found = 1; best_age = age; old_bytes = g_speed_ring[i].bytes_completed; old_ts = g_speed_ring[i].ts;
        }
    }
    uint64_t cur_bytes = hot_load(&a_bytes_copied) + g_stats.bytes_skipped;
    pthread_mutex_unlock(&g_lock);
    if (!found) return 0.0;
    double dt = diff_sec(&now, &old_ts);
    if (dt <= 0.0) return 0.0;
    return stats_bytes_to_gib(cur_bytes - old_bytes) / dt;
}

static double stats_rolling_files_per_sec(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    pthread_mutex_lock(&g_lock);
    int found = 0;
    double best_age = 0.0;
    uint64_t old_files = 0;
    struct timespec old_ts = now;
    for (int i = 0; i < SPEED_SLOTS; i++) {
        if (!g_speed_ring[i].valid) continue;
        double age = diff_sec(&now, &g_speed_ring[i].ts);
        if (age < 0.0 || age > SPEED_WINDOW_SEC) continue;
        if (!found || age > best_age) {
            found = 1; best_age = age; old_files = g_speed_ring[i].files_completed; old_ts = g_speed_ring[i].ts;
        }
    }
    uint64_t cur_files = g_stats.files_copied + g_stats.files_skipped;
    pthread_mutex_unlock(&g_lock);
    if (!found) return 0.0;
    double dt = diff_sec(&now, &old_ts);
    if (dt <= 0.0) return 0.0;
    return (double)(cur_files - old_files) / dt;
}

void stats_get_progress_snapshot(progress_snapshot_t *snap) {
    if (!snap) return;
    uint64_t bytes_copied = hot_load(&a_bytes_copied);
    uint64_t current_file_done = hot_load(&a_current_file_done);
    pthread_mutex_lock(&g_lock);
    snap->bytes_copied = bytes_copied;
    snap->bytes_completed = bytes_copied + g_stats.bytes_skipped;
    snap->files_seen = g_stats.files_seen;
    snap->files_copied = g_stats.files_copied;
    snap->files_skipped = g_stats.files_skipped;
    snap->dirs_seen = g_stats.dirs_seen;
    snap->dirs_created = g_stats.dirs_created;
    snap->planned_copy_bytes = g_stats.planned_copy_bytes;
    snap->traversal_done = g_stats.traversal_done;
    snap->current_file_done = current_file_done;
    snap->current_file_total = g_current_file_total;
    snap->current_file_parallel = g_current_file_parallel;
    snprintf(snap->current_file, sizeof(snap->current_file), "%s", g_current_file);
    pthread_mutex_unlock(&g_lock);
    snap->rolling_gibs = stats_rolling_gibs();
    snap->rolling_completed_gibs = stats_rolling_completed_gibs();
    snap->rolling_files_per_sec = stats_rolling_files_per_sec();
    snap->elapsed_sec = stats_elapsed_sec();
}


void stats_get_final(stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&g_lock);
    *out = g_stats;
    pthread_mutex_unlock(&g_lock);
    stats_load_hot(out);
}

void stats_print_final(int verbose) {
    stats_t s;
    pthread_mutex_lock(&g_lock);
    s = g_stats;
    pthread_mutex_unlock(&g_lock);
    stats_load_hot(&s);
    double sec = s.shutdown_done ? ts_to_sec(&s.shutdown_done_ts) - ts_to_sec(&s.start_ts)
                                 : stats_elapsed_sec();
    double gib = stats_bytes_to_gib(s.bytes_copied);
    double avg = sec > 0.0 ? gib / sec : 0.0;
    printf("Done.\n");
    printf("Files seen    : %" PRIu64 "\n", s.files_seen);
    printf("Files copied  : %" PRIu64 "\n", s.files_copied);
    printf("Files skipped : %" PRIu64 "\n", s.files_skipped);
    printf("Dirs seen     : %" PRIu64 "\n", s.dirs_seen);
    printf("Dirs created  : %" PRIu64 "\n", s.dirs_created);
    printf("GiB copied    : %.2f\n", gib);
    if (s.bytes_skipped > 0) {
        printf("GiB skipped   : %.2f\n", stats_bytes_to_gib(s.bytes_skipped));
    }
    printf("Elapsed       : %.2f s\n", sec);
    printf("Avg speed     : %.2f GiB/s\n", avg);
    if (s.metadata_warnings > 0) {
        printf("Metadata warnings : %" PRIu64 "\n", s.metadata_warnings);
    }
    if (s.metadata_errors > 0) {
        printf("Metadata errors   : %" PRIu64 "\n", s.metadata_errors);
    }
    if (s.verify_metadata_enabled || s.verify_data_enabled) {
        double achieved = s.verify_scope_bytes
                              ? 100.0 * (double)s.verify_bytes /
                                    (double)s.verify_scope_bytes
                              : 0.0;
        printf("Verify objects    : %" PRIu64 "\n", s.verify_objects);
        printf("Verify blocks     : %" PRIu64 "\n", s.verify_blocks);
        printf("Verify MiB        : %.2f\n",
               (double)s.verify_bytes / (1024.0 * 1024.0));
        printf("Verify coverage   : %.3f%% requested, %.3f%% achieved\n",
               s.verify_requested_percent, achieved);
        printf("Verify seed       : %" PRIu64 "\n", s.verify_seed);
        printf("Verify seconds    : %.3f\n", (double)s.verify_ns / 1e9);
        printf("Verify failures   : %" PRIu64 "\n", s.verify_failures);
    }

    if (!verbose) {
        return;
    }

    printf("Bytes copied  : %" PRIu64 "\n", s.bytes_copied);
    printf("Bytes skipped : %" PRIu64 "\n", s.bytes_skipped);
    printf("copy_file_range calls     : %" PRIu64 "\n", s.copy_file_range_calls);
    printf("copy_file_range bytes     : %" PRIu64 "\n", s.copy_file_range_bytes);
    printf("copy_file_range fallbacks : %" PRIu64 "\n", s.copy_file_range_fallbacks);
    printf("Read opens   direct      : %" PRIu64 "\n", s.read_direct_opens);
    printf("Read opens   buffered    : %" PRIu64 "\n", s.read_buffered_opens);
    printf("Write opens  direct      : %" PRIu64 "\n", s.write_direct_opens);
    printf("Write opens  buffered    : %" PRIu64 "\n", s.write_buffered_opens);
    printf("Queue wait   seconds     : %.3f\n", (double)s.queue_wait_ns / 1e9);
    printf("Read syscalls            : %" PRIu64 "\n", s.read_syscalls);
    printf("Read time    seconds     : %.3f\n", (double)s.read_ns / 1e9);
    printf("Write syscalls           : %" PRIu64 "\n", s.write_syscalls);
    printf("Write time   seconds     : %.3f\n", (double)s.write_ns / 1e9);
    printf("cfr syscalls             : %" PRIu64 "\n", s.copy_file_range_syscalls);
    printf("cfr time     seconds     : %.3f\n", (double)s.copy_file_range_ns / 1e9);
    printf("Large chunk buffer allocs: %" PRIu64 "\n", s.large_chunk_buffer_allocs);
    printf("Reader buf waits        : %" PRIu64 "\n", s.reader_buffer_waits);
    printf("Reader buf wait seconds : %.3f\n", (double)s.reader_buffer_wait_ns / 1e9);
    printf("Writer data waits       : %" PRIu64 "\n", s.writer_data_waits);
    printf("Writer data wait seconds: %.3f\n", (double)s.writer_data_wait_ns / 1e9);
    printf("Ready queue peak        : %" PRIu64 "\n", s.ready_queue_peak);
    printf("Ready queue avg depth   : %.2f\n", s.ready_queue_samples ? (double)s.ready_queue_total / (double)s.ready_queue_samples : 0.0);
    if (s.traversal_done) {
        double traversal_sec = ts_to_sec(&s.traversal_done_ts) - ts_to_sec(&s.start_ts);
        printf("Traversal seen seconds : %.2f\n", traversal_sec);
    }
    if (s.file_work_drained) {
        double drained_sec = ts_to_sec(&s.file_work_drained_ts) - ts_to_sec(&s.start_ts);
        printf("File work drained sec : %.2f\n", drained_sec);
    }
    if (s.file_work_drained && s.finalize_done) {
        double finalize_phase_sec = ts_to_sec(&s.finalize_done_ts) - ts_to_sec(&s.file_work_drained_ts);
        double finalize_done_sec = ts_to_sec(&s.finalize_done_ts) - ts_to_sec(&s.start_ts);
        printf("Finalize dir seconds  : %.2f\n", finalize_phase_sec);
        printf("Finalize done sec     : %.2f\n", finalize_done_sec);
    }
    if (s.finalize_done && s.shutdown_done) {
        double shutdown_tail_sec = ts_to_sec(&s.shutdown_done_ts) - ts_to_sec(&s.finalize_done_ts);
        printf("Shutdown tail seconds : %.2f\n", shutdown_tail_sec);
    }
}

