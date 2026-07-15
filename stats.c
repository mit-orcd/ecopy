/*
 * stats.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "stats.h"
#include "config.h"
#include "telemetry.h"
#include "third_party/blake3/blake3.h"

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
static struct timespec g_rate_window_ts;
static uint64_t g_rate_window_bytes;
static int g_rate_window_started;
static int g_rate_window_finished;

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
static _Atomic int a_first_payload_seen;
static struct timespec g_first_payload_ts;

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

static uint64_t diff_ns(const struct timespec *a, const struct timespec *b) {
    int64_t sec = (int64_t)a->tv_sec - (int64_t)b->tv_sec;
    int64_t nsec = (int64_t)a->tv_nsec - (int64_t)b->tv_nsec;
    int64_t total = sec * INT64_C(1000000000) + nsec;
    return total > 0 ? (uint64_t)total : 0;
}

double stats_bytes_to_gib(uint64_t bytes) {
    return (double)bytes / (1024.0 * 1024.0 * 1024.0);
}

static void format_bps(uint64_t bps, char *out, size_t out_sz) {
    double value = (double)bps;
    const char *unit = "B/s";
    if (value >= 1024.0) { value /= 1024.0; unit = "KiB/s"; }
    if (value >= 1024.0) { value /= 1024.0; unit = "MiB/s"; }
    if (value >= 1024.0) { value /= 1024.0; unit = "GiB/s"; }
    snprintf(out, out_sz, "%.2f %s", value, unit);
}

static void format_bytes(uint64_t bytes, char *out, size_t out_sz) {
    static const char *units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    double value = (double)bytes;
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        unit++;
    }
    snprintf(out, out_sz, "%.2f %s", value, units[unit]);
}

void stats_format_rate(uint64_t bytes, double seconds, char *out, size_t out_sz) {
    uint64_t bps = 0;
    if (seconds > 0.0 && bytes > 0) {
        long double value = (long double)bytes / seconds;
        bps = value >= (long double)UINT64_MAX ? UINT64_MAX : (uint64_t)value;
    }
    format_bps(bps, out, out_sz);
}

static void format_latency(uint64_t ns, char *out, size_t out_sz) {
    if (ns < 1000) snprintf(out, out_sz, "%" PRIu64 " ns", ns);
    else if (ns < 1000000) snprintf(out, out_sz, "%.2f us", (double)ns / 1e3);
    else if (ns < UINT64_C(1000000000))
        snprintf(out, out_sz, "%.2f ms", (double)ns / 1e6);
    else snprintf(out, out_sz, "%.2f s", (double)ns / 1e9);
}

void stats_init(void) {
    pthread_mutex_lock(&g_lock);
    memset(&g_stats, 0, sizeof(g_stats));
    memset(g_speed_ring, 0, sizeof(g_speed_ring));
    memset(g_current_file, 0, sizeof(g_current_file));
    g_current_file_total = 0;
    g_current_file_parallel = 0;
    g_speed_index = 0;
    g_rate_window_started = 0;
    g_rate_window_finished = 0;
    g_rate_window_bytes = 0;
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
    atomic_store_explicit(&a_first_payload_seen, 0, memory_order_relaxed);
    clock_gettime(CLOCK_MONOTONIC, &g_stats.start_ts);
    pthread_mutex_unlock(&g_lock);
    telemetry_init();
}

static void note_first_payload(void) {
    int expected = 0;
    if (atomic_compare_exchange_strong_explicit(&a_first_payload_seen, &expected, 1,
                                                memory_order_acquire,
                                                memory_order_relaxed)) {
        clock_gettime(CLOCK_MONOTONIC, &g_first_payload_ts);
        atomic_store_explicit(&a_first_payload_seen, 2, memory_order_release);
    } else {
        while (atomic_load_explicit(&a_first_payload_seen,
                                    memory_order_acquire) != 2) {
        }
    }
}

void stats_add_bytes(uint64_t bytes) {
    if (bytes > 0) note_first_payload();
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

void stats_set_copy_complete(void) {
    pthread_mutex_lock(&g_lock);
    if (!g_stats.copy_complete) {
        g_stats.copy_complete = 1;
        clock_gettime(CLOCK_MONOTONIC, &g_stats.copy_complete_ts);
    }
    pthread_mutex_unlock(&g_lock);
}

void stats_set_verification_started(void) {
    pthread_mutex_lock(&g_lock);
    if (!g_stats.verification_started) {
        g_stats.verification_started = 1;
        clock_gettime(CLOCK_MONOTONIC, &g_stats.verification_start_ts);
    }
    pthread_mutex_unlock(&g_lock);
}

void stats_set_verification_done(void) {
    pthread_mutex_lock(&g_lock);
    if (!g_stats.verification_done) {
        g_stats.verification_done = 1;
        clock_gettime(CLOCK_MONOTONIC, &g_stats.verification_done_ts);
    }
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
void stats_inc_symlink_seen(void){ pthread_mutex_lock(&g_lock); g_stats.symlinks_seen++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_symlink_created(void){ pthread_mutex_lock(&g_lock); g_stats.symlinks_created++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_hardlink_seen(void){ pthread_mutex_lock(&g_lock); g_stats.hardlinks_seen++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_hardlink_created(void){ pthread_mutex_lock(&g_lock); g_stats.hardlinks_created++; pthread_mutex_unlock(&g_lock);} 
void stats_add_hardlink_saved(uint64_t bytes){ pthread_mutex_lock(&g_lock); g_stats.hardlink_bytes_saved += bytes; pthread_mutex_unlock(&g_lock);} 
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
void stats_set_verify_runtime(int verify_only, int workers,
                              uint64_t queue_peak, uint64_t active_peak) {
    pthread_mutex_lock(&g_lock);
    if (verify_only >= 0) g_stats.verify_only = verify_only;
    g_stats.verify_workers = workers;
    if (queue_peak > g_stats.verify_queue_peak) {
        g_stats.verify_queue_peak = queue_peak;
    }
    if (active_peak > g_stats.verify_active_peak) {
        g_stats.verify_active_peak = active_peak;
    }
    pthread_mutex_unlock(&g_lock);
}
void stats_add_verify_busy_ns(uint64_t ns) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_busy_ns += ns;
    pthread_mutex_unlock(&g_lock);
}
void stats_set_verify_pending_peak(uint64_t pending_peak) {
    pthread_mutex_lock(&g_lock);
    if (pending_peak > g_stats.verify_pending_peak) {
        g_stats.verify_pending_peak = pending_peak;
    }
    pthread_mutex_unlock(&g_lock);
}
void stats_record_verify(uint64_t bytes, uint64_t scope_bytes, uint64_t blocks,
                         int metadata_checked,
                         int data_mismatch, int metadata_mismatch,
                         int expected_zero_mismatch, int io_failure, int failed) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_objects++;
    g_stats.verify_bytes += bytes;
    g_stats.verify_scope_bytes += scope_bytes;
    g_stats.verify_blocks += blocks;
    if (metadata_checked) g_stats.verify_metadata_objects++;
    if (data_mismatch) g_stats.verify_data_mismatches++;
    if (metadata_mismatch) g_stats.verify_metadata_mismatches++;
    if (expected_zero_mismatch) g_stats.verify_zero_mismatches++;
    if (io_failure) g_stats.verify_io_failures++;
    if (failed) g_stats.verify_failures++;
    pthread_mutex_unlock(&g_lock);
}
void stats_mark_verify_failure(void) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_failures++;
    pthread_mutex_unlock(&g_lock);
}
void stats_record_verify_holes(uint64_t blocks, uint64_t bytes) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_hole_blocks += blocks;
    g_stats.verify_hole_bytes += bytes;
    g_stats.verify_source_reads_avoided += blocks;
    pthread_mutex_unlock(&g_lock);
}
void stats_record_verify_ownership(uint64_t n) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_ownership_unpreserved += n;
    pthread_mutex_unlock(&g_lock);
}
void stats_set_remote_drain(uint64_t bytes, uint64_t ns) {
    pthread_mutex_lock(&g_lock);
    g_stats.remote_drain_bytes = bytes;
    g_stats.remote_drain_ns = ns;
    pthread_mutex_unlock(&g_lock);
}
void stats_record_verify_categories(uint64_t metadata, uint64_t data,
                                    uint64_t expected_zero, uint64_t io,
                                    uint64_t malformed, uint64_t ownership) {
    pthread_mutex_lock(&g_lock);
    g_stats.verify_metadata_mismatches += metadata;
    g_stats.verify_data_mismatches += data;
    g_stats.verify_zero_mismatches += expected_zero;
    g_stats.verify_io_failures += io;
    g_stats.verify_malformed_batches += malformed;
    g_stats.verify_ownership_unpreserved += ownership;
    g_stats.verify_failures += metadata + data + expected_zero + io + malformed;
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
    if (bytes > 0) note_first_payload();
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
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    pthread_mutex_lock(&g_lock);
    g_speed_ring[g_speed_index].ts = now;
    g_speed_ring[g_speed_index].bytes_copied = bytes_copied;
    g_speed_ring[g_speed_index].bytes_completed = bytes_copied + g_stats.bytes_skipped;
    g_speed_ring[g_speed_index].files_completed = g_stats.files_copied + g_stats.files_skipped;
    g_speed_ring[g_speed_index].valid = 1;
    g_speed_index = (g_speed_index + 1) % SPEED_SLOTS;
    if (!g_rate_window_started && bytes_copied > 0) {
        g_rate_window_started = 1;
        g_rate_window_ts = g_first_payload_ts;
        g_rate_window_bytes = 0;
    } else if (g_rate_window_started && !g_rate_window_finished) {
        uint64_t elapsed_ns = diff_ns(&now, &g_rate_window_ts);
        int drained = g_stats.file_work_drained;
        if (elapsed_ns >= UINT64_C(1000000000) || drained) {
            telemetry_note_rate_window(bytes_copied - g_rate_window_bytes,
                                       elapsed_ns);
            g_rate_window_ts = now;
            g_rate_window_bytes = bytes_copied;
            if (drained) g_rate_window_finished = 1;
        }
    }
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
    snap->verify_objects = g_stats.verify_objects;
    snap->verify_bytes = g_stats.verify_bytes;
    snap->verify_scope_bytes = g_stats.verify_scope_bytes;
    snap->verify_enabled = g_stats.verify_metadata_enabled ||
                           g_stats.verify_data_enabled;
    snap->verify_only = g_stats.verify_only;
    snap->copy_complete = g_stats.copy_complete;
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
    transfer_class_summary_t classes[TRANSFER_CLASS_COUNT];
    transfer_rate_summary_t rolling;
    char data_rate[32], complete_rate[32], verify_rate[32];
    char logical[32], payload[32];
    double data_sec = s.file_work_drained
                          ? diff_sec(&s.file_work_drained_ts, &s.start_ts) : 0.0;
    double complete_sec = s.copy_complete
                              ? diff_sec(&s.copy_complete_ts, &s.start_ts) : 0.0;
    double verify_wall_sec = s.verification_started && s.verification_done
                                 ? diff_sec(&s.verification_done_ts,
                                            &s.verification_start_ts) : 0.0;
    /* Summed worker service time; parallel checkers can exceed wall. Rates use
     * this so an overlapped (pipelined) verify phase is not diluted by the copy
     * window it hides behind. */
    double verify_busy_sec = (double)s.verify_busy_ns / 1e9;
    /* Pipelined dir copies start verification before copy completes. */
    int verify_overlapped = !s.verify_only && s.verification_started &&
                            s.copy_complete &&
                            diff_sec(&s.verification_start_ts,
                                     &s.copy_complete_ts) < 0.0;
    telemetry_get(classes, &rolling);
    stats_format_rate(s.bytes_copied, data_sec, data_rate, sizeof(data_rate));
    stats_format_rate(s.bytes_copied, complete_sec, complete_rate, sizeof(complete_rate));
    stats_format_rate(s.verify_bytes, verify_busy_sec, verify_rate, sizeof(verify_rate));
    printf("Done.\n");
    if (!s.verify_only) {
        format_bytes(s.planned_copy_bytes, logical, sizeof(logical));
        format_bytes(s.bytes_copied, payload, sizeof(payload));
        printf("\nCopy\n");
    }
    printf("Files seen    : %" PRIu64 "\n", s.files_seen);
    if (!s.verify_only) {
        printf("Files copied  : %" PRIu64 "\n", s.files_copied);
        printf("Files skipped : %" PRIu64 "\n", s.files_skipped);
    }
    printf("Dirs seen     : %" PRIu64 "\n", s.dirs_seen);
    if (!s.verify_only) {
        printf("Dirs created  : %" PRIu64 "\n", s.dirs_created);
        if (s.symlinks_seen > 0) {
            printf("Symlinks      : %" PRIu64 " of %" PRIu64 "\n",
                   s.symlinks_created, s.symlinks_seen);
        }
        if (s.hardlinks_seen > 0) {
            char saved[32];
            format_bytes(s.hardlink_bytes_saved, saved, sizeof(saved));
            printf("Hard links    : %" PRIu64 " linked, %s not duplicated\n",
                   s.hardlinks_created, saved);
        }
        printf("Logical bytes : %" PRIu64 " (%s)\n", s.planned_copy_bytes, logical);
        printf("Payload bytes : %" PRIu64 " (%s)\n", s.bytes_copied, payload);
        if (s.planned_copy_bytes > s.bytes_copied) {
            printf("Sparse savings: %" PRIu64 " bytes (%.2f%%)\n",
                   s.planned_copy_bytes - s.bytes_copied,
                   100.0 * (double)(s.planned_copy_bytes - s.bytes_copied) /
                       (double)s.planned_copy_bytes);
        }
        printf("Copy data elapsed : %.2f s\n", data_sec);
        printf("Copy data rate    : %s\n", data_rate);
        printf("Copied files rate : %.2f files/s\n",
               data_sec > 0.0 ? (double)s.files_copied / data_sec : 0.0);
        printf("Copy complete sec : %.2f s\n", complete_sec);
        printf("Copy complete rate: %s\n", complete_rate);
        if (s.remote_drain_ns > 0) {
            char drain_rate[32];
            double drain_sec = (double)s.remote_drain_ns / 1e9;
            stats_format_rate(s.remote_drain_bytes, drain_sec,
                              drain_rate, sizeof(drain_rate));
            printf("Remote drain rate : %s (server write+fsync service)\n",
                   drain_rate);
            printf("Remote drain busy : %.2f s\n", drain_sec);
        }
        if (s.bytes_skipped > 0) {
            printf("GiB skipped   : %.2f\n", stats_bytes_to_gib(s.bytes_skipped));
        }
    }
    if (s.metadata_warnings > 0) {
        printf("Metadata warnings : %" PRIu64 "\n", s.metadata_warnings);
    }
    if (s.metadata_errors > 0) {
        printf("Metadata errors   : %" PRIu64 "\n", s.metadata_errors);
    }
    if (!s.verify_only) {
        printf("\nTransfer distribution (worker service)\n");
        if (rolling.samples > 0) {
            char min[32], p10[32], p50[32], p90[32], p99[32], max[32];
            format_bps(rolling.min_bps, min, sizeof(min));
            format_bps(rolling.p10_bps, p10, sizeof(p10));
            format_bps(rolling.p50_bps, p50, sizeof(p50));
            format_bps(rolling.p90_bps, p90, sizeof(p90));
            format_bps(rolling.p99_bps, p99, sizeof(p99));
            format_bps(rolling.max_bps, max, sizeof(max));
            printf("1s payload rate (%" PRIu64 " windows): min %s, p10 %s, p50 %s, p90 %s, p99 %s, max %s\n",
                   rolling.samples, min, p10, p50, p90, p99, max);
        }
        for (int i = 0; i < TRANSFER_CLASS_COUNT; i++) {
            transfer_class_summary_t *c = &classes[i];
            if (c->count == 0) continue;
            char service_rate[32], l50[24], l95[24], l99[24];
            char rmin[32], r10[32], r50[32], r90[32], r99[32], rmax[32];
            stats_format_rate(c->payload_bytes, (double)c->service_ns / 1e9,
                              service_rate, sizeof(service_rate));
            format_latency(c->latency_p50_ns, l50, sizeof(l50));
            format_latency(c->latency_p95_ns, l95, sizeof(l95));
            format_latency(c->latency_p99_ns, l99, sizeof(l99));
            printf("%s: files %" PRIu64 ", logical %" PRIu64 ", payload %" PRIu64
                   ", summed-service rate %s\n",
                   telemetry_class_name((transfer_class_t)i), c->count,
                   c->logical_bytes, c->payload_bytes, service_rate);
            printf("  latency p50 %s, p95 %s, p99 %s\n", l50, l95, l99);
            if (c->rate_samples > 0) {
                format_bps(c->rate_min_bps, rmin, sizeof(rmin));
                format_bps(c->rate_p10_bps, r10, sizeof(r10));
                format_bps(c->rate_p50_bps, r50, sizeof(r50));
                format_bps(c->rate_p90_bps, r90, sizeof(r90));
                format_bps(c->rate_p99_bps, r99, sizeof(r99));
                format_bps(c->rate_max_bps, rmax, sizeof(rmax));
                printf("  per-file throughput min %s, p10 %s, p50 %s, p90 %s, p99 %s, max %s\n",
                       rmin, r10, r50, r90, r99, rmax);
            }
        }
    }
    if (s.verify_metadata_enabled || s.verify_data_enabled) {
        double achieved = s.verify_scope_bytes
                              ? 100.0 * (double)s.verify_bytes /
                                    (double)s.verify_scope_bytes
                              : 0.0;
        printf("\nVerification\n");
        printf("Verify objects    : %" PRIu64 "\n", s.verify_objects);
        printf("Verify blocks     : %" PRIu64 "\n", s.verify_blocks);
        char verify_scope[32], verify_sample[32];
        format_bytes(s.verify_scope_bytes, verify_scope, sizeof(verify_scope));
        format_bytes(s.verify_bytes, verify_sample, sizeof(verify_sample));
        printf("Verify scope      : %" PRIu64 " (%s)\n",
               s.verify_scope_bytes, verify_scope);
        printf("Verify sampled    : %" PRIu64 " (%s)\n",
               s.verify_bytes, verify_sample);
        printf("Verify MiB        : %.2f\n",
               (double)s.verify_bytes / (1024.0 * 1024.0));
        printf("Verify coverage   : %.3f%% requested, %.3f%% achieved\n",
               s.verify_requested_percent, achieved);
        printf("Verify seed       : %" PRIu64 "\n", s.verify_seed);
        printf("Verify wall sec   : %.3f\n", verify_wall_sec);
        if (verify_overlapped) {
            printf("  (overlapped copy; wall is not additive with copy time - "
                   "see Total Elapsed)\n");
        }
        printf("Verify busy sec   : %.3f (summed worker service)\n",
               verify_busy_sec);
        printf("Verify object rate: %.2f objects/s (worker-equivalent)\n",
               verify_busy_sec > 0.0
                   ? (double)s.verify_objects / verify_busy_sec
                   : 0.0);
        printf("Verify sample rate: %s (worker-equivalent)\n", verify_rate);
        printf("Verify hash backend: %s\n", blake3_backend());
        printf("Verify readahead  : %s\n",
               s.verify_requested_percent >= 100.0 ? "sequential" : "random");
        if (s.verify_hole_blocks > 0) {
            printf("Verify hole blocks: %" PRIu64 " (%" PRIu64 " bytes)\n",
                   s.verify_hole_blocks, s.verify_hole_bytes);
            printf("Source reads saved: %" PRIu64 "\n",
                   s.verify_source_reads_avoided);
        }
        printf("Verify workers    : %d configured, %" PRIu64 " peak active\n",
               s.verify_workers, s.verify_active_peak);
        if (s.verify_pending_peak > 0) {
            printf("Verify pending peak: %" PRIu64 "\n",
                   s.verify_pending_peak);
        }
        printf("Verify queue peak : %" PRIu64 "\n", s.verify_queue_peak);
        if (s.verify_ownership_unpreserved)
            printf("Ownership not preserved: %" PRIu64 " (uid/gid, unprivileged; not a failure)\n",
                   s.verify_ownership_unpreserved);
        printf("Verify failures   : %" PRIu64 "\n", s.verify_failures);
        if (s.verify_metadata_mismatches)
            printf("  metadata mismatches : %" PRIu64 "\n", s.verify_metadata_mismatches);
        if (s.verify_data_mismatches)
            printf("  data mismatches     : %" PRIu64 "\n", s.verify_data_mismatches);
        if (s.verify_zero_mismatches)
            printf("  hole-zero mismatches: %" PRIu64 "\n", s.verify_zero_mismatches);
        if (s.verify_io_failures)
            printf("  checker I/O failures: %" PRIu64 "\n", s.verify_io_failures);
        if (s.verify_malformed_batches)
            printf("  malformed batches   : %" PRIu64 "\n", s.verify_malformed_batches);
    }
    printf("\nTotal\n");
    printf("Elapsed       : %.2f s\n", sec);

    if (!verbose) {
        return;
    }

    if (!s.verify_only) {
        printf("Bytes copied  : %" PRIu64 "\n", s.bytes_copied);
        printf("Bytes skipped : %" PRIu64 "\n", s.bytes_skipped);
    }
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

