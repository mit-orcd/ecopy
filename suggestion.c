#define _GNU_SOURCE
#include "suggestion.h"
#include "stats.h"
#include "workers.h"
#include "fs_util.h"
#include "types.h"

#include <stdio.h>

static double ts_to_sec_local(const struct timespec *ts)
{
    return (double)ts->tv_sec + (double)ts->tv_nsec / 1e9;
}

void suggestion_print_next_run(void)
{
    stats_t s;
    double elapsed;
    double traversal_sec = 0.0;
    double finalize_sec = 0.0;
    double avg_file_mib = 0.0;
    double ready_avg = 0.0;

    stats_get_final(&s);
    if (!s.shutdown_done) {
        return;
    }

    elapsed = ts_to_sec_local(&s.shutdown_done_ts) - ts_to_sec_local(&s.start_ts);
    if (elapsed <= 0.0) {
        return;
    }
    if (s.traversal_done) {
        traversal_sec = ts_to_sec_local(&s.traversal_done_ts) - ts_to_sec_local(&s.start_ts);
    }
    if (s.file_work_drained && s.finalize_done) {
        finalize_sec = ts_to_sec_local(&s.finalize_done_ts) - ts_to_sec_local(&s.file_work_drained_ts);
    }
    if (s.files_seen > 0) {
        avg_file_mib = (double)s.bytes_copied / (1024.0 * 1024.0) / (double)s.files_seen;
    }
    if (s.ready_queue_samples > 0) {
        ready_avg = (double)s.ready_queue_total / (double)s.ready_queue_samples;
    }

    printf("Suggested next run:\n");

    if (direct_io_enabled() && s.files_seen >= 10000 && avg_file_mib < 8.0) {
        printf("  DIRECT_COPY_DISABLE_DIRECT_IO=1\n");
        printf("Reason: this looks like a small-file or metadata-heavy workload, and buffered I/O is usually a better fit.\n");
        return;
    }

    if (traversal_sec > 0.0 && traversal_sec / elapsed > 0.50) {
        printf("  DIRECT_COPY_TRAVERSAL_WORKERS=%d\n", workers_traversal_workers() + 4);
        printf("Reason: traversal is dominating the run, so more directory walkers are the most likely next lever.\n");
        return;
    }

    if (finalize_sec > 0.50) {
        printf("  DIRECT_COPY_TRAVERSAL_WORKERS=%d\n", workers_traversal_workers() + 4);
        printf("Reason: directory finalization is still measurable, and it scales with the traversal/finalize worker pool.\n");
        return;
    }

    if (!direct_io_enabled() && s.copy_file_range_fallbacks > 1000 && s.copy_file_range_calls == 0) {
        printf("  DIRECT_COPY_DISABLE_COPY_FILE_RANGE=1\n");
        printf("Reason: copy_file_range is mostly falling back on this filesystem, so disabling it may reduce pointless retries.\n");
        return;
    }

    if (s.large_chunk_buffer_allocs > 0 && s.writer_data_wait_ns > s.reader_buffer_wait_ns * 2 && ready_avg < 1.0) {
        printf("  DIRECT_COPY_LARGE_THRESHOLD_MB=%d\n", workers_large_threshold_mb() * 2);
        printf("Reason: large-file writers are waiting for data and medium files may be entering the large pipeline too early.\n");
        return;
    }

    if (workers_small_worker_limit() < workers_max_workers()) {
        printf("  DIRECT_COPY_SMALL_MAX_WORKERS=%d\n", workers_small_worker_limit() + 16);
        printf("Reason: the run does not show a clear bottleneck, so the least risky next experiment is a modestly higher small-file cap.\n");
        return;
    }

    printf("  DIRECT_COPY_MAX_QUEUED_FILES=%d\n", workers_max_queued_files() * 2);
    printf("Reason: the current report does not show a single dominant bottleneck, so try a slightly larger queue cap for the same workload.\n");
}
