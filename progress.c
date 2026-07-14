/*
 * progress.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "progress.h"
#include "stats.h"
#include "workers.h"
#include "verify.h"
#include "types.h"
#include "config.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <inttypes.h>
#include <stdint.h>

static pthread_t g_monitor_thread;
static int g_monitor_stop = 0;
static int g_monitor_running = 0;
static int g_monitor_tty = 0;
static pthread_mutex_t g_monitor_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_monitor_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_output_lock = PTHREAD_MUTEX_INITIALIZER;

static int get_terminal_width(void) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 20) {
        return ws.ws_col - 1;
    }
    return 120;
}

static void trim_to_width(char *s, int width) {
    int len = (int)strlen(s);
    if (len < width) {
        return;
    }
    if (width <= 4) {
        if (width > 0) {
            s[width - 1] = '\0';
        }
        return;
    }
    s[width - 4] = '.';
    s[width - 3] = '.';
    s[width - 2] = '.';
    s[width - 1] = '\0';
}

static void format_duration(double sec, char *out, size_t out_sz)
{
    if (!out || out_sz == 0) {
        return;
    }

    if (sec < 0.0) {
        sec = 0.0;
    }

    uint64_t total = (uint64_t)(sec + 0.5);
    uint64_t hours = total / 3600;
    uint64_t minutes = (total % 3600) / 60;
    uint64_t seconds = total % 60;

    snprintf(out, out_sz, "%02" PRIu64 ":%02" PRIu64 ":%02" PRIu64,
             hours, minutes, seconds);
}

static void format_bytes_adaptive(uint64_t bytes, char *out, size_t out_sz)
{
    double value = (double)bytes;
    const char *unit = "B";

    if (value >= 1024.0) {
        value /= 1024.0;
        unit = "KiB";
    }
    if (value >= 1024.0) {
        value /= 1024.0;
        unit = "MiB";
    }
    if (value >= 1024.0) {
        value /= 1024.0;
        unit = "GiB";
    }

    if (strcmp(unit, "B") == 0) {
        snprintf(out, out_sz, "%.0f %s", value, unit);
    } else {
        snprintf(out, out_sz, "%.2f %s", value, unit);
    }
}

static void format_rate_adaptive(double bytes_per_sec, char *out, size_t out_sz)
{
    double value = bytes_per_sec;
    const char *unit = "B/s";

    if (value >= 1024.0) {
        value /= 1024.0;
        unit = "KiB/s";
    }
    if (value >= 1024.0) {
        value /= 1024.0;
        unit = "MiB/s";
    }
    if (value >= 1024.0) {
        value /= 1024.0;
        unit = "GiB/s";
    }

    if (strcmp(unit, "B/s") == 0) {
        snprintf(out, out_sz, "%.0f %s", value, unit);
    } else {
        snprintf(out, out_sz, "%.2f %s", value, unit);
    }
}

static void format_file_rate(double files_per_sec, char *out, size_t out_sz)
{
    if (files_per_sec >= 100.0) {
        snprintf(out, out_sz, "%.0f files/s", files_per_sec);
    } else if (files_per_sec >= 10.0) {
        snprintf(out, out_sz, "%.1f files/s", files_per_sec);
    } else {
        snprintf(out, out_sz, "%.2f files/s", files_per_sec);
    }
}

static void build_progress_line(char *out, size_t out_sz) {
    progress_snapshot_t snap;
    uint64_t sq = workers_small_queue_depth();
    uint64_t sa = workers_small_active_count();
    uint64_t lq = workers_large_queue_depth();
    uint64_t la = workers_large_active_count();
    char elapsed_buf[32];
    char remaining_buf[32];
    char copied_buf[32];
    char rate_buf[32];
    char file_rate_buf[32];
    char current_done_buf[32];
    char current_total_buf[32];
    double bytes_per_sec;

    stats_get_progress_snapshot(&snap);
    format_duration(snap.elapsed_sec, elapsed_buf, sizeof(elapsed_buf));
    if (snap.verify_enabled &&
        (snap.verify_only || verify_queue_depth() > 0 ||
         verify_active_count() > 0)) {
        char verified_buf[32];
        double coverage = snap.verify_scope_bytes
                              ? 100.0 * (double)snap.verify_bytes /
                                    (double)snap.verify_scope_bytes
                              : 0.0;
        format_bytes_adaptive(snap.verify_bytes, verified_buf,
                              sizeof(verified_buf));
        snprintf(out, out_sz,
                 "verify: %" PRIu64 " objects, %s sampled, %.3f%% coverage | "
                 "vq:%" PRIu64 " va:%" PRIu64 "/%d | el:%s",
                 snap.verify_objects, verified_buf, coverage,
                 verify_queue_depth(), verify_active_count(),
                 verify_worker_count(), elapsed_buf);
        trim_to_width(out, get_terminal_width());
        return;
    }
    format_bytes_adaptive(snap.bytes_completed, copied_buf, sizeof(copied_buf));
    bytes_per_sec = snap.rolling_completed_gibs * 1024.0 * 1024.0 * 1024.0;
    format_rate_adaptive(bytes_per_sec, rate_buf, sizeof(rate_buf));
    format_file_rate(snap.rolling_files_per_sec, file_rate_buf, sizeof(file_rate_buf));

    int n = snprintf(out, out_sz,
        "%s payload, %s, %s, %" PRIu64 "/%" PRIu64 " files, %" PRIu64 " dirs | sq:%" PRIu64 " sa:%" PRIu64 " lq:%" PRIu64 " la:%" PRIu64 " | el:%s",
        copied_buf,
        rate_buf,
        file_rate_buf,
        snap.files_copied + snap.files_skipped,
        snap.files_seen,
        snap.dirs_seen,
        sq,
        sa,
        lq,
        la,
        elapsed_buf);

    if (snap.traversal_done && snap.planned_copy_bytes > snap.bytes_copied && snap.rolling_gibs > 0.0 && n > 0 && (size_t)n < out_sz) {
        uint64_t remaining_bytes = snap.planned_copy_bytes - snap.bytes_copied;
        double remaining_sec = stats_bytes_to_gib(remaining_bytes) / snap.rolling_gibs;
        format_duration(remaining_sec, remaining_buf, sizeof(remaining_buf));
        n += snprintf(out + n, out_sz - (size_t)n, " | left:%s", remaining_buf);
    } else if (snap.traversal_done && n > 0 && (size_t)n < out_sz) {
        n += snprintf(out + n, out_sz - (size_t)n, " | left:00:00:00");
    } else if (n > 0 && (size_t)n < out_sz) {
        n += snprintf(out + n, out_sz - (size_t)n, " | left:scan");
    }

    if (snap.current_file_total > 0 && n > 0 && (size_t)n < out_sz) {
        double pct = 100.0 * (double)snap.current_file_done / (double)snap.current_file_total;
        format_bytes_adaptive(snap.current_file_done, current_done_buf, sizeof(current_done_buf));
        format_bytes_adaptive(snap.current_file_total, current_total_buf, sizeof(current_total_buf));
        n += snprintf(out + n, out_sz - (size_t)n,
            " | %.1f%% (%s/%s): %s",
            pct,
            current_done_buf,
            current_total_buf,
            snap.current_file);
    }

    trim_to_width(out, get_terminal_width());
}

static void print_progress(void) {
    char line[16384];
    build_progress_line(line, sizeof(line));
    pthread_mutex_lock(&g_output_lock);
    printf("\r\033[2K%s", line);
    fflush(stdout);
    pthread_mutex_unlock(&g_output_lock);
}

static void *monitor_main(void *arg) {
    (void)arg;

    for (;;) {
        stats_record_speed_sample();
        if (g_monitor_tty) print_progress();

        pthread_mutex_lock(&g_monitor_lock);
        if (!g_monitor_stop) {
            struct timespec deadline;
            clock_gettime(CLOCK_REALTIME, &deadline);
            deadline.tv_nsec += MONITOR_INTERVAL_MS * 1000000L;
            deadline.tv_sec += deadline.tv_nsec / 1000000000L;
            deadline.tv_nsec %= 1000000000L;
            pthread_cond_timedwait(&g_monitor_cond, &g_monitor_lock, &deadline);
        }
        int stop = g_monitor_stop;
        pthread_mutex_unlock(&g_monitor_lock);
        if (stop) break;
    }

    stats_record_speed_sample();
    if (g_monitor_tty) print_progress();
    return NULL;
}

int progress_start(void) {
    pthread_mutex_lock(&g_monitor_lock);
    g_monitor_stop = 0;
    g_monitor_tty = isatty(STDOUT_FILENO);
    g_monitor_running = 1;
    pthread_mutex_unlock(&g_monitor_lock);

    if (pthread_create(&g_monitor_thread, NULL, monitor_main, NULL) != 0) {
        perror("pthread_create");
        pthread_mutex_lock(&g_monitor_lock);
        g_monitor_running = 0;
        pthread_mutex_unlock(&g_monitor_lock);
        return -1;
    }

    return 0;
}

void progress_stop(void) {
    int running;

    pthread_mutex_lock(&g_monitor_lock);
    g_monitor_stop = 1;
    pthread_cond_signal(&g_monitor_cond);
    running = g_monitor_running;
    pthread_mutex_unlock(&g_monitor_lock);

    if (running) {
        pthread_join(g_monitor_thread, NULL);
    }

    pthread_mutex_lock(&g_monitor_lock);
    g_monitor_running = 0;
    pthread_mutex_unlock(&g_monitor_lock);
}

void progress_interrupt(void) {
    pthread_mutex_lock(&g_monitor_lock);
    int active = g_monitor_running && g_monitor_tty && !g_monitor_stop;
    pthread_mutex_unlock(&g_monitor_lock);

    if (active) {
        pthread_mutex_lock(&g_output_lock);
        printf("\r\033[2K");
        fflush(stdout);
        pthread_mutex_unlock(&g_output_lock);
    }
}
