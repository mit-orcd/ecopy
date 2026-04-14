#define _GNU_SOURCE
#include "progress.h"
#include "stats.h"
#include "workers.h"
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
static pthread_mutex_t g_monitor_lock = PTHREAD_MUTEX_INITIALIZER;

static int get_terminal_width(void) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 20) {
        return ws.ws_col;
    }
    return 200;
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

static void append_chunk_worker_stats(char *out, size_t out_sz, int *n_io) {
    int n = *n_io;
    chunk_worker_stat_t workers[128];
    int worker_count = stats_copy_active_parallel_workers(workers, (int)(sizeof(workers) / sizeof(workers[0])));

    if (worker_count <= 0) {
        *n_io = n;
        return;
    }

    for (int i = 0; i < worker_count; i++) {
        chunk_worker_stat_t *ws = &workers[i];

        if (n <= 0 || (size_t)n >= out_sz) {
            break;
        }

        n += snprintf(out + n, out_sz - (size_t)n,
            " | w%d:%c %.1fG %" PRIu64,
            i,
            ws->active ? 'A' : 'I',
            stats_bytes_to_gib(ws->bytes_done),
            ws->chunks_done);
    }

    *n_io = n;
}

static void build_progress_line(char *out, size_t out_sz) {
    progress_snapshot_t snap;
    uint64_t sq = workers_small_queue_depth();
    uint64_t sa = workers_small_active_count();
    uint64_t lq = workers_large_queue_depth();
    uint64_t la = workers_large_active_count();
    char elapsed_buf[32];
    char remaining_buf[32];

    stats_get_progress_snapshot(&snap);
    format_duration(snap.elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

    int n = snprintf(out, out_sz,
        "%.2f GiB, %.2f GiB/s, %" PRIu64 " files, %" PRIu64 " dirs | sq:%" PRIu64 " sa:%" PRIu64 " lq:%" PRIu64 " la:%" PRIu64 " | el:%s",
        stats_bytes_to_gib(snap.bytes_copied),
        snap.rolling_gibs,
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
        n += snprintf(out + n, out_sz - (size_t)n,
            " | %.1f%% (%.2f/%.2f GiB): %s",
            pct,
            stats_bytes_to_gib(snap.current_file_done),
            stats_bytes_to_gib(snap.current_file_total),
            snap.current_file);
    }

    if (snap.current_file_parallel && n > 0 && (size_t)n < out_sz) {
        append_chunk_worker_stats(out, out_sz, &n);
    }

    trim_to_width(out, get_terminal_width());
}

static void print_progress(void) {
    char line[16384];
    build_progress_line(line, sizeof(line));
    printf("\033[2K\r%s", line);
    fflush(stdout);
}

static void *monitor_main(void *arg) {
    (void)arg;

    for (;;) {
        pthread_mutex_lock(&g_monitor_lock);
        int stop = g_monitor_stop;
        pthread_mutex_unlock(&g_monitor_lock);

        if (stop) {
            break;
        }

        stats_record_speed_sample();
        print_progress();
        usleep(MONITOR_INTERVAL_MS * 1000);
    }

    print_progress();
    return NULL;
}

int progress_start(void) {
    pthread_mutex_lock(&g_monitor_lock);
    g_monitor_stop = 0;
    pthread_mutex_unlock(&g_monitor_lock);

    if (pthread_create(&g_monitor_thread, NULL, monitor_main, NULL) != 0) {
        perror("pthread_create");
        return -1;
    }

    return 0;
}

void progress_stop(void) {
    pthread_mutex_lock(&g_monitor_lock);
    g_monitor_stop = 1;
    pthread_mutex_unlock(&g_monitor_lock);

    pthread_join(g_monitor_thread, NULL);
}

void progress_interrupt(void) {
    pthread_mutex_lock(&g_monitor_lock);
    int active = !g_monitor_stop;
    pthread_mutex_unlock(&g_monitor_lock);

    if (active) {
        printf("\033[2K\r");
        fflush(stdout);
    }
}
