#define _GNU_SOURCE
#include "stats.h"
#include "config.h"

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <inttypes.h>

static stats_t g_stats;
static speed_sample_t g_speed_ring[SPEED_SLOTS];
static int g_speed_index = 0;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_current_file[PATH_MAX];
static uint64_t g_current_file_done = 0;
static uint64_t g_current_file_total = 0;
static int g_current_file_parallel = 0;
static parallel_ctx_t *g_active_parallel_ctx = NULL;

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
    g_current_file_done = 0;
    g_current_file_total = 0;
    g_current_file_parallel = 0;
    g_active_parallel_ctx = NULL;
    g_speed_index = 0;
    clock_gettime(CLOCK_MONOTONIC, &g_stats.start_ts);
    pthread_mutex_unlock(&g_lock);
}

void stats_add_bytes(uint64_t bytes) {
    pthread_mutex_lock(&g_lock);
    g_stats.bytes_copied += bytes;
    pthread_mutex_unlock(&g_lock);
}
void stats_inc_files_seen(void){ pthread_mutex_lock(&g_lock); g_stats.files_seen++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_files_copied(void){ pthread_mutex_lock(&g_lock); g_stats.files_copied++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_files_skipped(void){ pthread_mutex_lock(&g_lock); g_stats.files_skipped++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_dirs_seen(void){ pthread_mutex_lock(&g_lock); g_stats.dirs_seen++; pthread_mutex_unlock(&g_lock);} 
void stats_inc_dirs_created(void){ pthread_mutex_lock(&g_lock); g_stats.dirs_created++; pthread_mutex_unlock(&g_lock);} 

void stats_begin_current_file(const char *path, uint64_t total_bytes, int is_parallel) {
    pthread_mutex_lock(&g_lock);
    snprintf(g_current_file, sizeof(g_current_file), "%s", path ? path : "");
    g_current_file_done = 0;
    g_current_file_total = total_bytes;
    g_current_file_parallel = is_parallel;
    pthread_mutex_unlock(&g_lock);
}

void stats_advance_current_file(uint64_t bytes) {
    pthread_mutex_lock(&g_lock);
    g_current_file_done += bytes;
    g_stats.bytes_copied += bytes;
    pthread_mutex_unlock(&g_lock);
}

void stats_end_current_file(void) {
    pthread_mutex_lock(&g_lock);
    g_current_file[0] = '\0';
    g_current_file_done = 0;
    g_current_file_total = 0;
    g_current_file_parallel = 0;
    g_active_parallel_ctx = NULL;
    pthread_mutex_unlock(&g_lock);
}

void stats_set_active_parallel_ctx(parallel_ctx_t *ctx){ pthread_mutex_lock(&g_lock); g_active_parallel_ctx = ctx; pthread_mutex_unlock(&g_lock);} 
void stats_clear_active_parallel_ctx(void){ pthread_mutex_lock(&g_lock); g_active_parallel_ctx = NULL; pthread_mutex_unlock(&g_lock);} 
parallel_ctx_t *stats_get_active_parallel_ctx(void){ parallel_ctx_t *ctx; pthread_mutex_lock(&g_lock); ctx = g_active_parallel_ctx; pthread_mutex_unlock(&g_lock); return ctx; }

void stats_record_speed_sample(void) {
    pthread_mutex_lock(&g_lock);
    clock_gettime(CLOCK_MONOTONIC, &g_speed_ring[g_speed_index].ts);
    g_speed_ring[g_speed_index].bytes_copied = g_stats.bytes_copied;
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

double stats_avg_gibs(void) {
    uint64_t bytes;
    double elapsed = stats_elapsed_sec();
    pthread_mutex_lock(&g_lock);
    bytes = g_stats.bytes_copied;
    pthread_mutex_unlock(&g_lock);
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
    uint64_t cur_bytes = g_stats.bytes_copied;
    pthread_mutex_unlock(&g_lock);
    if (!found) return 0.0;
    double dt = diff_sec(&now, &old_ts);
    if (dt <= 0.0) return 0.0;
    return stats_bytes_to_gib(cur_bytes - old_bytes) / dt;
}

void stats_get_progress_snapshot(progress_snapshot_t *snap) {
    if (!snap) return;
    pthread_mutex_lock(&g_lock);
    snap->bytes_copied = g_stats.bytes_copied;
    snap->files_seen = g_stats.files_seen;
    snap->files_copied = g_stats.files_copied;
    snap->files_skipped = g_stats.files_skipped;
    snap->dirs_seen = g_stats.dirs_seen;
    snap->dirs_created = g_stats.dirs_created;
    snap->current_file_done = g_current_file_done;
    snap->current_file_total = g_current_file_total;
    snap->current_file_parallel = g_current_file_parallel;
    snprintf(snap->current_file, sizeof(snap->current_file), "%s", g_current_file);
    pthread_mutex_unlock(&g_lock);
    snap->rolling_gibs = stats_rolling_gibs();
    snap->elapsed_sec = stats_elapsed_sec();
}

void stats_print_final(void) {
    stats_t s;
    pthread_mutex_lock(&g_lock);
    s = g_stats;
    pthread_mutex_unlock(&g_lock);
    double sec = stats_elapsed_sec();
    double gib = stats_bytes_to_gib(s.bytes_copied);
    double avg = sec > 0.0 ? gib / sec : 0.0;
    printf("Done.\n");
    printf("Files seen    : %" PRIu64 "\n", s.files_seen);
    printf("Files copied  : %" PRIu64 "\n", s.files_copied);
    printf("Files skipped : %" PRIu64 "\n", s.files_skipped);
    printf("Dirs seen     : %" PRIu64 "\n", s.dirs_seen);
    printf("Dirs created  : %" PRIu64 "\n", s.dirs_created);
    printf("Bytes copied  : %" PRIu64 "\n", s.bytes_copied);
    printf("GiB copied    : %.2f\n", gib);
    printf("Elapsed       : %.2f s\n", sec);
    printf("Avg speed     : %.2f GiB/s\n", avg);
}
