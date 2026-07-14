/*
 * Low-overhead transfer telemetry.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */
#include "telemetry.h"

#include <limits.h>
#include <pthread.h>
#include <string.h>

#define HIST_SUB_BITS 3u
#define HIST_SUBS (1u << HIST_SUB_BITS)
#define HIST_BINS (64u * HIST_SUBS)

typedef struct {
    uint64_t bins[HIST_BINS];
    uint64_t count;
    uint64_t sum;
    uint64_t min;
    uint64_t max;
} histogram_t;

typedef struct {
    histogram_t latency;
    histogram_t rate;
    uint64_t count;
    uint64_t logical_bytes;
    uint64_t payload_bytes;
    uint64_t service_ns;
} class_accum_t;

static class_accum_t g_classes[TRANSFER_CLASS_COUNT];
static histogram_t g_rate;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static _Thread_local class_accum_t g_tls[TRANSFER_CLASS_COUNT];

static unsigned hist_index(uint64_t value)
{
    if (value == 0) return 0;
    unsigned exponent = 63u - (unsigned)__builtin_clzll(value);
    uint64_t base = UINT64_C(1) << exponent;
    unsigned sub = exponent == 0
                       ? 0
                       : (unsigned)(((value - base) * HIST_SUBS) / base);
    if (sub >= HIST_SUBS) sub = HIST_SUBS - 1;
    return exponent * HIST_SUBS + sub;
}

static uint64_t hist_value(unsigned index)
{
    unsigned exponent = index / HIST_SUBS;
    unsigned sub = index % HIST_SUBS;
    uint64_t base = UINT64_C(1) << exponent;
    if (exponent == 0) return index == 0 ? 0 : 1;
    return base + (uint64_t)((long double)base * sub / HIST_SUBS);
}

static void hist_add(histogram_t *hist, uint64_t value)
{
    hist->bins[hist_index(value)]++;
    hist->count++;
    hist->sum += value;
    if (hist->count == 1 || value < hist->min) hist->min = value;
    if (hist->count == 1 || value > hist->max) hist->max = value;
}

static void hist_merge(histogram_t *dst, histogram_t *src)
{
    if (src->count == 0) return;
    for (unsigned i = 0; i < HIST_BINS; i++) {
        dst->bins[i] += src->bins[i];
    }
    if (dst->count == 0 || src->min < dst->min) dst->min = src->min;
    if (dst->count == 0 || src->max > dst->max) dst->max = src->max;
    dst->count += src->count;
    dst->sum += src->sum;
    memset(src, 0, sizeof(*src));
}

static uint64_t hist_percentile(const histogram_t *hist, unsigned percent)
{
    if (hist->count == 0) return 0;
    uint64_t rank = (hist->count * percent + 99u) / 100u;
    if (rank == 0) rank = 1;
    uint64_t seen = 0;
    for (unsigned i = 0; i < HIST_BINS; i++) {
        seen += hist->bins[i];
        if (seen >= rank) return hist_value(i);
    }
    return hist->max;
}

static uint64_t bytes_per_second(uint64_t bytes, uint64_t elapsed_ns)
{
    if (bytes == 0 || elapsed_ns == 0) return 0;
    long double value = (long double)bytes * 1000000000.0L /
                        (long double)elapsed_ns;
    return value >= (long double)UINT64_MAX ? UINT64_MAX : (uint64_t)value;
}

void telemetry_init(void)
{
    pthread_mutex_lock(&g_lock);
    memset(g_classes, 0, sizeof(g_classes));
    memset(&g_rate, 0, sizeof(g_rate));
    pthread_mutex_unlock(&g_lock);
    memset(g_tls, 0, sizeof(g_tls));
}

void telemetry_note_file(transfer_class_t cls, uint64_t logical_bytes,
                         uint64_t payload_bytes, uint64_t service_ns)
{
    if (cls < 0 || cls >= TRANSFER_CLASS_COUNT || service_ns == 0) return;
    class_accum_t *accum = &g_tls[cls];
    accum->count++;
    accum->logical_bytes += logical_bytes;
    accum->payload_bytes += payload_bytes;
    accum->service_ns += service_ns;
    hist_add(&accum->latency, service_ns);
    if (payload_bytes > 0) {
        hist_add(&accum->rate, bytes_per_second(payload_bytes, service_ns));
    }
}

void telemetry_flush_thread(void)
{
    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < TRANSFER_CLASS_COUNT; i++) {
        class_accum_t *dst = &g_classes[i];
        class_accum_t *src = &g_tls[i];
        dst->count += src->count;
        dst->logical_bytes += src->logical_bytes;
        dst->payload_bytes += src->payload_bytes;
        dst->service_ns += src->service_ns;
        hist_merge(&dst->latency, &src->latency);
        hist_merge(&dst->rate, &src->rate);
        src->count = src->logical_bytes = src->payload_bytes = src->service_ns = 0;
    }
    pthread_mutex_unlock(&g_lock);
}

void telemetry_note_rate_window(uint64_t bytes, uint64_t elapsed_ns)
{
    if (elapsed_ns == 0) return;
    pthread_mutex_lock(&g_lock);
    hist_add(&g_rate, bytes_per_second(bytes, elapsed_ns));
    pthread_mutex_unlock(&g_lock);
}

void telemetry_get(transfer_class_summary_t out[TRANSFER_CLASS_COUNT],
                   transfer_rate_summary_t *rate)
{
    telemetry_flush_thread();
    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < TRANSFER_CLASS_COUNT; i++) {
        const class_accum_t *src = &g_classes[i];
        transfer_class_summary_t *dst = &out[i];
        memset(dst, 0, sizeof(*dst));
        dst->count = src->count;
        dst->logical_bytes = src->logical_bytes;
        dst->payload_bytes = src->payload_bytes;
        dst->service_ns = src->service_ns;
        dst->latency_min_ns = src->latency.min;
        dst->latency_p50_ns = hist_percentile(&src->latency, 50);
        dst->latency_p95_ns = hist_percentile(&src->latency, 95);
        dst->latency_p99_ns = hist_percentile(&src->latency, 99);
        dst->latency_max_ns = src->latency.max;
        dst->rate_samples = src->rate.count;
        dst->rate_min_bps = src->rate.min;
        dst->rate_p10_bps = hist_percentile(&src->rate, 10);
        dst->rate_p50_bps = hist_percentile(&src->rate, 50);
        dst->rate_p90_bps = hist_percentile(&src->rate, 90);
        dst->rate_p99_bps = hist_percentile(&src->rate, 99);
        dst->rate_max_bps = src->rate.max;
    }
    memset(rate, 0, sizeof(*rate));
    rate->samples = g_rate.count;
    rate->min_bps = g_rate.min;
    rate->p10_bps = hist_percentile(&g_rate, 10);
    rate->p50_bps = hist_percentile(&g_rate, 50);
    rate->p90_bps = hist_percentile(&g_rate, 90);
    rate->p99_bps = hist_percentile(&g_rate, 99);
    rate->max_bps = g_rate.max;
    pthread_mutex_unlock(&g_lock);
}

const char *telemetry_class_name(transfer_class_t cls)
{
    switch (cls) {
    case TRANSFER_SMALL: return "Small dense";
    case TRANSFER_LARGE: return "Large dense";
    case TRANSFER_SPARSE: return "Sparse";
    default: return "Unknown";
    }
}
