/*
 * Low-overhead transfer telemetry.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */
#ifndef TELEMETRY_H
#define TELEMETRY_H

#include <stdint.h>

typedef enum {
    TRANSFER_SMALL = 0,
    TRANSFER_LARGE = 1,
    TRANSFER_SPARSE = 2,
    TRANSFER_CLASS_COUNT = 3
} transfer_class_t;

typedef struct {
    uint64_t count;
    uint64_t logical_bytes;
    uint64_t payload_bytes;
    uint64_t service_ns;
    uint64_t latency_min_ns;
    uint64_t latency_p50_ns;
    uint64_t latency_p95_ns;
    uint64_t latency_p99_ns;
    uint64_t latency_max_ns;
    uint64_t rate_samples;
    uint64_t rate_min_bps;
    uint64_t rate_p10_bps;
    uint64_t rate_p50_bps;
    uint64_t rate_p90_bps;
    uint64_t rate_p99_bps;
    uint64_t rate_max_bps;
} transfer_class_summary_t;

typedef struct {
    uint64_t samples;
    uint64_t min_bps;
    uint64_t p10_bps;
    uint64_t p50_bps;
    uint64_t p90_bps;
    uint64_t p99_bps;
    uint64_t max_bps;
} transfer_rate_summary_t;

void telemetry_init(void);
void telemetry_note_file(transfer_class_t cls, uint64_t logical_bytes,
                         uint64_t payload_bytes, uint64_t service_ns);
void telemetry_flush_thread(void);
void telemetry_note_rate_window(uint64_t bytes, uint64_t elapsed_ns);
void telemetry_get(transfer_class_summary_t out[TRANSFER_CLASS_COUNT],
                   transfer_rate_summary_t *rate);
const char *telemetry_class_name(transfer_class_t cls);

#endif
