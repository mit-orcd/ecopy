/*
 * format.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#include "format.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

void format_duration(double sec, char *out, size_t out_sz)
{
    uint64_t total, hours, minutes, seconds;

    if (!out || out_sz == 0) {
        return;
    }
    if (sec < 0.0) {
        sec = 0.0;
    }

    total = (uint64_t)(sec + 0.5);
    hours = total / 3600;
    minutes = (total % 3600) / 60;
    seconds = total % 60;

    snprintf(out, out_sz, "%02" PRIu64 ":%02" PRIu64 ":%02" PRIu64, hours, minutes, seconds);
}

void format_count_si(double v, char *out, size_t out_sz)
{
    static const char *units[] = {"", "K", "M", "G", "T", "P", "E"};
    int i = 0;

    if (!out || out_sz == 0) {
        return;
    }
    while (v >= 1000.0 && i < 6) {
        v /= 1000.0;
        i++;
    }

    if (v >= 100.0) {
        snprintf(out, out_sz, "%.0f%s", v, units[i]);
    } else if (v >= 10.0) {
        snprintf(out, out_sz, "%.1f%s", v, units[i]);
    } else {
        snprintf(out, out_sz, "%.2f%s", v, units[i]);
    }
}
