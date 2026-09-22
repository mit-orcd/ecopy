/*
 * format.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Human-readable number formatting shared by the ecopy and edelete progress
 * lines and reports.
 */

#ifndef FORMAT_H
#define FORMAT_H

#include <stddef.h>

/* Seconds -> "HH:MM:SS" (rounded to the nearest second, never negative). */
void format_duration(double sec, char *out, size_t out_sz);

/*
 * Plain count with an SI suffix and three significant digits: 950 -> "950",
 * 12345 -> "12.3K", 2.5e9 -> "2.50G". For item counts and rates, not bytes.
 */
void format_count_si(double v, char *out, size_t out_sz);

#endif /* FORMAT_H */
