/*
 * env_util.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#include "env_util.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int env_int_or_default(const char *name, int defval, int minval, int maxval)
{
    const char *s = getenv(name);
    char *end = NULL;
    long v;

    if (!s || !*s) {
        return defval;
    }

    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') {
        fprintf(stderr, "Warning: %s=%s is invalid; using default %d.\n", name, s, defval);
        return defval;
    }
    if (v < minval) {
        fprintf(stderr, "Warning: %s=%ld is below minimum %d; using %d.\n", name, v, minval, minval);
        return minval;
    }
    if (v > maxval) {
        fprintf(stderr, "Warning: %s=%ld exceeds maximum %d; using %d.\n", name, v, maxval, maxval);
        return maxval;
    }
    return (int)v;
}

int env_flag_set(const char *name)
{
    const char *s = getenv(name);
    return s && *s && strcmp(s, "0") != 0;
}
