/*
 * copy_policy.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#include "copy_policy.h"

#include <stdlib.h>
#include <string.h>

static int g_destination_fresh;
static int g_auto_inplace_fresh;
static int g_preserve_times = 1;
static int g_explicit_inplace;

void copy_policy_init(int preserve_times)
{
    const char *s = getenv("DIRECT_COPY_SMALL_INPLACE");
    const char *no_times = getenv("DIRECT_COPY_NO_PRESERVE_TIMES");

    g_destination_fresh = 0;
    g_auto_inplace_fresh = 0;
    g_preserve_times = preserve_times &&
                       !(no_times && *no_times && strcmp(no_times, "0") != 0);
    g_explicit_inplace = s && *s && strcmp(s, "0") != 0;
}

void copy_policy_set_destination(int destination_fresh, int auto_inplace_fresh)
{
    g_destination_fresh = destination_fresh ? 1 : 0;
    g_auto_inplace_fresh = auto_inplace_fresh ? 1 : 0;
}

int copy_policy_destination_fresh(void)
{
    return g_destination_fresh;
}

int copy_policy_preserve_times(void)
{
    return g_preserve_times;
}

int copy_policy_small_inplace(void)
{
    return g_explicit_inplace ||
           (g_destination_fresh && g_auto_inplace_fresh);
}
