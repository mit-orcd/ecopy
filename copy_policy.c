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
static int g_uid_override_set;
static uint32_t g_uid_override;
static int g_gid_override_set;
static uint32_t g_gid_override;

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

void copy_policy_set_id_override(int uid_set, uint32_t uid,
                                 int gid_set, uint32_t gid)
{
    g_uid_override_set = uid_set ? 1 : 0;
    g_uid_override = uid;
    g_gid_override_set = gid_set ? 1 : 0;
    g_gid_override = gid;
}

void copy_policy_apply_id_override(struct stat *st)
{
    if (g_uid_override_set) st->st_uid = (uid_t)g_uid_override;
    if (g_gid_override_set) st->st_gid = (gid_t)g_gid_override;
}

int copy_policy_uid_override_set(void)
{
    return g_uid_override_set;
}

uint32_t copy_policy_uid_override_value(void)
{
    return g_uid_override;
}

int copy_policy_gid_override_set(void)
{
    return g_gid_override_set;
}

uint32_t copy_policy_gid_override_value(void)
{
    return g_gid_override;
}
