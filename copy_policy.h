/*
 * copy_policy.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef COPY_POLICY_H
#define COPY_POLICY_H

#include <stdint.h>
#include <sys/stat.h>

/*
 * Configure immutable per-run copy semantics before traversal/workers start.
 * auto_inplace_fresh is deliberately separate from destination_fresh: a local
 * single-file target may live in an existing directory, so root freshness
 * cannot prove that its requested final name is new.
 */
void copy_policy_init(int preserve_times);
void copy_policy_set_destination(int destination_fresh, int auto_inplace_fresh);
int copy_policy_destination_fresh(void);
int copy_policy_preserve_times(void);
int copy_policy_small_inplace(void);

/*
 * Optional target-ownership override (--uid/--gid). When a side is "set", it
 * replaces the source uid/gid for every transferred object; an unset side keeps
 * the source value. The override is applied once where each object's stat is
 * captured, so both the apply and the verify comparison see the same ids.
 */
void copy_policy_set_id_override(int uid_set, uint32_t uid,
                                 int gid_set, uint32_t gid);
void copy_policy_apply_id_override(struct stat *st);
int copy_policy_uid_override_set(void);
uint32_t copy_policy_uid_override_value(void);
int copy_policy_gid_override_set(void);
uint32_t copy_policy_gid_override_value(void);

#endif
