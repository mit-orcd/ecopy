/*
 * copy_policy.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef COPY_POLICY_H
#define COPY_POLICY_H

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

#endif
