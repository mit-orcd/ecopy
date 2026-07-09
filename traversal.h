/*
 * traversal.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef TRAVERSAL_H
#define TRAVERSAL_H
int traversal_start(const char *src_dir, const char *dst_dir);
void traversal_wait(void);
int traversal_finalize_metadata(void);
int traversal_status(void);

/*
 * Remote-only: select the destination scan mode. In fresh mode (destination
 * root absent at startup) every entry is treated as new, so the per-directory
 * bulk STAT round trip is skipped entirely. Incremental mode keeps bulk STAT so
 * re-runs still skip unchanged files.
 */
void traversal_set_remote_fresh(int fresh);
#endif
