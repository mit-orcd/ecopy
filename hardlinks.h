/*
 * hardlinks.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Hard-link preservation. During traversal every regular file with st_nlink > 1
 * is registered by its source (st_dev, st_ino). The first occurrence in the tree
 * is the "primary" and is copied normally; later occurrences are recorded and,
 * after all copying completes, materialized as real hard links to the primary's
 * destination instead of duplicating data.
 */

#ifndef HARDLINKS_H
#define HARDLINKS_H

#include <sys/stat.h>

typedef enum {
    HL_PRIMARY,   /* first sighting of this inode: caller copies it normally */
    HL_SECONDARY, /* additional link: deferred, do not copy */
    HL_ERROR      /* allocation failure */
} hl_result_t;

/* Discard all registry state and deferred jobs. Call before a run. */
void hardlinks_reset(void);

/*
 * Register a hard-linked regular file (st_nlink > 1) at dst_path. Thread-safe.
 * Returns HL_PRIMARY (first link; caller should copy) or HL_SECONDARY (a
 * deferred link job was recorded; caller should skip the copy).
 */
hl_result_t hardlinks_note(const struct stat *st, const char *dst_path);

/*
 * Materialize all deferred hard links. Call once after copying has drained
 * (workers_stop) and before the SSH flush. remote selects the SSH path
 * (MSG_LINK) vs the local path (linkat, with a cross-fs copy fallback).
 * Returns 0 on success, -1 if any link could not be created.
 */
int hardlinks_replay(int remote);

#endif
