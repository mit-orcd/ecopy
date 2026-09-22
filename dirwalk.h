/*
 * dirwalk.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Parallel directory walker shared by ecopy's traversal and edelete.
 *
 * Model: a single FIFO of directory nodes (path + lstat + depth, no open
 * descriptors) feeds N worker threads. A worker pops a node, opens the
 * directory (O_NOFOLLOW, verified against the discovery stat so a swapped
 * symlink is rejected), enumerates it with raw getdents64 where available
 * (libc readdir otherwise) and fstatat()s every entry relative to the open
 * descriptor, so no absolute path is re-walked per entry. Subdirectories the
 * consumer asks to descend into are pushed back onto the FIFO. Keeping
 * descriptors open only while a directory is being processed bounds them by
 * the thread count rather than by the queue depth.
 *
 * The consumer supplies callbacks for what happens per directory and per
 * entry. Only one walk may be in progress at a time.
 */

#ifndef DIRWALK_H
#define DIRWALK_H

#include <stdatomic.h>
#include <stddef.h>
#include <sys/stat.h>

typedef struct dirwalk_node {
    struct dirwalk_node *next;
    struct stat st;   /* lstat at discovery; refreshed by fstat once opened */
    int depth;        /* 0 for the root */
    char path[];      /* absolute, exact length (no PATH_MAX padding) */
} dirwalk_node_t;

/* entry() return values for directory entries. Ignored for non-directories. */
#define DIRWALK_SKIP    0
#define DIRWALK_DESCEND 1

typedef struct dirwalk_ops {
    /* Optional per-thread hooks (e.g. bind to an SSH connection, free scratch). */
    void (*thread_start)(int index);
    void (*thread_end)(void);

    /*
     * The directory is open on dir_fd (O_RDONLY|O_DIRECTORY|O_NOFOLLOW) and
     * node->st is fresh. Return 0 to enumerate it; -1 to skip it (report the
     * cause yourself). *ctx is passed unchanged to entry() and dir_end().
     */
    int (*dir_begin)(const dirwalk_node_t *node, int dir_fd, void **ctx);

    /*
     * One call per entry other than "." and "..", with its AT_SYMLINK_NOFOLLOW
     * stat. For S_ISDIR entries return DIRWALK_DESCEND to have the walker
     * queue "<node->path>/<name>"; the return value is ignored otherwise.
     */
    int (*entry)(const dirwalk_node_t *node, int dir_fd, void *ctx,
                 const char *name, const struct stat *st);

    /* After enumeration, before dir_fd is closed. rc is -1 if the entry
     * stream failed part-way, else 0. Only called if dir_begin returned 0. */
    void (*dir_end)(const dirwalk_node_t *node, int dir_fd, void *ctx, int rc);

    /* Walker-internal failure on `path` (open, fstat, stat, alloc); the
     * message has already been printed. Optional. */
    void (*error)(const char *path);
} dirwalk_ops_t;

typedef struct dirwalk_cfg {
    int threads;               /* walker threads, >= 1 */
    size_t getdents_buf;       /* per-thread raw getdents64 buffer bytes; 0 = readdir */
    const _Atomic int *stop;   /* optional external stop flag polled per entry */
} dirwalk_cfg_t;

/*
 * Start walking root (which must lstat as a directory; root_st is that stat).
 * Returns 0 once the threads are running, -1 on setup failure.
 */
int dirwalk_start(const char *root, const struct stat *root_st,
                  const dirwalk_cfg_t *cfg, const dirwalk_ops_t *ops);

/* Block until the FIFO drains and every worker has exited. */
void dirwalk_wait(void);

/* Wake the workers and make them exit without processing queued nodes. */
void dirwalk_request_stop(void);

/*
 * Run fn(i) for i in [0, n) on `threads` threads, in groups of equal depth
 * from the deepest up, joining all threads between groups so that every
 * child is handled before its parent. depth_of(i) must be non-increasing in i
 * (sort your array deepest-first before calling). between_groups (optional)
 * runs on the calling thread after each group; thread_start (optional) runs
 * once on each worker thread. Returns -1 if any fn or between_groups returned
 * nonzero or a thread could not be created; -1 stops at the current group.
 */
int dirwalk_depth_groups(size_t n, int threads,
                         int (*depth_of)(size_t i),
                         int (*fn)(size_t i),
                         int (*between_groups)(void),
                         void (*thread_start)(int index));

/*
 * Within each depth level, give every thread one contiguous slice of indices
 * instead of handing them out one at a time. When the level is sorted by path
 * this keeps the children of one parent on a single thread — the choice for
 * operations that take the parent's inode lock exclusively (rmdir, unlink),
 * where round-robin only makes the threads spin on that lock. same_run
 * (optional) tells whether indices a and b (adjacent, a < b) belong to the
 * same run, e.g. share a parent; slice boundaries are then moved to run
 * boundaries so no run is ever split between threads.
 */
#define DIRWALK_GROUPS_CONTIGUOUS 0x1

int dirwalk_depth_groups_ex(size_t n, int threads,
                            int (*depth_of)(size_t i),
                            int (*fn)(size_t i),
                            int (*between_groups)(void),
                            void (*thread_start)(int index),
                            int flags,
                            int (*same_run)(size_t a, size_t b));

#endif /* DIRWALK_H */
