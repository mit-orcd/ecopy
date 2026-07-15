/*
 * hardlinks.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "hardlinks.h"
#include "fs_util.h"
#include "stats.h"
#include "progress.h"
#include "ssh_transport.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>

/*
 * Registry keyed by (st_dev, st_ino). Only files with st_nlink > 1 ever reach
 * here, so the table and the deferred-job list are proportional to the number of
 * hard-linked inodes and extra links in the tree, not the whole file count.
 */
typedef struct hl_entry {
    dev_t dev;
    ino_t ino;
    char *primary_dst;
    struct hl_entry *next;
} hl_entry_t;

typedef struct hl_job {
    char *primary_dst;
    char *link_dst;
    struct stat st;
    struct hl_job *next;
} hl_job_t;

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static hl_entry_t **g_buckets;
static size_t g_nbuckets;
static size_t g_count;
static hl_job_t *g_jobs_head;
static hl_job_t *g_jobs_tail;

static size_t hl_hash(dev_t dev, ino_t ino, size_t nbuckets)
{
    uint64_t h = (uint64_t)dev * 1099511628211ULL;
    h ^= (uint64_t)ino + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    return (size_t)(h & (uint64_t)(nbuckets - 1));
}

static int hl_grow(void)
{
    size_t new_n = g_nbuckets ? g_nbuckets * 2 : 1024;
    hl_entry_t **nb = calloc(new_n, sizeof(*nb));
    if (!nb) return -1;
    for (size_t i = 0; i < g_nbuckets; i++) {
        hl_entry_t *e = g_buckets[i];
        while (e) {
            hl_entry_t *next = e->next;
            size_t idx = hl_hash(e->dev, e->ino, new_n);
            e->next = nb[idx];
            nb[idx] = e;
            e = next;
        }
    }
    free(g_buckets);
    g_buckets = nb;
    g_nbuckets = new_n;
    return 0;
}

void hardlinks_reset(void)
{
    pthread_mutex_lock(&g_lock);
    for (size_t i = 0; i < g_nbuckets; i++) {
        hl_entry_t *e = g_buckets[i];
        while (e) {
            hl_entry_t *next = e->next;
            free(e->primary_dst);
            free(e);
            e = next;
        }
    }
    free(g_buckets);
    g_buckets = NULL;
    g_nbuckets = 0;
    g_count = 0;
    hl_job_t *j = g_jobs_head;
    while (j) {
        hl_job_t *next = j->next;
        free(j->primary_dst);
        free(j->link_dst);
        free(j);
        j = next;
    }
    g_jobs_head = g_jobs_tail = NULL;
    pthread_mutex_unlock(&g_lock);
}

hl_result_t hardlinks_note(const struct stat *st, const char *dst_path)
{
    hl_result_t result;

    pthread_mutex_lock(&g_lock);

    if (g_nbuckets == 0 && hl_grow() != 0) {
        pthread_mutex_unlock(&g_lock);
        return HL_ERROR;
    }

    size_t idx = hl_hash(st->st_dev, st->st_ino, g_nbuckets);
    for (hl_entry_t *e = g_buckets[idx]; e; e = e->next) {
        if (e->dev == st->st_dev && e->ino == st->st_ino) {
            hl_job_t *job = calloc(1, sizeof(*job));
            char *pri = strdup(e->primary_dst);
            char *lnk = strdup(dst_path);
            if (!job || !pri || !lnk) {
                free(job); free(pri); free(lnk);
                pthread_mutex_unlock(&g_lock);
                return HL_ERROR;
            }
            job->primary_dst = pri;
            job->link_dst = lnk;
            job->st = *st;
            if (g_jobs_tail) g_jobs_tail->next = job;
            else g_jobs_head = job;
            g_jobs_tail = job;
            pthread_mutex_unlock(&g_lock);
            stats_inc_hardlink_seen();
            stats_add_hardlink_saved((uint64_t)(st->st_size > 0 ? st->st_size : 0));
            return HL_SECONDARY;
        }
    }

    /* First sighting: register this dst as the primary and let the caller copy. */
    if (g_count + 1 > g_nbuckets * 3 / 4 && hl_grow() != 0) {
        pthread_mutex_unlock(&g_lock);
        return HL_ERROR;
    }
    idx = hl_hash(st->st_dev, st->st_ino, g_nbuckets);
    hl_entry_t *e = calloc(1, sizeof(*e));
    char *pri = strdup(dst_path);
    if (!e || !pri) {
        free(e); free(pri);
        pthread_mutex_unlock(&g_lock);
        return HL_ERROR;
    }
    e->dev = st->st_dev;
    e->ino = st->st_ino;
    e->primary_dst = pri;
    e->next = g_buckets[idx];
    g_buckets[idx] = e;
    g_count++;
    result = HL_PRIMARY;

    pthread_mutex_unlock(&g_lock);
    return result;
}

/*
 * Cross-filesystem fallback: copy the already-materialized primary to link_dst
 * so no file is lost when a hard link is impossible. Rare (source hard links
 * live on one filesystem, and the destination is normally one filesystem too).
 */
static int hardlink_fallback_copy(const hl_job_t *job)
{
    int in = open(job->primary_dst, O_RDONLY | O_CLOEXEC);
    if (in < 0) {
        progress_interrupt();
        fprintf(stderr, "%s: ", job->primary_dst);
        perror("open");
        return -1;
    }
    int out = open(job->link_dst, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                   job->st.st_mode & 07777);
    if (out < 0) {
        progress_interrupt();
        fprintf(stderr, "%s: ", job->link_dst);
        perror("open");
        close(in);
        return -1;
    }

    char buf[1u << 16];
    ssize_t r;
    int rc = 0;
    uint64_t copied = 0;
    while ((r = read(in, buf, sizeof(buf))) > 0) {
        ssize_t off = 0;
        while (off < r) {
            ssize_t w = write(out, buf + off, (size_t)(r - off));
            if (w < 0) {
                if (errno == EINTR) continue;
                progress_interrupt();
                fprintf(stderr, "%s: ", job->link_dst);
                perror("write");
                rc = -1;
                break;
            }
            off += w;
            copied += (uint64_t)w;
        }
        if (rc != 0) break;
    }
    if (r < 0) {
        progress_interrupt();
        fprintf(stderr, "%s: ", job->primary_dst);
        perror("read");
        rc = -1;
    }
    close(in);
    if (close(out) != 0 && rc == 0) {
        progress_interrupt();
        fprintf(stderr, "%s: ", job->link_dst);
        perror("close");
        rc = -1;
    }
    if (rc == 0) {
        (void)finalize_copied_file(job->link_dst, &job->st);
        stats_inc_files_copied();
        stats_add_bytes(copied);
    }
    return rc;
}

int hardlinks_replay(int remote)
{
    int rc = 0;
    int warned_exdev = 0;

    for (hl_job_t *j = g_jobs_head; j; j = j->next) {
        if (remote) {
            if (sshx_link(j->primary_dst, j->link_dst) == 0) {
                stats_inc_hardlink_created();
            } else {
                rc = -1;
            }
            continue;
        }

        int r = hardlink_create(j->primary_dst, j->link_dst);
        if (r == 0) {
            stats_inc_hardlink_created();
        } else if (r == FS_LINK_EXDEV) {
            if (!warned_exdev) {
                progress_interrupt();
                fprintf(stderr,
                        "Warning: hard link across filesystems; copying data instead of linking.\n");
                warned_exdev = 1;
            }
            if (hardlink_fallback_copy(j) != 0) rc = -1;
        } else {
            rc = -1;
        }
    }
    return rc;
}
