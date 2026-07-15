/*
 * config.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef CONFIG_H
#define CONFIG_H

#include <sys/types.h>

#define ALIGNMENT 4096
#define CHUNK_SIZE ((off_t)(1ULL * 1024ULL * 1024ULL))
/*
 * Files larger than this use the chunked large-file pipeline (bounded
 * concurrency, big sequential I/O); smaller files go to the many-way small
 * pool. Historically this was 10*CHUNK_SIZE (10 MiB); keep that default so the
 * 10-128 MiB band is not stranded in the 32-way small pool where concurrent
 * medium files thrash a bandwidth-limited target. Override with
 * DIRECT_COPY_LARGE_THRESHOLD_MB.
 */
#define LARGE_FILE_THRESHOLD_MB 10

#define MAX_WORKER_SLOTS 256
#define SMALL_WORKER_SLOTS 32
#define LARGE_FILE_WORKERS 6
#define LARGE_FILE_INFLIGHT 16

#define MONITOR_INTERVAL_MS 200
#define SPEED_WINDOW_SEC 10
#define SPEED_SLOTS (SPEED_WINDOW_SEC * 1000 / MONITOR_INTERVAL_MS)

/*
 * Pipelined verification releases remote fire-and-forget (PUTFILE) files in
 * barrier-gated generations: after this many such files accumulate (or the
 * feeder goes idle for ~250 ms), one sshx_barrier_all() materializes them and
 * they are handed to the verify pool. Lower = fresher/more barriers; higher =
 * fewer barriers but more staging memory. Override with
 * DIRECT_COPY_VERIFY_PIPELINE_OPS.
 */
#define VERIFY_PIPELINE_OPS_DEFAULT 4096

#endif
