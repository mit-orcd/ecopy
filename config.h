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
#define LARGE_FILE_THRESHOLD_MB 128

#define MAX_WORKER_SLOTS 256
#define SMALL_WORKER_SLOTS 32
#define LARGE_FILE_WORKERS 6
#define LARGE_FILE_INFLIGHT 16

#define MONITOR_INTERVAL_MS 200
#define SPEED_WINDOW_SEC 10
#define SPEED_SLOTS (SPEED_WINDOW_SEC * 1000 / MONITOR_INTERVAL_MS)

#endif
