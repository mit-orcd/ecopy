/*
 * shutdown.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * SIGINT/SIGTERM handling. The first signal sets a global flag (polled by the
 * work loops, which then abandon queued work instead of draining it) and posts
 * a semaphore; a dedicated watcher thread wakes every subsystem condvar from
 * normal thread context. A second signal exits immediately via _exit().
 *
 * Only async-signal-safe operations run in the handler itself: an atomic
 * store, sem_post(), and _exit(). Taking mutexes or broadcasting condvars
 * directly in the handler can self-deadlock when the signal interrupts a
 * thread that already holds that mutex.
 */

#define _GNU_SOURCE
#include "shutdown.h"
#include "workers.h"
#include "traversal.h"
#include "verify.h"

#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <unistd.h>

_Atomic int g_ecopy_shutdown = 0;

static sem_t g_shutdown_sem;
static volatile sig_atomic_t g_signal_count = 0;

static void shutdown_handler(int sig)
{
    if (g_signal_count++ == 0) {
        g_ecopy_shutdown = 1;
        sem_post(&g_shutdown_sem);
    } else {
        /* Second Ctrl+C: force an immediate exit even if a thread is stuck
         * in an uninterruptible I/O wait. */
        _exit(128 + sig);
    }
}

static void *shutdown_watcher_main(void *arg)
{
    (void)arg;
    while (sem_wait(&g_shutdown_sem) != 0) {
        /* retry on EINTR */
    }
    /* Normal thread context: mutexes and condvar broadcasts are legal here. */
    traversal_request_stop();
    workers_request_stop();
    verify_request_stop();
    return NULL;
}

void shutdown_install_handlers(void)
{
    struct sigaction sa;
    pthread_t watcher;

    sem_init(&g_shutdown_sem, 0, 0);

    sa.sa_handler = shutdown_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; /* no SA_RESTART: interrupt blocking I/O with EINTR */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (pthread_create(&watcher, NULL, shutdown_watcher_main, NULL) == 0) {
        pthread_detach(watcher);
    }
}
