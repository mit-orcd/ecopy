/*
 * shutdown.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Only async-signal-safe operations run in the handler itself: an atomic
 * store, sem_post(), and _exit(). Taking mutexes or broadcasting condvars
 * directly in the handler can self-deadlock when the signal interrupts a
 * thread that already holds that mutex, so the stop callback runs on a
 * watcher thread that the handler wakes with the semaphore.
 */

#define _GNU_SOURCE
#include "shutdown.h"

#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <unistd.h>

_Atomic int g_shutdown_requested = 0;

static sem_t g_shutdown_sem;
static volatile sig_atomic_t g_signal_count = 0;
static void (*g_on_stop)(void) = NULL;

static void shutdown_handler(int sig)
{
    if (g_signal_count++ == 0) {
        g_shutdown_requested = 1;
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
    if (g_on_stop) {
        g_on_stop();
    }
    return NULL;
}

void shutdown_install_handlers(void (*on_stop)(void))
{
    struct sigaction sa;
    pthread_t watcher;

    g_on_stop = on_stop;
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
