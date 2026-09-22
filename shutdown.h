/*
 * shutdown.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * SIGINT/SIGTERM handling shared by ecopy and edelete. The first signal sets
 * g_shutdown_requested (polled by every work loop, which then abandons queued
 * work instead of draining it) and runs the registered stop callback from a
 * normal thread so it may take mutexes and broadcast condvars. A second signal
 * exits immediately via _exit().
 */

#ifndef ECOPY_SHUTDOWN_H
#define ECOPY_SHUTDOWN_H

#include <stdatomic.h>

extern _Atomic int g_shutdown_requested;

/*
 * Install the handlers. on_stop (may be NULL) runs once, on a dedicated
 * thread, after the first signal; it should wake every blocked worker.
 */
void shutdown_install_handlers(void (*on_stop)(void));

#endif /* ECOPY_SHUTDOWN_H */
