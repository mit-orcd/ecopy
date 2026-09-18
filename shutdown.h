/*
 * shutdown.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef ECOPY_SHUTDOWN_H
#define ECOPY_SHUTDOWN_H

#include <stdatomic.h>

extern _Atomic int g_ecopy_shutdown;

void shutdown_install_handlers(void);

#endif /* ECOPY_SHUTDOWN_H */
