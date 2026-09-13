/*
 * progress.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef PROGRESS_H
#define PROGRESS_H
int progress_start(int verbose);
void progress_stop(void);
void progress_interrupt(void);
#endif
