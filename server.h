/*
 * server.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef SERVER_H
#define SERVER_H

/*
 * Run ecopy in remote-server mode. Reads the ecopy SSH protocol from stdin and
 * writes replies to stdout, performing destination filesystem operations
 * confined under root. Returns a process exit code.
 */
int server_main(const char *root, int read_only);

#endif
