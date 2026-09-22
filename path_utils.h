/*
 * path_utils.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Path string helpers shared by ecopy and edelete. This is also the single
 * place that supplies a PATH_MAX fallback for platforms that leave it
 * undefined.
 */

#ifndef PATH_UTILS_H
#define PATH_UTILS_H

#include <limits.h>
#include <stddef.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* Strip trailing '/' while the string is longer than one character ("/" stays "/"). */
void path_rstrip_slashes(char *s);

/*
 * Write "<base>/<name>" into out. Lengths are passed in because callers on the
 * traversal hot path already have them; a base of exactly "/" yields "/name"
 * rather than "//name". Returns 0, or -1 if the result would not fit.
 */
int path_join_fast(const char *base, size_t base_len, const char *name, size_t name_len,
                   char *out, size_t out_sz);

/*
 * True if path equals root or lies strictly beneath it as a path prefix
 * (component-aware: "/a/bc" is not under "/a/b"). A root of "/" contains every
 * path except "/" itself.
 */
int path_is_under_root(const char *path, const char *root);

/*
 * realpath(3) an existing path into out (PATH_MAX bytes). On failure prints
 * "<err_label><path>: <strerror>" to stderr and returns -1; err_label may be
 * NULL.
 */
int path_resolve_existing(const char *in, char out[PATH_MAX], const char *err_label);

#endif /* PATH_UTILS_H */
