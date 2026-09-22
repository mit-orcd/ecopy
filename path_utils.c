/*
 * path_utils.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#include "path_utils.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void path_rstrip_slashes(char *s)
{
    size_t n;

    if (!s) {
        return;
    }
    n = strlen(s);
    while (n > 1U && s[n - 1U] == '/') {
        s[--n] = '\0';
    }
}

int path_join_fast(const char *base, size_t base_len, const char *name, size_t name_len,
                   char *out, size_t out_sz)
{
    size_t need;

    if (!base || !name || !out || out_sz == 0) {
        return -1;
    }

    if (base_len == 1U && base[0] == '/') {
        base_len = 0; /* avoid "//name" */
    }
    need = base_len + 1U + name_len + 1U;
    if (need > out_sz) {
        return -1;
    }
    memcpy(out, base, base_len);
    out[base_len] = '/';
    if (name_len > 0U) {
        memcpy(out + base_len + 1U, name, name_len);
    }
    out[base_len + 1U + name_len] = '\0';
    return 0;
}

int path_is_under_root(const char *path, const char *root)
{
    size_t lr;

    if (!path || !root) {
        return 0;
    }
    if (strcmp(root, "/") == 0) {
        return strcmp(path, "/") != 0;
    }
    lr = strlen(root);
    if (strncmp(path, root, lr) != 0) {
        return 0;
    }
    return path[lr] == '\0' || path[lr] == '/';
}

int path_resolve_existing(const char *in, char out[PATH_MAX], const char *err_label)
{
    const char *label = err_label ? err_label : "";

    if (!in || !out || in[0] == '\0') {
        errno = EINVAL;
        fprintf(stderr, "%s(empty path)\n", label);
        return -1;
    }
    if (!realpath(in, out)) {
        fprintf(stderr, "%s%s: %s\n", label, in, strerror(errno));
        return -1;
    }
    return 0;
}
