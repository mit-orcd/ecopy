/*
 * main.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _GNU_SOURCE
#include "stats.h"
#include "progress.h"
#include "workers.h"
#include "traversal.h"
#include "suggestion.h"
#include "ssh_transport.h"
#include "server.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fcntl.h>

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [-v|--verbose] <source_dir> <target_dir>\n"
            "       <target_dir> may be a local path or ssh://[user@]host[:port]/path\n",
            prog);
}

/*
 * Raise the open-file soft limit to the hard limit so that highly concurrent
 * copies do not fail with "Too many open files". This is best effort: if the
 * limit cannot be queried or raised we continue with whatever is in effect.
 */
static void raise_open_file_limit(int verbose) {
    struct rlimit rl;

    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        if (verbose) {
            perror("getrlimit(RLIMIT_NOFILE)");
        }
        return;
    }

    if (rl.rlim_max != RLIM_INFINITY && rl.rlim_cur >= rl.rlim_max) {
        /* Already at the hard limit; nothing to raise. */
    } else if (rl.rlim_cur != RLIM_INFINITY) {
        struct rlimit want = rl;
        want.rlim_cur = rl.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &want) == 0) {
            rl = want;
        } else if (verbose) {
            perror("setrlimit(RLIMIT_NOFILE)");
        }
    }

    if (verbose) {
        if (rl.rlim_cur == RLIM_INFINITY) {
            printf("Open file limit (RLIMIT_NOFILE): unlimited\n");
        } else {
            printf("Open file limit (RLIMIT_NOFILE): %llu\n",
                   (unsigned long long)rl.rlim_cur);
        }
    }
}

static int to_abs_path(const char *in, char *out, size_t out_sz) {
    if (!in || !out || out_sz == 0) {
        return -1;
    }

    if (in[0] == '/') {
        if (snprintf(out, out_sz, "%s", in) >= (int)out_sz) {
            return -1;
        }
    } else {
        char cwd[PATH_MAX];
        if (!getcwd(cwd, sizeof(cwd))) {
            perror("getcwd");
            return -1;
        }
        if (snprintf(out, out_sz, "%s/%s", cwd, in) >= (int)out_sz) {
            return -1;
        }
    }

    size_t len = strlen(out);
    while (len > 1 && out[len - 1] == '/') {
        out[len - 1] = '\0';
        len--;
    }

    return 0;
}

static int to_canonical_dir_path(const char *in, char *out, size_t out_sz) {
    char tmp[PATH_MAX];
    if (to_abs_path(in, tmp, sizeof(tmp)) != 0) {
        return -1;
    }

    if (!realpath(tmp, out)) {
        perror(in);
        return -1;
    }

    if (strlen(out) >= out_sz) {
        errno = ENAMETOOLONG;
        perror("realpath");
        return -1;
    }

    return 0;
}

static void remove_last_path_component(char *path)
{
    char *slash;

    if (strcmp(path, "/") == 0) {
        return;
    }

    slash = strrchr(path, '/');
    if (!slash || slash == path) {
        path[1] = '\0';
    } else {
        *slash = '\0';
    }
}

static int append_path_component(char *path, size_t path_sz, const char *component)
{
    size_t len = strlen(path);
    size_t component_len = strlen(component);
    size_t extra_slash = (strcmp(path, "/") == 0) ? 0 : 1;

    if (len + extra_slash + component_len >= path_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }

    if (extra_slash) {
        strcat(path, "/");
    }
    strcat(path, component);
    return 0;
}

static int normalize_abs_path_lexically(const char *in, char *out, size_t out_sz)
{
    char tmp[PATH_MAX];
    char *saveptr = NULL;
    char *component;

    if (!in || in[0] != '/' || !out || out_sz < 2) {
        errno = EINVAL;
        return -1;
    }
    if (snprintf(tmp, sizeof(tmp), "%s", in) >= (int)sizeof(tmp)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    snprintf(out, out_sz, "/");
    component = strtok_r(tmp, "/", &saveptr);
    while (component) {
        if (strcmp(component, ".") == 0) {
            /* skip */
        } else if (strcmp(component, "..") == 0) {
            remove_last_path_component(out);
        } else if (append_path_component(out, out_sz, component) != 0) {
            return -1;
        }
        component = strtok_r(NULL, "/", &saveptr);
    }

    return 0;
}

static int prepend_suffix_component(char *suffix, size_t suffix_sz, const char *component)
{
    char tmp[PATH_MAX];

    if (suffix[0] == '\0') {
        if (snprintf(suffix, suffix_sz, "%s", component) >= (int)suffix_sz) {
            errno = ENAMETOOLONG;
            return -1;
        }
        return 0;
    }

    if (snprintf(tmp, sizeof(tmp), "%s/%s", component, suffix) >= (int)sizeof(tmp) ||
        snprintf(suffix, suffix_sz, "%s", tmp) >= (int)suffix_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int join_parent_suffix(const char *parent, const char *suffix, char *out, size_t out_sz)
{
    if (!suffix || suffix[0] == '\0') {
        if (snprintf(out, out_sz, "%s", parent) >= (int)out_sz) {
            errno = ENAMETOOLONG;
            return -1;
        }
        return 0;
    }

    if (strcmp(parent, "/") == 0) {
        if (snprintf(out, out_sz, "/%s", suffix) >= (int)out_sz) {
            errno = ENAMETOOLONG;
            return -1;
        }
    } else if (snprintf(out, out_sz, "%s/%s", parent, suffix) >= (int)out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }

    return 0;
}

static int to_canonical_requested_dir_path(const char *in, char *out, size_t out_sz)
{
    char abs_path[PATH_MAX];
    char normalized[PATH_MAX];
    char probe[PATH_MAX];
    char suffix[PATH_MAX] = "";
    struct stat st;

    if (to_abs_path(in, abs_path, sizeof(abs_path)) != 0 ||
        normalize_abs_path_lexically(abs_path, normalized, sizeof(normalized)) != 0) {
        fprintf(stderr, "Target path too long: %s\n", in);
        return -1;
    }
    if (snprintf(probe, sizeof(probe), "%s", normalized) >= (int)sizeof(probe)) {
        fprintf(stderr, "Target path too long: %s\n", in);
        return -1;
    }

    for (;;) {
        if (stat(probe, &st) == 0) {
            char parent_real[PATH_MAX];

            if (!S_ISDIR(st.st_mode)) {
                fprintf(stderr, "Target path component exists but is not a directory: %s\n", probe);
                return -1;
            }
            if (!realpath(probe, parent_real)) {
                perror(probe);
                return -1;
            }
            if (join_parent_suffix(parent_real, suffix, out, out_sz) != 0) {
                fprintf(stderr, "Target path too long: %s\n", in);
                return -1;
            }
            return 0;
        }

        if (errno != ENOENT) {
            perror(probe);
            return -1;
        }
        if (lstat(probe, &st) == 0) {
            fprintf(stderr, "Target path component exists but is not a directory: %s\n", probe);
            return -1;
        }
        if (errno != ENOENT) {
            perror(probe);
            return -1;
        }
        if (strcmp(probe, "/") == 0) {
            perror(probe);
            return -1;
        }

        {
            char *slash = strrchr(probe, '/');
            const char *component;

            if (!slash) {
                fprintf(stderr, "Target path too long: %s\n", in);
                return -1;
            }
            component = slash + 1;
            if (*component == '\0' ||
                prepend_suffix_component(suffix, sizeof(suffix), component) != 0) {
                fprintf(stderr, "Target path too long: %s\n", in);
                return -1;
            }
            if (slash == probe) {
                probe[1] = '\0';
            } else {
                *slash = '\0';
            }
        }
    }
}

static int mkdir_p_dir(const char *path, mode_t mode) {
    char tmp[PATH_MAX];
    char *p;
    int dir_fd;

    if (snprintf(tmp, sizeof(tmp), "%s", path) >= (int)sizeof(tmp)) {
        errno = ENAMETOOLONG;
        perror(path);
        return -1;
    }

    if (tmp[0] == '\0') {
        fprintf(stderr, "Target path is empty\n");
        return -1;
    }

    if (tmp[0] != '/') {
        errno = EINVAL;
        perror(path);
        return -1;
    }

    dir_fd = open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir_fd < 0) {
        perror("/");
        return -1;
    }

    p = tmp + 1;
    while (*p) {
        char *slash = strchr(p, '/');
        char saved = '\0';
        struct stat st;
        int child_fd;

        if (slash) {
            saved = *slash;
            *slash = '\0';
        }

        if (*p != '\0') {
            if (fstatat(dir_fd, p, &st, AT_SYMLINK_NOFOLLOW) == 0) {
                if (!S_ISDIR(st.st_mode)) {
                    fprintf(stderr, "Target path component exists but is not a directory: %s\n", path);
                    close(dir_fd);
                    if (slash) {
                        *slash = saved;
                    }
                    return -1;
                }
            } else if (errno == ENOENT) {
                if (mkdirat(dir_fd, p, mode) != 0 && errno != EEXIST) {
                    perror(path);
                    close(dir_fd);
                    if (slash) {
                        *slash = saved;
                    }
                    return -1;
                }
            } else {
                perror(path);
                close(dir_fd);
                if (slash) {
                    *slash = saved;
                }
                return -1;
            }

            child_fd = openat(dir_fd, p, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
            if (child_fd < 0) {
                perror(path);
                close(dir_fd);
                if (slash) {
                    *slash = saved;
                }
                return -1;
            }
            close(dir_fd);
            dir_fd = child_fd;
        }

        if (!slash) {
            break;
        }
        *slash = saved;
        p = slash + 1;
    }

    if (close(dir_fd) != 0) {
        perror(path);
        return -1;
    }

    return 0;
}

static int ensure_destination_root(const char *dst_arg) {
    char dst_path[PATH_MAX];

    if (to_abs_path(dst_arg, dst_path, sizeof(dst_path)) != 0) {
        fprintf(stderr, "Target path too long: %s\n", dst_arg);
        return -1;
    }

    return mkdir_p_dir(dst_path, 0777);
}

static int is_same_or_child_path(const char *base, const char *path) {
    size_t n = strlen(base);
    if (strcmp(base, "/") == 0) {
        return path[0] == '/';
    }
    if (strncmp(base, path, n) != 0) {
        return 0;
    }
    return path[n] == '\0' || path[n] == '/';
}

int main(int argc, char **argv) {
    struct stat st;
    char src_abs[PATH_MAX];
    char dst_abs[PATH_MAX];
    const char *src_arg;
    const char *dst_arg;
    const char *dst_root;
    int verbose = 0;
    int remote = 0;
    ssh_target_t target;

    /* Hidden remote peer mode: `ecopy --server <root>`. */
    if (argc == 3 && strcmp(argv[1], "--server") == 0) {
        return server_main(argv[2]);
    }

    if (argc == 4 && (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--verbose") == 0)) {
        verbose = 1;
        src_arg = argv[2];
        dst_arg = argv[3];
    } else if (argc == 3) {
        src_arg = argv[1];
        dst_arg = argv[2];
    } else {
        usage(argv[0]);
        return 1;
    }

    if (ssh_target_is_url(src_arg)) {
        fprintf(stderr, "ecopy: ssh:// sources are not supported yet (push only)\n");
        return 1;
    }

    if (lstat(src_arg, &st) != 0) { perror(src_arg); return 1; }
    if (!S_ISDIR(st.st_mode)) { fprintf(stderr, "Source is not a directory: %s\n", src_arg); return 1; }

    if (to_canonical_dir_path(src_arg, src_abs, sizeof(src_abs)) != 0) {
        return 1;
    }

    remote = ssh_target_is_url(dst_arg);
    if (remote) {
        if (ssh_target_parse(dst_arg, &target) != 0) {
            fprintf(stderr, "ecopy: malformed ssh target: %s\n", dst_arg);
            return 1;
        }
        if (sshx_connect(&target) != 0) {
            return 1;
        }
        dst_root = sshx_remote_root();
    } else {
        if (to_canonical_requested_dir_path(dst_arg, dst_abs, sizeof(dst_abs)) != 0) {
            return 1;
        }
        if (is_same_or_child_path(src_abs, dst_abs) || is_same_or_child_path(dst_abs, src_abs)) {
            fprintf(stderr, "Source and target directories overlap: %s <-> %s\n", src_abs, dst_abs);
            return 1;
        }
        if (ensure_destination_root(dst_abs) != 0) {
            return 1;
        }
        dst_root = dst_abs;
    }

    raise_open_file_limit(verbose);

    stats_init();
    workers_set_collect_wait_timing(verbose);
    if (workers_start() != 0) { sshx_disconnect(); return 1; }
    if (verbose) {
        workers_print_startup_config();
    }
    if (progress_start() != 0) { workers_stop(); sshx_disconnect(); return 1; }
    if (traversal_start(src_abs, dst_root) != 0) {
        progress_stop();
        workers_stop();
        sshx_disconnect();
        return 1;
    }
    traversal_wait();
    workers_stop();
    if (traversal_finalize_metadata() != 0) {
        sshx_disconnect();
        progress_stop();
        stats_set_shutdown_done();
        printf("\n");
        stats_print_final(verbose);
        if (verbose) {
            workers_print_runtime_summary();
        }
        suggestion_print_next_run();
        return 1;
    }
    /* All remote operations are complete; close the SSH channel. */
    sshx_disconnect();
    progress_stop();
    stats_set_shutdown_done();
    printf("\n");
    stats_print_final(verbose);
    if (verbose) {
        workers_print_runtime_summary();
    }
    suggestion_print_next_run();

    if (traversal_status() != 0 || workers_status() != 0) {
        return 1;
    }
    return 0;
}
