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
#include "fs_util.h"
#include "copy_policy.h"
#include "verify.h"
#include "types.h"

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
            "Usage: %s [options] <source> <target>\n"
            "       <source> may be a directory or a single regular file\n"
            "       <target> may be a local path or ssh://[user@]host[:port]/path\n"
            "       For a file source, <target> is the destination directory (or\n"
            "       a full destination path locally); ssh:// targets are directories.\n"
            "       --no-preserve-times skips atime/mtime preservation.\n"
            "       --verify enables metadata checks plus 1%% sampled data checks.\n"
            "       --verify-metadata checks type/size/mode/uid/gid/timestamps.\n"
            "       --verify-data[=PERCENT] checks sampled 4 KiB blocks (default 1%%).\n"
            "       --verify-skipped also checks files skipped as unchanged.\n"
            "       --verify-seed=N makes block selection reproducible.\n"
            "       --verify-only compares source and target without modifying either.\n"
            "       --verify-workers=N sets checker parallelism (1-128).\n",
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

static int mkdir_p_dir(const char *path, mode_t mode, int *final_created) {
    char tmp[PATH_MAX];
    char *p;
    int dir_fd;

    if (final_created) {
        *final_created = 0;
    }
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
                if (mkdirat(dir_fd, p, mode) == 0) {
                    if (!slash && final_created) {
                        *final_created = 1;
                    }
                } else if (errno != EEXIST) {
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

static int ensure_destination_root(const char *dst_arg, int *created) {
    char dst_path[PATH_MAX];

    if (to_abs_path(dst_arg, dst_path, sizeof(dst_path)) != 0) {
        fprintf(stderr, "Target path too long: %s\n", dst_arg);
        return -1;
    }

    return mkdir_p_dir(dst_path, 0777, created);
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

/* Split an absolute path into its parent directory and final component. */
static void split_parent_name(const char *abs, char *parent, size_t psz,
                              char *name, size_t nsz) {
    const char *slash = strrchr(abs, '/');
    if (!slash) {
        snprintf(parent, psz, ".");
        snprintf(name, nsz, "%s", abs);
        return;
    }
    size_t plen = (size_t)(slash - abs);
    if (plen == 0) {
        snprintf(parent, psz, "/");
    } else {
        if (plen >= psz) plen = psz - 1;
        memcpy(parent, abs, plen);
        parent[plen] = '\0';
    }
    snprintf(name, nsz, "%s", slash + 1);
}

/*
 * Enqueue a single regular file for copy through the normal worker path (so it
 * shares the sparse/large/remote handling). Builds a one-shot directory handle
 * for the source parent (and, locally, the destination directory). The worker
 * lands the file under its source name in the destination directory; callers
 * that requested a different final name rename it afterwards.
 */
static int enqueue_single_file(const char *src_parent, const char *src_name,
                               const char *src_full, const char *dst_full,
                               const struct stat *st, int remote) {
    int src_fd = open(src_parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (src_fd < 0) { perror(src_parent); return -1; }

    int dst_fd = -1;
    char dst_dir[PATH_MAX];
    char dst_name[PATH_MAX];
    split_parent_name(dst_full, dst_dir, sizeof(dst_dir),
                      dst_name, sizeof(dst_name));
    if (!remote) {
        dst_fd = open(dst_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dst_fd < 0) { perror(dst_dir); close(src_fd); return -1; }
    }

    dir_handle_t *h = dir_handle_create(src_parent, dst_dir, src_fd, dst_fd);
    if (!h) { close(src_fd); if (dst_fd >= 0) close(dst_fd); return -1; }

    stats_inc_files_seen();
    stats_add_planned_copy_bytes((uint64_t)st->st_size);
    int rc = workers_enqueue_small_file(h, src_name, src_full, dst_full, st);
    dir_handle_release(h);
    return rc;
}

int main(int argc, char **argv) {
    struct stat st;
    char src_abs[PATH_MAX];
    char dst_abs[PATH_MAX];
    const char *src_arg;
    const char *dst_arg;
    const char *dst_root = NULL;
    int verbose = 0;
    int remote = 0;
    int destination_fresh = 0;
    int verify_metadata = 0;
    int verify_data = 0;
    int verify_skipped = 0;
    double verify_pct = 1.0;
    uint64_t verify_seed_value = 0;
    int verify_seed_set = 0;
    int verify_only = 0;
    int verify_selector_seen = 0;
    int verify_workers = 0;
    ssh_target_t target;

    /* Hidden remote peer mode: `ecopy --server <root>`. */
    if (argc == 3 && strcmp(argv[1], "--server") == 0) {
        return server_main(argv[2], 0);
    }
    if (argc == 3 && strcmp(argv[1], "--server-readonly") == 0) {
        return server_main(argv[2], 1);
    }

    int no_preserve_times = 0;
    {
        const char *worker_env = getenv("DIRECT_COPY_VERIFY_WORKERS");
        if (worker_env && *worker_env) {
            char *end = NULL;
            long value = strtol(worker_env, &end, 10);
            if (!end || *end || value < 1 || value > 128) {
                fprintf(stderr, "ecopy: invalid DIRECT_COPY_VERIFY_WORKERS: %s\n",
                        worker_env);
                return 1;
            }
            verify_workers = (int)value;
        }
        const char *pos[2] = { NULL, NULL };
        int npos = 0;
        for (int i = 1; i < argc; i++) {
            if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
                verbose = 1;
            } else if (strcmp(argv[i], "--no-preserve-times") == 0) {
                no_preserve_times = 1;
            } else if (strcmp(argv[i], "--verify") == 0) {
                verify_metadata = 1;
                verify_data = 1;
                verify_selector_seen = 1;
            } else if (strcmp(argv[i], "--verify-metadata") == 0) {
                verify_metadata = 1;
                verify_selector_seen = 1;
            } else if (strcmp(argv[i], "--verify-data") == 0) {
                verify_data = 1;
                verify_selector_seen = 1;
            } else if (strncmp(argv[i], "--verify-data=", 14) == 0) {
                char *end = NULL;
                errno = 0;
                verify_pct = strtod(argv[i] + 14, &end);
                if (errno || end == argv[i] + 14 || *end || verify_pct < 0.0 ||
                    verify_pct > 100.0) {
                    fprintf(stderr, "ecopy: invalid verification percentage: %s\n",
                            argv[i] + 14);
                    return 1;
                }
                verify_data = 1;
                verify_selector_seen = 1;
            } else if (strcmp(argv[i], "--verify-skipped") == 0) {
                verify_skipped = 1;
            } else if (strcmp(argv[i], "--verify-only") == 0) {
                verify_only = 1;
            } else if (strncmp(argv[i], "--verify-workers=", 17) == 0) {
                char *end = NULL;
                long value = strtol(argv[i] + 17, &end, 10);
                if (!end || end == argv[i] + 17 || *end ||
                    value < 1 || value > 128) {
                    fprintf(stderr, "ecopy: invalid verification worker count: %s\n",
                            argv[i] + 17);
                    return 1;
                }
                verify_workers = (int)value;
            } else if (strncmp(argv[i], "--verify-seed=", 14) == 0) {
                char *end = NULL;
                errno = 0;
                unsigned long long value = strtoull(argv[i] + 14, &end, 0);
                if (errno || end == argv[i] + 14 || *end) {
                    fprintf(stderr, "ecopy: invalid verification seed: %s\n",
                            argv[i] + 14);
                    return 1;
                }
                verify_seed_value = (uint64_t)value;
                verify_seed_set = 1;
            } else if (argv[i][0] == '-' && argv[i][1] != '\0' &&
                       strcmp(argv[i], "-") != 0) {
                fprintf(stderr, "ecopy: unknown option: %s\n", argv[i]);
                usage(argv[0]);
                return 1;
            } else if (npos < 2) {
                pos[npos++] = argv[i];
            } else {
                usage(argv[0]);
                return 1;
            }
        }
        if (npos != 2) {
            usage(argv[0]);
            return 1;
        }
        src_arg = pos[0];
        dst_arg = pos[1];
    }
    if (verify_only && !verify_selector_seen) {
        verify_metadata = 1;
        verify_data = 1;
        verify_pct = 1.0;
    }
    copy_policy_init(!no_preserve_times);

    if (ssh_target_is_url(src_arg)) {
        fprintf(stderr, "ecopy: ssh:// sources are not supported yet (push only)\n");
        return 1;
    }

    if (lstat(src_arg, &st) != 0) { perror(src_arg); return 1; }
    int src_is_dir = S_ISDIR(st.st_mode);
    if (!src_is_dir && !S_ISREG(st.st_mode)) {
        fprintf(stderr, "Source is not a regular file or directory: %s\n", src_arg);
        return 1;
    }

    if (to_canonical_dir_path(src_arg, src_abs, sizeof(src_abs)) != 0) {
        return 1;
    }

    /* Single-file mode: destination file path (enqueue target) and, locally,
     * the final path if the user asked for a different name. */
    char src_parent[PATH_MAX];
    char src_name[PATH_MAX];
    char file_enqueue_dst[PATH_MAX];
    char file_final_dst[PATH_MAX];
    file_enqueue_dst[0] = '\0';
    file_final_dst[0] = '\0';
    if (!src_is_dir) {
        split_parent_name(src_abs, src_parent, sizeof(src_parent), src_name, sizeof(src_name));
    }

    remote = ssh_target_is_url(dst_arg);
    if (verify_workers == 0) {
        long cpus = sysconf(_SC_NPROCESSORS_ONLN);
        if (cpus < 1) cpus = 1;
        long cap = remote ? 8 : 16;
        verify_workers = (int)(cpus < cap ? cpus : cap);
    }
    if (remote) {
        if (ssh_target_parse(dst_arg, &target) != 0) {
            fprintf(stderr, "ecopy: malformed ssh target: %s\n", dst_arg);
            return 1;
        }
        sshx_set_read_only(verify_only);
        if (sshx_connect(&target) != 0) {
            return 1;
        }
        dst_root = sshx_remote_root();
        destination_fresh = !sshx_remote_root_present();
        copy_policy_set_destination(destination_fresh, 1);
        /* Pick the scan mode from the handshake: absent root -> fresh (skip
         * per-dir bulk STAT); present -> incremental (keep bulk STAT so re-runs
         * skip unchanged files). */
        if (!src_is_dir) {
            /* The ssh:// path is always the destination directory; the file
             * keeps its source name inside it. */
            if (snprintf(file_enqueue_dst, sizeof(file_enqueue_dst), "%s/%s",
                         dst_root, src_name) >= (int)sizeof(file_enqueue_dst)) {
                fprintf(stderr, "ecopy: remote path too long\n");
                sshx_disconnect();
                return 1;
            }
            if (verify_only) {
                snprintf(file_final_dst, sizeof(file_final_dst), "%s",
                         file_enqueue_dst);
            }
        }
    } else if (src_is_dir) {
        if ((verify_only ? to_canonical_dir_path(dst_arg, dst_abs, sizeof(dst_abs))
                         : to_canonical_requested_dir_path(dst_arg, dst_abs,
                                                           sizeof(dst_abs))) != 0) {
            return 1;
        }
        if (is_same_or_child_path(src_abs, dst_abs) || is_same_or_child_path(dst_abs, src_abs)) {
            fprintf(stderr, "Source and target directories overlap: %s <-> %s\n", src_abs, dst_abs);
            return 1;
        }
        if (!verify_only &&
            ensure_destination_root(dst_abs, &destination_fresh) != 0) {
            return 1;
        }
        copy_policy_set_destination(destination_fresh, 1);
        dst_root = dst_abs;
    } else {
        /* Local single-file destination. If it names an existing directory or
         * ends with '/', the file is placed inside it under its source name;
         * otherwise the argument is the full destination path (rename). */
        char dst_dir[PATH_MAX];
        char final_name[PATH_MAX];
        struct stat dstat;
        size_t dlen = strlen(dst_arg);
        int is_existing_dir = (lstat(dst_arg, &dstat) == 0 && S_ISDIR(dstat.st_mode));
        int trailing_slash = (dlen > 0 && dst_arg[dlen - 1] == '/');

        if (is_existing_dir) {
            if (to_canonical_dir_path(dst_arg, dst_dir, sizeof(dst_dir)) != 0) {
                return 1;
            }
            snprintf(final_name, sizeof(final_name), "%s", src_name);
        } else {
            char dabs[PATH_MAX];
            char dnorm[PATH_MAX];
            if (to_abs_path(dst_arg, dabs, sizeof(dabs)) != 0 ||
                normalize_abs_path_lexically(dabs, dnorm, sizeof(dnorm)) != 0) {
                fprintf(stderr, "Target path too long: %s\n", dst_arg);
                return 1;
            }
            if (trailing_slash) {
                snprintf(dst_dir, sizeof(dst_dir), "%s", dnorm);
                snprintf(final_name, sizeof(final_name), "%s", src_name);
            } else {
                split_parent_name(dnorm, dst_dir, sizeof(dst_dir), final_name, sizeof(final_name));
            }
        }

        if (verify_only) {
            struct stat parent_st;
            if (lstat(dst_dir, &parent_st) != 0 || !S_ISDIR(parent_st.st_mode)) {
                fprintf(stderr,
                        "ecopy: verify-only target parent is not an existing directory: %s\n",
                        dst_dir);
                return 1;
            }
        } else if (mkdir_p_dir(dst_dir, 0777, NULL) != 0) {
            return 1;
        }
        copy_policy_set_destination(0, 0);
        if (snprintf(file_enqueue_dst, sizeof(file_enqueue_dst), "%s/%s", dst_dir, src_name)
                >= (int)sizeof(file_enqueue_dst) ||
            snprintf(file_final_dst, sizeof(file_final_dst), "%s/%s", dst_dir, final_name)
                >= (int)sizeof(file_final_dst)) {
            fprintf(stderr, "Target path too long: %s\n", dst_arg);
            return 1;
        }
        if (strcmp(src_abs, file_final_dst) == 0) {
            fprintf(stderr, "Source and destination are the same file: %s\n", src_abs);
            return 1;
        }
    }

    raise_open_file_limit(verbose);

    stats_init();
    verify_configure(verify_metadata, verify_data, verify_pct, verify_skipped,
                     verify_seed_value, verify_seed_set, verify_workers);
    stats_set_verify_runtime(verify_only, verify_workers, 0, 0);
    if (verify_only) {
        int verify_failed;
        if (verbose) {
            printf("Verification-only mode: %d workers, metadata=%s, data=%s (%.3f%%)\n",
                   verify_workers, verify_metadata ? "yes" : "no",
                   verify_data ? "yes" : "no", verify_pct);
        }
        if (progress_start() != 0) {
            sshx_disconnect();
            return 1;
        }
        stats_set_verification_started();
        verify_failed = verify_run_tree(src_abs,
                                        src_is_dir ? dst_root : file_final_dst,
                                        remote, src_is_dir) != 0;
        stats_set_verification_done();
        sshx_disconnect();
        progress_stop();
        stats_set_shutdown_done();
        printf("\n");
        stats_print_final(verbose);
        return verify_failed ? 1 : 0;
    }
    workers_set_collect_wait_timing(verbose);
    if (workers_start() != 0) { sshx_disconnect(); return 1; }
    if (verbose) {
        workers_print_startup_config();
    }
    if (progress_start() != 0) { workers_stop(); sshx_disconnect(); return 1; }

    int enqueue_failed = 0;
    if (src_is_dir) {
        if (traversal_start(src_abs, dst_root) != 0) {
            progress_stop();
            workers_stop();
            sshx_disconnect();
            return 1;
        }
        traversal_wait();
    } else {
        enqueue_failed = (enqueue_single_file(src_parent, src_name, src_abs,
                                              file_enqueue_dst, &st, remote) != 0);
        stats_set_traversal_done();
    }
    workers_stop();
    int finalize_failed = src_is_dir ? (traversal_finalize_metadata() != 0) : enqueue_failed;

    /* Single-file local rename: the worker landed the file under its source
     * name; move it to the requested final name if they differ. */
    if (!finalize_failed && !src_is_dir && !remote &&
        strcmp(file_enqueue_dst, file_final_dst) != 0) {
        if (rename(file_enqueue_dst, file_final_dst) != 0) {
            perror(file_final_dst);
            finalize_failed = 1;
        } else if (verify_retarget_path(file_enqueue_dst, file_final_dst) != 0) {
            fprintf(stderr, "ecopy: unable to update verification target path\n");
            finalize_failed = 1;
        }
    }

    /*
     * Remote runs are fire-and-forget: every PUTFILE / MKDIR / SETMETA has been
     * sent by now (workers stopped, metadata finalized). A final barrier drains
     * the server, flushes to stable storage, and reports any deferred errors.
     */
    int remote_failed = 0;
    if (remote) {
        remote_failed = (sshx_flush() != 0);
    }
    stats_set_copy_complete();
    int verify_failed = 0;
    if (!finalize_failed && !remote_failed && verify_enabled()) {
        stats_set_verification_started();
        verify_failed = (verify_run_queued(remote) != 0);
        stats_set_verification_done();
    } else {
        verify_queue_clear();
    }
    if (finalize_failed) {
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

    if (traversal_status() != 0 || workers_status() != 0 || remote_failed ||
        verify_failed) {
        return 1;
    }
    return 0;
}
