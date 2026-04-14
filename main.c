#define _GNU_SOURCE
#include "stats.h"
#include "progress.h"
#include "workers.h"
#include "traversal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s [-v] <source_dir> <target_dir>\n", prog);
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

static int is_same_or_child_path(const char *base, const char *path) {
    size_t n = strlen(base);
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
    int verbose = 0;

    if (argc == 4 && strcmp(argv[1], "-v") == 0) {
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

    if (lstat(src_arg, &st) != 0) { perror(src_arg); return 1; }
    if (!S_ISDIR(st.st_mode)) { fprintf(stderr, "Source is not a directory: %s\n", src_arg); return 1; }

    if (to_canonical_dir_path(src_arg, src_abs, sizeof(src_abs)) != 0 ||
        to_canonical_dir_path(dst_arg, dst_abs, sizeof(dst_abs)) != 0) {
        return 1;
    }

    if (is_same_or_child_path(src_abs, dst_abs) || is_same_or_child_path(dst_abs, src_abs)) {
        fprintf(stderr, "Source and target directories overlap: %s <-> %s\n", src_abs, dst_abs);
        return 1;
    }

    stats_init();
    if (workers_start() != 0) return 1;
    if (verbose) {
        workers_print_startup_config();
    }
    if (progress_start() != 0) { workers_stop(); return 1; }
    if (traversal_start(src_abs, dst_abs) != 0) { progress_stop(); workers_stop(); return 1; }
    traversal_wait();
    workers_stop();
    if (traversal_finalize_metadata() != 0) {
        progress_stop();
        stats_set_shutdown_done();
        printf("\n");
        stats_print_final();
        workers_print_runtime_summary();
        return 1;
    }
    progress_stop();
    stats_set_shutdown_done();
    printf("\n");
    stats_print_final();
    workers_print_runtime_summary();

    if (traversal_status() != 0 || workers_status() != 0) {
        return 1;
    }
    return 0;
}
