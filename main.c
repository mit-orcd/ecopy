#define _GNU_SOURCE
#include "stats.h"
#include "progress.h"
#include "workers.h"
#include "traversal.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s <source_dir> <target_dir>\n", prog);
}

int main(int argc, char **argv) {
    struct stat st;
    if (argc != 3) { usage(argv[0]); return 1; }
    if (lstat(argv[1], &st) != 0) { perror(argv[1]); return 1; }
    if (!S_ISDIR(st.st_mode)) { fprintf(stderr, "Source is not a directory: %s\n", argv[1]); return 1; }
    stats_init();
    if (workers_start() != 0) return 1;
    if (progress_start() != 0) { workers_stop(); return 1; }
    if (traversal_start(argv[1], argv[2]) != 0) { progress_stop(); workers_stop(); return 1; }
    traversal_wait();
    workers_stop();
    progress_stop();
    printf("\n");
    stats_print_final();
    return traversal_status() == 0 ? 0 : 1;
}
