#define _GNU_SOURCE
#include "traversal.h"
#include "config.h"
#include "stats.h"
#include "fs_util.h"
#include "workers.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

typedef struct dir_node {
    char src[PATH_MAX];
    char dst[PATH_MAX];
    struct dir_node *next;
} dir_node_t;

static pthread_t g_thread;
static int g_status = 0;
static char g_src_root[PATH_MAX];
static char g_dst_root[PATH_MAX];

static int process_file(const char *src, const char *dst) {
    struct stat src_st, dst_st;
    if (lstat(src, &src_st) != 0) { perror(src); return -1; }
    if (!S_ISREG(src_st.st_mode)) return 0;
    stats_inc_files_seen();
    if (lstat(dst, &dst_st) == 0) {
        if (S_ISREG(dst_st.st_mode) && same_size_and_mtime(&src_st, &dst_st)) {
            stats_inc_files_skipped();
            return 0;
        }
    }
    if (src_st.st_size > PARALLEL_THRESHOLD) return workers_enqueue_large_file(src, dst, &src_st);
    return workers_enqueue_small_file(src, dst, &src_st);
}

static int push_dir(dir_node_t **head, dir_node_t **tail, const char *src, const char *dst) {
    dir_node_t *n = calloc(1, sizeof(*n));
    if (!n) { perror("calloc"); return -1; }
    snprintf(n->src, sizeof(n->src), "%s", src);
    snprintf(n->dst, sizeof(n->dst), "%s", dst);
    if (*tail) (*tail)->next = n; else *head = n;
    *tail = n;
    return 0;
}

static void *traversal_main(void *arg) {
    (void)arg;
    dir_node_t *head = NULL, *tail = NULL;
    if (push_dir(&head, &tail, g_src_root, g_dst_root) != 0) { g_status = 1; return NULL; }
    while (head) {
        dir_node_t *node = head; head = node->next; if (!head) tail = NULL;
        struct stat st;
        if (lstat(node->src, &st) != 0) { perror(node->src); free(node); g_status = 1; continue; }
        stats_inc_dirs_seen();
        if (ensure_dir_exists(node->dst, st.st_mode & 07777) != 0) { free(node); g_status = 1; continue; }
        DIR *dir = opendir(node->src);
        if (!dir) { perror(node->src); free(node); g_status = 1; continue; }
        struct dirent *entry;
        char src_path[PATH_MAX], dst_path[PATH_MAX];
        while ((entry = readdir(dir)) != NULL) {
            if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
            if (snprintf(src_path, sizeof(src_path), "%s/%s", node->src, entry->d_name) >= (int)sizeof(src_path)) continue;
            if (snprintf(dst_path, sizeof(dst_path), "%s/%s", node->dst, entry->d_name) >= (int)sizeof(dst_path)) continue;
            if (lstat(src_path, &st) != 0) { perror(src_path); g_status = 1; continue; }
            if (S_ISDIR(st.st_mode)) {
                if (push_dir(&head, &tail, src_path, dst_path) != 0) g_status = 1;
            } else if (S_ISREG(st.st_mode)) {
                if (process_file(src_path, dst_path) != 0) g_status = 1;
            }
        }
        closedir(dir);
        free(node);
    }
    return NULL;
}

int traversal_start(const char *src_dir, const char *dst_dir) {
    snprintf(g_src_root, sizeof(g_src_root), "%s", src_dir);
    snprintf(g_dst_root, sizeof(g_dst_root), "%s", dst_dir);
    g_status = 0;
    if (pthread_create(&g_thread, NULL, traversal_main, NULL) != 0) {
        perror("pthread_create");
        return -1;
    }
    return 0;
}

void traversal_wait(void) { pthread_join(g_thread, NULL); }
int traversal_status(void) { return g_status; }
