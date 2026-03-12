#ifndef FS_UTIL_H
#define FS_UTIL_H

#include <sys/stat.h>
#include <sys/types.h>

int ensure_dir_exists(const char *path, mode_t mode);
int set_file_times(const char *dst, const struct stat *src_st);
int same_size_and_mtime(const struct stat *a, const struct stat *b);
int finalize_copied_file(const char *dst, const struct stat *src_st);
int open_read_maybe_direct(const char *path, int *used_direct);
int open_write_maybe_direct(const char *path, mode_t mode, int *used_direct);

#endif
