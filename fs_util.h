/*
 * fs_util.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#ifndef FS_UTIL_H
#define FS_UTIL_H

#include <stddef.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "types.h"

dir_handle_t *dir_handle_create(const char *src, const char *dst, int src_fd, int dst_fd);
void dir_handle_retain(dir_handle_t *dir);
void dir_handle_release(dir_handle_t *dir);
int ensure_dir_exists(const char *path, mode_t mode);
int same_size_and_mtime(const struct stat *a, const struct stat *b);
int preserve_path_metadata(const char *dst, const struct stat *src_st);
int preserve_fd_metadata(int fd, const char *path_for_warning, const struct stat *src_st);
int finalize_copied_file(const char *dst, const struct stat *src_st);
int finalize_copied_file_fd(int fd, const char *path_for_warning, const struct stat *src_st);
int direct_io_enabled(void);
int read_direct_io_enabled(void);
int write_direct_io_enabled(void);
int copy_file_range_enabled(void);
int open_read_maybe_direct(const char *path, int *used_direct);
int open_read_at_maybe_direct(int dir_fd,
                              const char *name,
                              const char *display_path,
                              const struct stat *expected_st,
                              int *used_direct);
int open_read_at_buffered(int dir_fd,
                          const char *name,
                          const char *display_path,
                          const struct stat *expected_st);
int open_write_maybe_direct(const char *path, mode_t mode, int *used_direct);
int open_write_existing_maybe_direct(const char *path, int *used_direct);
int open_write_existing_buffered(const char *path);
int create_temp_write_at_maybe_direct(int dir_fd,
                                      const char *display_path,
                                      mode_t mode,
                                      char *tmp_name,
                                      size_t tmp_name_sz,
                                      int *used_direct);
int create_final_write_at_maybe_direct(int dir_fd,
                                       const char *name,
                                       const char *display_path,
                                       mode_t mode,
                                       int *used_direct);
int open_temp_write_existing_at_maybe_direct(int dir_fd,
                                             const char *tmp_name,
                                             const char *display_path,
                                             int *used_direct);
int open_temp_write_existing_at_buffered(int dir_fd,
                                         const char *tmp_name,
                                         const char *display_path);
int preserve_path_metadata_at(int dir_fd,
                              const char *name,
                              const char *display_path,
                              const struct stat *src_st,
                              mode_t expected_type);
int rename_temp_to_final_at(int dir_fd,
                            const char *tmp_name,
                            const char *final_name,
                            const char *display_path);
void unlink_temp_at(int dir_fd, const char *tmp_name);

/* hardlink_create() return code: the two paths are on different filesystems. */
#define FS_LINK_EXDEV (-2)

/*
 * Recreate a symlink (never dereferenced) at dir_fd/name pointing at target,
 * preserving uid/gid and times. Uses temp+rename on an existing tree for atomic
 * overwrite. Returns 0 on success, -1 on error.
 */
int symlink_recreate_at(int dir_fd, const char *name, const char *display_path,
                        const char *target, const struct stat *src_st);

/*
 * Create a hard link at link_path referencing primary_path. Returns 0 on
 * success, FS_LINK_EXDEV for a cross-filesystem link, or -1 on other errors.
 */
int hardlink_create(const char *primary_path, const char *link_path);

#endif
