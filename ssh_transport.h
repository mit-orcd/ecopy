/*
 * ssh_transport.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Client side of ecopy's SSH target support: parses ssh:// targets, spawns an
 * `ecopy --server` peer over ssh, and exposes thread-safe, pipelined
 * destination operations used by traversal and the workers.
 */

#ifndef SSH_TRANSPORT_H
#define SSH_TRANSPORT_H

#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include "verify.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
    char user[256];   /* may be empty */
    char host[256];
    int  port;        /* 0 = default */
    char path[PATH_MAX];
} ssh_target_t;

/*
 * Parse "ssh://[user@]host[:port]/path" into out. Returns 0 on success, -1 if
 * the string is not an ssh:// URL or is malformed. A leading scheme is
 * required so plain local paths are never misinterpreted.
 */
int ssh_target_parse(const char *s, ssh_target_t *out);

/* True if the argument begins with the ssh:// scheme. */
int ssh_target_is_url(const char *s);

/* Select a server mode that requires an existing root and never creates it. */
void sshx_set_read_only(int read_only);

/*
 * Connect to the remote target: spawn ssh + `ecopy --server <path>`, perform
 * the version/capability handshake, and self-bootstrap the remote binary if it
 * is missing or an incompatible version. Returns 0 on success. On success the
 * remote destination backend becomes active (see sshx_active()).
 */
int sshx_connect(const ssh_target_t *t);

/* True once a remote destination is active for this run. */
int sshx_active(void);

/* Number of parallel SSH connections established for this run (>=1). */
int sshx_connection_count(void);

/*
 * Bind the calling thread to one connection of the pool (by index, taken mod
 * the connection count). All subsequent transport calls on this thread use that
 * connection. Threads that never bind use connection 0. Call once at thread
 * start so a streamed file's frames all land on one server.
 *
 * sshx_bind_thread() is the COPY role: when a verify partition is active it maps
 * only into the copy connections; otherwise it uses the whole pool.
 * sshx_bind_thread_verify() is the VERIFY role: when a partition is active it
 * maps only into the reserved verify connections; otherwise it uses the whole
 * pool (shared, as before).
 */
void sshx_bind_thread(int idx);
void sshx_bind_thread_verify(int idx);

/*
 * Request `nverify` additional connections dedicated to verify traffic, opened
 * in addition to the DIRECT_COPY_SSH_CONNECTIONS copy connections (never carved
 * from them, so copy parallelism is unaffected). Must be called before
 * sshx_connect(), which sizes and establishes the pool accordingly. If the
 * extra connections cannot be established, verify falls back to sharing the copy
 * connections. Clamped to [0, 16].
 */
void sshx_request_verify_connections(int nverify);

/* Clean shutdown: send BYE, join threads, reap ssh. Safe to call once. */
void sshx_disconnect(void);

/* The canonical remote root path (server-confirmed) for building child paths. */
const char *sshx_remote_root(void);

/*
 * Whether the destination root already existed before this run (reported at
 * handshake). 0 => fresh destination (per-directory bulk stat can be skipped).
 */
int sshx_remote_root_present(void);

/* ---- directory / metadata operations (synchronous, used by traversal) ---- */

/* Ensure a remote directory exists with mode. Returns 0 ok, -1 error. */
int sshx_mkdir(const char *path, mode_t mode);

/*
 * Stat a single remote path. Returns 1 if present (fills mode/size/mtime in
 * st), 0 if absent, -1 on protocol/error.
 */
int sshx_stat(const char *path, struct stat *st);

/*
 * Bulk-stat: base directory path + n child names, one round-trip. results[i]
 * is set to 1 present / 0 absent, and st[i] filled when present. Returns 0 on
 * success, -1 on error.
 */
int sshx_stat_bulk(const char *base, const char *const *names, int n,
                   int *results, struct stat *st);

/* Apply uid/gid/mode/atime/mtime from src_st to a remote path. */
int sshx_setmeta(const char *path, const struct stat *src_st, int is_dir);

/*
 * Recreate a symlink at link_path pointing at target (verbatim, never
 * dereferenced), preserving uid/gid/times from src_st. Fire-and-forget.
 */
int sshx_symlink(const char *link_path, const char *target,
                 const struct stat *src_st);

/*
 * Create a hard link at link_path referencing an already-transferred file at
 * primary_path. Fire-and-forget; must be sent after the primary's frames.
 */
int sshx_link(const char *primary_path, const char *link_path);

/*
 * Send a whole small file in one fire-and-forget frame (metadata from src_st +
 * path + data). The server creates parent dirs, writes, applies metadata, and
 * renames into place. No per-file round trip; failures surface at the next
 * barrier. Returns 0 if the frame was queued, -1 on transport failure.
 */
int sshx_putfile(const char *final_path, const struct stat *src_st,
                 mode_t parent_mode, const void *buf, size_t len, int inplace);

/*
 * Synchronization point on the calling thread's connection: drain its prior
 * fire-and-forget frames, optionally flush to stable storage, and collect the
 * cumulative remote error count. Returns 0 if no remote errors so far, -1
 * otherwise (a diagnostic with the first failing path is printed). Use
 * sshx_barrier_all at phase boundaries that must cover every connection.
 */
int sshx_barrier(int flush);

/* Barrier every connection in the pool (used at phase boundaries). */
int sshx_barrier_all(int flush);

/* Final drain + flush across all connections. */
int sshx_flush(void);

/* Queue a bounded remote verification batch. final reapplies the captured
 * metadata after data reads; metadata comparison follows --verify-metadata. */
int sshx_verify_batch(const char *path, const struct stat *src_st,
                      const verify_digest_t *digests, size_t count,
                      int is_dir, int check_metadata);

/* ---- per-file streaming (used by the remote copy path) ---- */

typedef struct sshx_file sshx_file_t;

/*
 * Begin writing a destination file. final_path is the absolute remote path.
 * expected_size lets the server preallocate non-sparse files. When sparse is
 * set the server preserves holes and skips preallocation. When inplace is set
 * the server writes the final name directly (no temp+rename). Returns a handle
 * or NULL on failure.
 */
sshx_file_t *sshx_file_begin(const char *final_path, mode_t mode,
                             mode_t parent_mode, off_t expected_size,
                             int sparse, int inplace);

/* Queue a positioned write (fire-and-forget, flow-controlled by the pipe). */
int sshx_file_write(sshx_file_t *f, const void *buf, size_t len, off_t offset);

/* Set the exact file size (used to preserve trailing holes of sparse files). */
int sshx_file_ftruncate(sshx_file_t *f, off_t size);

/*
 * Finish a file: fsync + metadata + rename-into-place on the server. This is
 * the synchronization point where any deferred write error surfaces. Returns 0
 * on success. Frees the handle.
 */
int sshx_file_commit(sshx_file_t *f, const struct stat *src_st);

/* Abort a file: close + unlink the temp on the server. Frees the handle. */
void sshx_file_abort(sshx_file_t *f);

#endif
