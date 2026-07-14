/*
 * server.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Remote peer for ecopy SSH targets. Launched on the destination host as
 * `ecopy --server <root>` with its stdin/stdout wired through the SSH channel.
 * It executes destination filesystem operations on behalf of the client,
 * confining every path under <root> and using O_NOFOLLOW on leaf opens.
 *
 * A single reader thread pulls frames in arrival order. Fire-and-forget work
 * (PUTFILE/MKDIR) is handed to an apply pool so many latency-bound NFS RPCs run
 * concurrently; every other frame first drains the pool, then runs inline, so
 * ordering that matters (a directory's metadata after its files, streamed
 * files, barriers) is preserved. Deferred errors are reported at the next
 * BARRIER (fire-and-forget) or at COMMIT (streamed files).
 */

#define _GNU_SOURCE
#include "server.h"
#include "protocol.h"
#include "verify.h"
#include "third_party/blake3/blake3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdatomic.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* -------------------- open-file table -------------------- */

typedef struct open_file {
    uint64_t id;
    int fd;
    int inplace;
    int failed;
    int32_t err;      /* negative errno of the first failure */
    char tmp_path[PATH_MAX];
    char final_path[PATH_MAX];
    struct open_file *next;
} open_file_t;

static open_file_t *g_files;
static char g_root_norm[PATH_MAX];   /* lexically normalized confinement root */
static uint32_t g_caps;
static int g_root_present;           /* did the confinement root exist before we ran? */
static uid_t g_euid;                 /* our effective ids: skip no-op chown RPCs */
static gid_t g_egid;
static int g_preserve_times = 1;     /* client HELLO option: apply atime/mtime? */

/*
 * Deferred error log for fire-and-forget operations (PUTFILE/MKDIR/SETMETA).
 * These get no per-op reply; the client learns about failures at the next
 * BARRIER, which reports the cumulative count and the first failing path.
 */
static uint64_t g_err_count;
static int32_t  g_err_first;         /* negative errno of the first failure */
static char     g_err_first_path[PATH_MAX];
static pthread_mutex_t g_err_lock = PTHREAD_MUTEX_INITIALIZER;  /* pool workers log here */
static _Atomic int g_verify_atime_warned;
static uint64_t g_verify_metadata_mismatches;
static uint64_t g_verify_data_mismatches;
static uint64_t g_verify_zero_mismatches;
static uint64_t g_verify_io_failures;
static uint64_t g_verify_malformed_batches;

static void log_op_error(const char *path, int err)
{
    pthread_mutex_lock(&g_err_lock);
    if (g_err_count == 0) {
        g_err_first = -(err > 0 ? err : -err);
        snprintf(g_err_first_path, sizeof(g_err_first_path), "%s", path ? path : "");
    }
    g_err_count++;
    pthread_mutex_unlock(&g_err_lock);
}

typedef enum {
    VERIFY_ERR_METADATA,
    VERIFY_ERR_DATA,
    VERIFY_ERR_ZERO,
    VERIFY_ERR_IO,
    VERIFY_ERR_MALFORMED
} verify_error_kind_t;

static void log_verify_error(verify_error_kind_t kind, const char *path, int err)
{
    pthread_mutex_lock(&g_err_lock);
    switch (kind) {
    case VERIFY_ERR_METADATA: g_verify_metadata_mismatches++; break;
    case VERIFY_ERR_DATA: g_verify_data_mismatches++; break;
    case VERIFY_ERR_ZERO: g_verify_zero_mismatches++; break;
    case VERIFY_ERR_IO: g_verify_io_failures++; break;
    case VERIFY_ERR_MALFORMED: g_verify_malformed_batches++; break;
    }
    if (g_err_count == 0) {
        g_err_first = -(err > 0 ? err : -err);
        snprintf(g_err_first_path, sizeof(g_err_first_path), "%s",
                 path ? path : "");
    }
    g_err_count++;
    pthread_mutex_unlock(&g_err_lock);
}

/*
 * Directory creation cache. The thread-local last-hit avoids locking for runs
 * of files in one directory. The shared hash table is the important part:
 * without it every apply worker independently ran mkdir -p for the same path,
 * multiplying synchronous NFS LOOKUP/MKDIR RPCs by the pool width.
 *
 * An entry is inserted as CREATING before the syscall. Other workers asking
 * for that exact directory wait on the entry, so only one mkdir sequence is
 * ever in flight per path. A wide table keeps chains short on million-dir
 * trees, while sharded locks let unrelated paths proceed concurrently.
 * Entries live for the server process lifetime.
 */
typedef enum {
    DIR_CREATING = 0,
    DIR_READY = 1,
    DIR_FAILED = 2
} dir_cache_state_t;

typedef struct dir_cache_entry {
    struct dir_cache_entry *next;
    pthread_cond_t ready;
    dir_cache_state_t state;
    int err;
    int mode_known;
    mode_t mode;
    char path[];
} dir_cache_entry_t;

#define DIR_CACHE_BUCKETS (1u << 20)
#define DIR_CACHE_SHARDS  256u
static dir_cache_entry_t *g_dir_cache[DIR_CACHE_BUCKETS];
static pthread_mutex_t g_dir_cache_locks[DIR_CACHE_SHARDS];
static int g_dir_cache_initialized;
static __thread char g_last_dir[PATH_MAX];

/*
 * Apply pool state (worker logic lives near server_main). g_pool_n == 0 means
 * no pool: fire-and-forget ops run inline on the reader thread as before.
 */
static pthread_mutex_t g_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_pool_work_cv  = PTHREAD_COND_INITIALIZER; /* job available */
static pthread_cond_t  g_pool_drain_cv = PTHREAD_COND_INITIALIZER; /* fully drained */
static pthread_cond_t  g_pool_space_cv = PTHREAD_COND_INITIALIZER; /* room to submit */
static int g_job_queued;     /* jobs waiting to be taken */
static int g_job_active;     /* jobs currently being processed */
static int g_pool_stop;
static pthread_t *g_pool;
static int g_pool_n;         /* 0 => no pool, run inline (single-threaded) */
static int g_pool_limit;     /* max outstanding (queued + active) jobs */

static void pool_start(int n);
static void pool_submit(uint8_t type, uint8_t *payload, uint32_t plen);
static void pool_drain(void);
static void pool_shutdown(void);

static open_file_t *file_find(uint64_t id)
{
    for (open_file_t *f = g_files; f; f = f->next) {
        if (f->id == id) return f;
    }
    return NULL;
}

static void file_remove(open_file_t *f)
{
    open_file_t **pp = &g_files;
    while (*pp) {
        if (*pp == f) { *pp = f->next; break; }
        pp = &(*pp)->next;
    }
    free(f);
}

/* -------------------- helpers -------------------- */

static mode_t copy_data_mode(mode_t final_mode)
{
    return (final_mode & 0777) | S_IRUSR | S_IWUSR;
}

static mode_t copy_dir_mode(mode_t final_mode)
{
    mode_t mode = final_mode & 07777;
    if ((mode & (S_IWUSR | S_IXUSR)) != (S_IWUSR | S_IXUSR)) {
        mode |= S_IRWXU;
    }
    return mode;
}

/* Lexically normalize an absolute path (resolve . and ..), no filesystem access. */
static int normalize_abs(const char *in, char *out, size_t outsz)
{
    if (!in || in[0] != '/' || outsz < 2) { errno = EINVAL; return -1; }

    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s", in) >= (int)sizeof(tmp)) { errno = ENAMETOOLONG; return -1; }

    out[0] = '/'; out[1] = '\0';
    size_t olen = 1;

    char *save = NULL;
    for (char *tok = strtok_r(tmp, "/", &save); tok; tok = strtok_r(NULL, "/", &save)) {
        if (strcmp(tok, ".") == 0) continue;
        if (strcmp(tok, "..") == 0) {
            char *slash = strrchr(out, '/');
            if (slash && slash != out) *slash = '\0';
            else out[1] = '\0';
            olen = strlen(out);
            continue;
        }
        size_t tl = strlen(tok);
        size_t need = (olen == 1 ? 0 : 1) + tl;
        if (olen + need + 1 > outsz) { errno = ENAMETOOLONG; return -1; }
        if (olen != 1) out[olen++] = '/';
        memcpy(out + olen, tok, tl);
        olen += tl;
        out[olen] = '\0';
    }
    return 0;
}

/* True if normalized 'path' is equal to or under g_root_norm. */
static int path_under_root(const char *norm)
{
    size_t rl = strlen(g_root_norm);
    if (strcmp(g_root_norm, "/") == 0) return norm[0] == '/';
    if (strncmp(norm, g_root_norm, rl) != 0) return 0;
    return norm[rl] == '\0' || norm[rl] == '/';
}

/* Normalize + confine. Returns 0 and fills out, or -1 (errno set) if it escapes. */
static int resolve_path(const char *in, char *out, size_t outsz)
{
    if (normalize_abs(in, out, outsz) != 0) return -1;
    if (!path_under_root(out)) { errno = EACCES; return -1; }
    return 0;
}

static void split_dir_base(const char *path, char *dir, size_t dirsz, char *base, size_t basesz)
{
    const char *slash = strrchr(path, '/');
    if (!slash) {
        snprintf(dir, dirsz, ".");
        snprintf(base, basesz, "%s", path);
        return;
    }
    size_t dl = (size_t)(slash - path);
    if (dl == 0) { snprintf(dir, dirsz, "/"); }
    else { if (dl >= dirsz) dl = dirsz - 1; memcpy(dir, path, dl); dir[dl] = '\0'; }
    snprintf(base, basesz, "%s", slash + 1);
}

static int reply_status(uint64_t id, int32_t status)
{
    uint8_t buf[8];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, (uint32_t)status);
    return frame_write(STDOUT_FILENO, MSG_STATUS, id, buf, (uint32_t)e.len);
}

static void encode_stat(penc_t *e, const char *path)
{
    struct stat st;
    if (lstat(path, &st) == 0) {
        penc_u8(e, 1);
        penc_u32(e, (uint32_t)st.st_mode);
        penc_i64(e, (int64_t)st.st_size);
        penc_i64(e, (int64_t)st.st_mtim.tv_sec);
        penc_i64(e, (int64_t)st.st_mtim.tv_nsec);
    } else {
        penc_u8(e, 0);
        penc_u32(e, 0);
        penc_i64(e, 0);
        penc_i64(e, 0);
        penc_i64(e, 0);
    }
}

/* mkdir -p for the confinement root at startup. */
static int mkdir_p(const char *path)
{
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s", path) >= (int)sizeof(tmp)) { errno = ENAMETOOLONG; return -1; }
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0777) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0777) != 0 && errno != EEXIST) return -1;
    return 0;
}

static uint32_t dir_cache_hash(const char *path)
{
    uint64_t h = 1469598103934665603ULL;
    const unsigned char *p = (const unsigned char *)path;

    while (*p) {
        h ^= (uint64_t)*p++;
        h *= 1099511628211ULL;
    }
    return (uint32_t)(h ^ (h >> 32));
}

static dir_cache_entry_t *dir_cache_find_locked(const char *path, uint32_t bucket)
{
    for (dir_cache_entry_t *e = g_dir_cache[bucket]; e; e = e->next) {
        if (strcmp(e->path, path) == 0) {
            return e;
        }
    }
    return NULL;
}

static int dir_cache_init(void)
{
    for (size_t i = 0; i < DIR_CACHE_SHARDS; i++) {
        if (pthread_mutex_init(&g_dir_cache_locks[i], NULL) != 0) {
            for (size_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&g_dir_cache_locks[j]);
            }
            return -1;
        }
    }
    g_dir_cache_initialized = 1;
    return 0;
}

static int existing_path_is_dir(const char *path)
{
    struct stat st;
    if (lstat(path, &st) != 0) {
        return -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    return 0;
}

/*
 * Optimistic mkdir: the common case is either that the exact directory
 * already exists, or only its leaf is missing. Walk upward only after ENOENT,
 * and use the same shared cache recursively so sibling creators single-flight
 * their common parents too.
 */
static int ensure_dir_cached_mode(const char *dir, mode_t mode)
{
    uint32_t hash;
    uint32_t bucket;
    uint32_t shard;
    pthread_mutex_t *lock;
    dir_cache_entry_t *entry;
    size_t len;
    int rc = -1;
    int saved_err = 0;

    if (strcmp(dir, g_last_dir) == 0) {
        return 0;
    }

    hash = dir_cache_hash(dir);
    bucket = hash & (DIR_CACHE_BUCKETS - 1u);
    shard = hash & (DIR_CACHE_SHARDS - 1u);
    lock = &g_dir_cache_locks[shard];
    pthread_mutex_lock(lock);
    entry = dir_cache_find_locked(dir, bucket);
    if (entry) {
        while (entry->state == DIR_CREATING) {
            pthread_cond_wait(&entry->ready, lock);
        }
        if (entry->state == DIR_READY) {
            snprintf(g_last_dir, sizeof(g_last_dir), "%s", dir);
            pthread_mutex_unlock(lock);
            return 0;
        }
        saved_err = entry->err;
        pthread_mutex_unlock(lock);
        errno = saved_err;
        return -1;
    }

    len = strlen(dir);
    entry = malloc(sizeof(*entry) + len + 1);
    if (!entry) {
        pthread_mutex_unlock(lock);
        errno = ENOMEM;
        return -1;
    }
    entry->state = DIR_CREATING;
    entry->err = 0;
    entry->mode_known = 0;
    entry->mode = 0;
    memcpy(entry->path, dir, len + 1);
    pthread_cond_init(&entry->ready, NULL);
    entry->next = g_dir_cache[bucket];
    g_dir_cache[bucket] = entry;
    pthread_mutex_unlock(lock);

    if (mkdir(dir, mode) == 0) {
        entry->mode_known = 1;
        entry->mode = mode & 07777;
        rc = 0;
    } else if (errno == EEXIST) {
        if (existing_path_is_dir(dir) == 0) {
            rc = 0;
        } else {
            saved_err = errno;
        }
    } else if (errno == ENOENT) {
        char parent[PATH_MAX], base[PATH_MAX];
        split_dir_base(dir, parent, sizeof(parent), base, sizeof(base));
        if (strcmp(parent, dir) != 0 &&
            ensure_dir_cached_mode(parent, 0777) == 0) {
            if (mkdir(dir, mode) == 0) {
                entry->mode_known = 1;
                entry->mode = mode & 07777;
                rc = 0;
            } else if (errno == EEXIST) {
                if (existing_path_is_dir(dir) == 0) {
                    rc = 0;
                } else {
                    saved_err = errno;
                }
            } else {
                saved_err = errno;
            }
        } else {
            saved_err = errno;
        }
    } else {
        saved_err = errno;
    }

    pthread_mutex_lock(lock);
    if (rc == 0) {
        entry->state = DIR_READY;
        snprintf(g_last_dir, sizeof(g_last_dir), "%s", dir);
    } else {
        entry->state = DIR_FAILED;
        entry->err = saved_err ? saved_err : EIO;
    }
    pthread_cond_broadcast(&entry->ready);
    pthread_mutex_unlock(lock);

    if (rc != 0) {
        errno = entry->err;
    }
    return rc;
}

static int dir_cache_mode_matches(const char *dir, mode_t mode)
{
    uint32_t hash = dir_cache_hash(dir);
    uint32_t bucket = hash & (DIR_CACHE_BUCKETS - 1u);
    pthread_mutex_t *lock = &g_dir_cache_locks[hash & (DIR_CACHE_SHARDS - 1u)];
    int matches = 0;

    pthread_mutex_lock(lock);
    dir_cache_entry_t *entry = dir_cache_find_locked(dir, bucket);
    if (entry && entry->state == DIR_READY && entry->mode_known &&
        entry->mode == (mode & 07777)) {
        matches = 1;
    }
    pthread_mutex_unlock(lock);
    return matches;
}

static void dir_cache_destroy(void)
{
    for (size_t i = 0; i < DIR_CACHE_BUCKETS; i++) {
        dir_cache_entry_t *entry = g_dir_cache[i];
        while (entry) {
            dir_cache_entry_t *next = entry->next;
            pthread_cond_destroy(&entry->ready);
            free(entry);
            entry = next;
        }
        g_dir_cache[i] = NULL;
    }
    if (g_dir_cache_initialized) {
        for (size_t i = 0; i < DIR_CACHE_SHARDS; i++) {
            pthread_mutex_destroy(&g_dir_cache_locks[i]);
        }
        g_dir_cache_initialized = 0;
    }
}

/*
 * Apply uid/gid/mode/times to an open fd, minimizing SETATTR RPCs (each of
 * fchown/fchmod/futimens is a separate round trip on NFS):
 *   - skip fchown when the target owner already matches ours (the file was
 *     created by us, so it already has these ids -- a no-op RPC otherwise);
 *   - skip fchmod when the caller created the file with the final mode already
 *     (mode_is_set); directories and re-used temps still pass 0.
 *   - skip futimens entirely when the client asked not to preserve times.
 */
static int apply_meta(int fd, uint32_t uid, uint32_t gid, uint32_t mode,
                      int64_t at_s, int64_t at_ns, int64_t mt_s, int64_t mt_ns,
                      int mode_is_set, const char **failed_op)
{
    int first_err = 0;
    const char *first_op = NULL;
    if ((uid_t)uid != g_euid || (gid_t)gid != g_egid) {
        if (fchown(fd, (uid_t)uid, (gid_t)gid) != 0) {
            first_err = errno;
            first_op = "fchown";
        }
    }
    if (!mode_is_set) {
        if (fchmod(fd, (mode_t)(mode & 07777)) != 0 && first_err == 0) {
            first_err = errno;
            first_op = "fchmod";
        }
    }
    if (g_preserve_times) {
        struct timespec ts[2];
        ts[0].tv_sec = (time_t)at_s; ts[0].tv_nsec = (long)at_ns;
        ts[1].tv_sec = (time_t)mt_s; ts[1].tv_nsec = (long)mt_ns;
        if (futimens(fd, ts) != 0 && first_err == 0) {
            first_err = errno;
            first_op = "futimens";
        }
    }
    if (failed_op) *failed_op = first_op;
    return first_err == 0 ? 0 : -first_err;
}

static const char *metadata_mismatch_stat(const struct stat *st,
                                          uint32_t uid, uint32_t gid,
                                          uint32_t mode,
                                          int64_t at_s, int64_t at_ns,
                                          int64_t mt_s, int64_t mt_ns,
                                          int is_dir, int check_times)
{
    if ((!is_dir && !S_ISREG(st->st_mode)) ||
        (is_dir && !S_ISDIR(st->st_mode))) return "type";
    if ((st->st_mode & 07777) != (mode_t)(mode & 07777)) return "mode";
    if (st->st_uid != (uid_t)uid) return "uid";
    if (st->st_gid != (gid_t)gid) return "gid";
    if (check_times &&
        ((int64_t)st->st_atim.tv_sec != at_s ||
         (int64_t)st->st_atim.tv_nsec != at_ns)) return "atime";
    if (check_times &&
        ((int64_t)st->st_mtim.tv_sec != mt_s ||
         (int64_t)st->st_mtim.tv_nsec != mt_ns)) return "mtime";
    return NULL;
}

/* -------------------- message handlers -------------------- */

static int handle_hello(uint64_t id, const uint8_t *payload, uint32_t plen, const char *root)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint32_t cver = pdec_u32(&d);
    uint32_t ccaps = pdec_u32(&d);
    char cname[128]; char cpath[PATH_MAX];
    (void)pdec_str(&d, cname, sizeof(cname));
    (void)pdec_str(&d, cpath, sizeof(cpath));
    /* v2 appends the client's requested apply-pool size; absent -> single. */
    uint32_t want_threads = pdec_u32(&d);
    if (d.error) want_threads = 0;
    /* ...followed by client option bits. A missing PRESERVE_TIMES bit from an
     * older client is treated as "preserve" so default behavior is unchanged. */
    uint32_t opts = pdec_u32(&d);
    g_preserve_times = (d.error || (opts & ECOPY_OPT_PRESERVE_TIMES)) ? 1 : 0;

    g_caps = ccaps;

    /* Start the apply pool (once). Bounded so a hostile/confused peer can't ask
     * us to spawn thousands of threads. */
    if (g_pool_n == 0 && !g_pool) {
        int n = (int)want_threads;
        if (n > 256) n = 256;
        pool_start(n);
    }

    uint8_t buf[512];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, ECOPY_PROTO_VERSION);
    penc_u32(&e, ECOPY_CAP_FALLOCATE | ECOPY_CAP_SPARSE);
    struct utsname u;
    char uname_s[160];
    if (uname(&u) == 0) snprintf(uname_s, sizeof(uname_s), "%s %s", u.sysname, u.machine);
    else snprintf(uname_s, sizeof(uname_s), "unknown unknown");
    penc_str(&e, uname_s);
    penc_str(&e, g_root_norm);
    /* Report whether the destination root already existed, so the client can
     * pick fresh (skip per-directory bulk stat) vs incremental mode. */
    penc_u8(&e, (uint8_t)(g_root_present ? 1 : 0));

    /* If the client speaks a different major version, still answer so it can
     * decide to bootstrap; the version field is what it checks. */
    (void)cver; (void)root;
    return frame_write(STDOUT_FILENO, MSG_HELLO_OK, id, buf, (uint32_t)e.len);
}

static void handle_open(const uint8_t *payload, uint32_t plen)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint64_t fid = pdec_u64(&d);
    uint32_t mode = pdec_u32(&d);
    int64_t expected = pdec_i64(&d);
    uint32_t flags = pdec_u32(&d);
    uint32_t parent_mode = pdec_u32(&d);
    char final_in[PATH_MAX];
    if (pdec_str(&d, final_in, sizeof(final_in)) != 0 || d.error) return;

    open_file_t *f = calloc(1, sizeof(*f));
    if (!f) return;
    f->id = fid;
    f->fd = -1;
    f->inplace = (flags & ECOPY_OPEN_INPLACE) ? 1 : 0;

    char finalp[PATH_MAX];
    if (resolve_path(final_in, finalp, sizeof(finalp)) != 0) {
        f->failed = 1; f->err = -errno;
        f->next = g_files; g_files = f;
        return;
    }
    snprintf(f->final_path, sizeof(f->final_path), "%s", finalp);

    /* Ensure the parent exists. The client no longer sends a per-directory
     * MKDIR for directories that contain files, so a streamed file may be the
     * thing that first materializes its directory. */
    char dir[PATH_MAX], base[PATH_MAX];
    split_dir_base(finalp, dir, sizeof(dir), base, sizeof(base));
    if (ensure_dir_cached_mode(dir, copy_dir_mode((mode_t)parent_mode)) != 0) {
        f->failed = 1; f->err = -errno;
        f->next = g_files; g_files = f;
        return;
    }

    int fd;
    if (f->inplace) {
        snprintf(f->tmp_path, sizeof(f->tmp_path), "%s", finalp);
        fd = open(finalp, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC,
                  copy_data_mode((mode_t)mode));
    } else {
        int need = snprintf(f->tmp_path, sizeof(f->tmp_path), "%s/.ecopy.tmp.%u.%llu",
                            dir, (unsigned)getpid(), (unsigned long long)fid);
        if (need < 0 || need >= (int)sizeof(f->tmp_path)) {
            f->failed = 1; f->err = -ENAMETOOLONG;
            f->next = g_files; g_files = f;
            return;
        }
        fd = open(f->tmp_path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                  copy_data_mode((mode_t)mode));
    }

    if (fd < 0) {
        f->failed = 1; f->err = -errno;
        f->next = g_files; g_files = f;
        return;
    }
    f->fd = fd;

    /* Preallocate non-sparse files to keep block allocation off the write path. */
    if (!(flags & ECOPY_OPEN_SPARSE) && expected > 0) {
        if (fallocate(fd, 0, 0, (off_t)expected) != 0) {
            if (errno == EOPNOTSUPP || errno == ENOSYS) {
                (void)ftruncate(fd, (off_t)expected);
            }
        }
    }

    f->next = g_files;
    g_files = f;
}

static void handle_write(const uint8_t *payload, uint32_t plen)
{
    if (plen < 16) return;
    pdec_t d; pdec_init(&d, payload, plen);
    uint64_t fid = pdec_u64(&d);
    int64_t offset = pdec_i64(&d);
    const uint8_t *data = payload + 16;
    size_t len = plen - 16;

    open_file_t *f = file_find(fid);
    if (!f) return;
    if (f->failed || f->fd < 0) return;

    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(f->fd, data + done, len - done, offset + (off_t)done);
        if (w < 0) {
            if (errno == EINTR) continue;
            f->failed = 1; f->err = -errno;
            return;
        }
        if (w == 0) { f->failed = 1; f->err = -EIO; return; }
        done += (size_t)w;
    }
}

static void handle_ftruncate(const uint8_t *payload, uint32_t plen)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint64_t fid = pdec_u64(&d);
    int64_t size = pdec_i64(&d);
    open_file_t *f = file_find(fid);
    if (!f || f->failed || f->fd < 0) return;
    if (ftruncate(f->fd, (off_t)size) != 0) {
        f->failed = 1; f->err = -errno;
    }
}

static int handle_commit(uint64_t id, const uint8_t *payload, uint32_t plen)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint64_t fid = pdec_u64(&d);
    uint32_t uid = pdec_u32(&d);
    uint32_t gid = pdec_u32(&d);
    uint32_t mode = pdec_u32(&d);
    int64_t at_s = pdec_i64(&d);
    int64_t at_ns = pdec_i64(&d);
    int64_t mt_s = pdec_i64(&d);
    int64_t mt_ns = pdec_i64(&d);

    open_file_t *f = file_find(fid);
    if (!f) return reply_status(id, -EBADF);

    int32_t status = 0;
    if (f->failed) {
        status = f->err ? f->err : -EIO;
    } else if (f->fd < 0) {
        status = -EBADF;
    } else {
        if (fsync(f->fd) != 0) status = -errno;
        if (status == 0) {
            status = apply_meta(f->fd, uid, gid, mode,
                                at_s, at_ns, mt_s, mt_ns, 0, NULL);
        }
        if (close(f->fd) != 0 && status == 0) status = -errno;
        f->fd = -1;
        if (status == 0 && !f->inplace) {
            if (rename(f->tmp_path, f->final_path) != 0) status = -errno;
        }
    }

    if (status != 0 && f->fd >= 0) { close(f->fd); f->fd = -1; }
    if (status != 0 && !f->inplace) (void)unlink(f->tmp_path);

    int rc = reply_status(id, status);
    file_remove(f);
    return rc;
}

static void handle_abort(const uint8_t *payload, uint32_t plen)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint64_t fid = pdec_u64(&d);
    open_file_t *f = file_find(fid);
    if (!f) return;
    if (f->fd >= 0) close(f->fd);
    if (!f->inplace) (void)unlink(f->tmp_path);
    file_remove(f);
}

/* Fire-and-forget in v2: create the directory, record failures in the log. */
static void handle_mkdir(const uint8_t *payload, uint32_t plen)
{
    char in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, in, sizeof(in)) != 0) return;
    uint32_t mode = pdec_u32(&d);
    if (d.error) return;

    char path[PATH_MAX];
    if (resolve_path(in, path, sizeof(path)) != 0) { log_op_error(in, errno); return; }

    /*
     * MKDIRs are applied in parallel and out of order by the pool, so a nested
     * directory can arrive before its parent. The shared cache creates missing
     * parents recursively and suppresses duplicate NFS RPCs from other workers.
     * Keep the leaf owner-writable until final SETMETA.
     */
    if (ensure_dir_cached_mode(path, (mode_t)((mode & 07777) | S_IRWXU)) != 0) {
        log_op_error(path, errno);
    }
}

static int handle_stat(uint64_t id, const uint8_t *payload, uint32_t plen)
{
    char in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, in, sizeof(in)) != 0) return reply_status(id, -EINVAL);

    char path[PATH_MAX];
    uint8_t buf[64];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    if (resolve_path(in, path, sizeof(path)) != 0) {
        /* Report as absent on confinement failure. */
        penc_u8(&e, 0); penc_u32(&e, 0); penc_i64(&e, 0); penc_i64(&e, 0); penc_i64(&e, 0);
    } else {
        encode_stat(&e, path);
    }
    return frame_write(STDOUT_FILENO, MSG_STAT_RESP, id, buf, (uint32_t)e.len);
}

static int handle_stat_bulk(uint64_t id, const uint8_t *payload, uint32_t plen)
{
    char base_in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, base_in, sizeof(base_in)) != 0) return reply_status(id, -EINVAL);
    uint32_t count = pdec_u32(&d);
    if (d.error || count > 1000000u) return reply_status(id, -EINVAL);

    char base[PATH_MAX];
    int base_ok = (resolve_path(base_in, base, sizeof(base)) == 0);

    /* Response buffer: count entries of 29 bytes each + 4 for count. */
    size_t rcap = 8 + (size_t)count * 40;
    uint8_t *rbuf = malloc(rcap);
    if (!rbuf) return reply_status(id, -ENOMEM);
    penc_t e; penc_init(&e, rbuf, rcap);
    penc_u32(&e, count);

    for (uint32_t i = 0; i < count; i++) {
        char name[PATH_MAX];
        if (pdec_str(&d, name, sizeof(name)) != 0) { free(rbuf); return reply_status(id, -EINVAL); }
        char full[PATH_MAX];
        if (base_ok && snprintf(full, sizeof(full), "%s/%s", base, name) < (int)sizeof(full)) {
            encode_stat(&e, full);
        } else {
            penc_u8(&e, 0); penc_u32(&e, 0); penc_i64(&e, 0); penc_i64(&e, 0); penc_i64(&e, 0);
        }
    }
    int rc = frame_write(STDOUT_FILENO, MSG_STAT_BULK_RESP, id, rbuf, (uint32_t)e.len);
    free(rbuf);
    return rc;
}

/* Fire-and-forget: apply metadata, record failures in the log. */
static void handle_setmeta(const uint8_t *payload, uint32_t plen)
{
    char in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, in, sizeof(in)) != 0) return;
    uint32_t uid = pdec_u32(&d);
    uint32_t gid = pdec_u32(&d);
    uint32_t mode = pdec_u32(&d);
    int64_t at_s = pdec_i64(&d);
    int64_t at_ns = pdec_i64(&d);
    int64_t mt_s = pdec_i64(&d);
    int64_t mt_ns = pdec_i64(&d);
    int is_dir = pdec_u8(&d) ? 1 : 0;
    if (d.error) return;

    char path[PATH_MAX];
    if (resolve_path(in, path, sizeof(path)) != 0) { log_op_error(in, errno); return; }

    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        /* Directories may need O_DIRECTORY on some systems; retry. */
        fd = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    }
    if (fd < 0) { log_op_error(path, errno); return; }
    int mode_is_set = is_dir && dir_cache_mode_matches(path, (mode_t)mode);
    if (((uid_t)uid != g_euid || (gid_t)gid != g_egid) &&
        (mode & (S_ISUID | S_ISGID))) {
        mode_is_set = 0; /* chown may clear special mode bits */
    }
    const char *failed_op = NULL;
    int status = apply_meta(fd, uid, gid, mode, at_s, at_ns, mt_s, mt_ns,
                            mode_is_set, &failed_op);
    if (close(fd) != 0 && status == 0) status = -errno;
    if (status != 0) {
        char diagnostic[PATH_MAX];
        if (failed_op) {
            snprintf(diagnostic, sizeof(diagnostic), "%.*s (%s)",
                     PATH_MAX - 32, path, failed_op);
        } else {
            snprintf(diagnostic, sizeof(diagnostic), "%s", path);
        }
        log_op_error(diagnostic, -status);
    }
}

/*
 * Whole small file in one fire-and-forget frame: metadata + path + data.
 * Creates parent dirs, writes a temp (or the final name for inplace), applies
 * metadata, and renames into place. No per-file fsync (durability is a flush at
 * the next BARRIER). Failures go to the error log.
 */
static void handle_putfile(const uint8_t *payload, uint32_t plen)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint32_t mode = pdec_u32(&d);
    uint32_t uid = pdec_u32(&d);
    uint32_t gid = pdec_u32(&d);
    int64_t at_s = pdec_i64(&d);
    int64_t at_ns = pdec_i64(&d);
    int64_t mt_s = pdec_i64(&d);
    int64_t mt_ns = pdec_i64(&d);
    uint32_t flags = pdec_u32(&d);
    uint32_t parent_mode = pdec_u32(&d);
    char final_in[PATH_MAX];
    if (pdec_str(&d, final_in, sizeof(final_in)) != 0 || d.error) return;

    const uint8_t *data = payload + d.off;
    size_t data_len = plen - d.off;

    char finalp[PATH_MAX];
    if (resolve_path(final_in, finalp, sizeof(finalp)) != 0) { log_op_error(final_in, errno); return; }

    char dir[PATH_MAX], base[PATH_MAX];
    split_dir_base(finalp, dir, sizeof(dir), base, sizeof(base));
    if (ensure_dir_cached_mode(dir, copy_dir_mode((mode_t)parent_mode)) != 0) {
        log_op_error(dir, errno);
        return;
    }

    int inplace = (flags & ECOPY_OPEN_INPLACE) ? 1 : 0;

    /*
     * If the owner may write (the common case: 0644/0755/...), create the file
     * with its final mode directly so no fchmod SETATTR is needed. A newly
     * created fd is writable regardless of the stored mode. Read-only targets
     * (no S_IWUSR) fall back to a scratch mode + fchmod so the write succeeds.
     * The server sets umask(0) so the create honors the mode exactly.
     */
    int mode_is_set = (mode & S_IWUSR) ? 1 : 0;
    mode_t create_mode = mode_is_set ? (mode_t)(mode & 07777) : copy_data_mode((mode_t)mode);

    char tmp_path[PATH_MAX];
    int fd;
    if (inplace) {
        snprintf(tmp_path, sizeof(tmp_path), "%s", finalp);
        fd = open(finalp, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, create_mode);
    } else {
        static _Atomic uint64_t put_seq;
        uint64_t seq = atomic_fetch_add(&put_seq, 1) + 1;
        int need = snprintf(tmp_path, sizeof(tmp_path), "%s/.ecopy.tmp.%u.p%llu",
                            dir, (unsigned)getpid(), (unsigned long long)seq);
        if (need < 0 || need >= (int)sizeof(tmp_path)) { log_op_error(finalp, ENAMETOOLONG); return; }
        fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, create_mode);
    }
    if (fd < 0) { log_op_error(finalp, errno); return; }

    int err = 0;
    const char *failed_op = NULL;
    size_t done = 0;
    while (done < data_len) {
        ssize_t w = write(fd, data + done, data_len - done);
        if (w < 0) { if (errno == EINTR) continue; err = errno; break; }
        if (w == 0) { err = EIO; break; }
        done += (size_t)w;
    }
    if (err == 0) {
        int status = apply_meta(fd, uid, gid, mode,
                                at_s, at_ns, mt_s, mt_ns, mode_is_set,
                                &failed_op);
        if (status != 0) err = -status;
    }
    if (close(fd) != 0 && err == 0) err = errno;
    if (err == 0 && !inplace) {
        if (rename(tmp_path, finalp) != 0) err = errno;
    }
    if (err != 0) {
        char diagnostic[PATH_MAX];
        if (!inplace) (void)unlink(tmp_path);
        if (failed_op) {
            snprintf(diagnostic, sizeof(diagnostic), "%.*s (%s)",
                     PATH_MAX - 32, finalp, failed_op);
        } else {
            snprintf(diagnostic, sizeof(diagnostic), "%s", finalp);
        }
        log_op_error(diagnostic, err);
    }
}

static int pread_exact(int fd, void *buf, size_t len, off_t offset)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = pread(fd, (uint8_t *)buf + done, len - done,
                          offset + (off_t)done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

/* Read-only verification job. It is safe to dispatch these concurrently. */
static void handle_verify_path(const uint8_t *payload, uint32_t plen)
{
    char in[PATH_MAX], path[PATH_MAX];
    pdec_t d;
    pdec_init(&d, payload, plen);
    if (pdec_str(&d, in, sizeof(in)) != 0) {
        log_verify_error(VERIFY_ERR_MALFORMED, "(malformed verify path)", EINVAL);
        return;
    }
    uint32_t uid = pdec_u32(&d);
    uint32_t gid = pdec_u32(&d);
    uint32_t mode = pdec_u32(&d);
    int64_t at_s = pdec_i64(&d);
    int64_t at_ns = pdec_i64(&d);
    int64_t mt_s = pdec_i64(&d);
    int64_t mt_ns = pdec_i64(&d);
    int64_t expected_size = pdec_i64(&d);
    uint8_t flags = pdec_u8(&d);
    uint32_t count = pdec_u32(&d);
    int is_dir = (flags & ECOPY_VERIFY_DIRECTORY) != 0;
    int check_metadata = (flags & ECOPY_VERIFY_METADATA) != 0;
    int check_times = (flags & ECOPY_VERIFY_PRESERVE_TIME) != 0;
    int sequential = (flags & ECOPY_VERIFY_SEQUENTIAL) != 0;
    if (d.error || expected_size < 0 || count > VERIFY_BATCH_MAX ||
        (flags & ~15u) != 0 || (is_dir && count != 0) ||
        resolve_path(in, path, sizeof(path)) != 0) {
        log_verify_error(VERIFY_ERR_MALFORMED, in, EINVAL);
        return;
    }

    /*
     * Metadata-only verification needs no file descriptor. On NFS, open()
     * commonly adds OPEN/ACCESS and lookup revalidation RPCs before fstat().
     * A single no-following stat is sufficient for directories and for files
     * when no data samples were requested.
     */
    if (count == 0) {
        struct stat st;
        if (fstatat(AT_FDCWD, path, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            log_verify_error(VERIFY_ERR_IO, path, errno);
            return;
        }
        const char *field = NULL;
        if ((is_dir ? !S_ISDIR(st.st_mode) : !S_ISREG(st.st_mode)) ||
            (!is_dir && st.st_size != (off_t)expected_size)) {
            field = "type/size";
        } else if (check_metadata) {
            field = metadata_mismatch_stat(&st, uid, gid, mode,
                                           at_s, at_ns, mt_s, mt_ns, is_dir,
                                           check_times);
        }
        if (field) {
            char diagnostic[PATH_MAX];
            snprintf(diagnostic, sizeof(diagnostic), "%.*s (%s)",
                     PATH_MAX - 32, path, field);
            log_verify_error(VERIFY_ERR_METADATA, diagnostic, EIO);
        }
        return;
    }

    int open_flags = O_RDONLY | O_NOFOLLOW | O_CLOEXEC;
    if (is_dir) open_flags |= O_DIRECTORY;
#ifdef O_NOATIME
    if (!is_dir && check_times) open_flags |= O_NOATIME;
#endif
    if (!is_dir && count > 0 && !check_times &&
        atomic_exchange(&g_verify_atime_warned, 1) == 0) {
        fprintf(stderr,
                "ecopy: remote verification data reads may update atime because "
                "timestamps are not being checked\n");
    }
    int fd = open(path, open_flags);
    if (fd < 0) {
        if (check_times && (errno == EPERM || errno == EACCES)) {
            fprintf(stderr,
                    "ecopy: remote verification cannot open %s with O_NOATIME: %s; "
                    "use --verify-metadata or --no-preserve-times\n",
                    path, strerror(errno));
        }
        log_verify_error(VERIFY_ERR_IO, path, errno);
        return;
    }
    if (!is_dir && count > 0) {
        (void)posix_fadvise(fd, 0, 0,
                           sequential ? POSIX_FADV_SEQUENTIAL
                                      : POSIX_FADV_RANDOM);
    }
    struct stat st;
    int failed = 0;
    int metadata_bad = 0;
    int data_bad = 0;
    int zero_bad = 0;
    int io_bad = 0;
    const char *field = NULL;
    int64_t bad_offset = -1;
    if (fstat(fd, &st) != 0) {
        failed = 1;
        io_bad = 1;
        field = "stat";
    } else if ((is_dir ? !S_ISDIR(st.st_mode) : !S_ISREG(st.st_mode)) ||
               (!is_dir && st.st_size != (off_t)expected_size)) {
        failed = 1;
        metadata_bad = 1;
        field = "type/size";
    }

    for (uint32_t i = 0; i < count && !d.error; i++) {
        int64_t offset = pdec_i64(&d);
        uint32_t length = pdec_u32(&d);
        uint8_t sample_flags = pdec_u8(&d);
        uint8_t expected[VERIFY_DIGEST_SIZE];
        uint8_t actual[VERIFY_DIGEST_SIZE];
        uint8_t data[VERIFY_BLOCK_SIZE];
        if (pdec_bytes(&d, expected, sizeof(expected)) != 0) break;
        if ((sample_flags & ~VERIFY_SAMPLE_EXPECT_ZERO) != 0 ||
            offset < 0 || length == 0 || length > VERIFY_BLOCK_SIZE ||
            offset > expected_size ||
            (uint64_t)length > (uint64_t)(expected_size - offset)) {
            d.error = 1;
            break;
        }
        if (!failed) {
            int read_failed = pread_exact(fd, data, length, (off_t)offset) != 0;
            int mismatch = read_failed;
            if (!mismatch && (sample_flags & VERIFY_SAMPLE_EXPECT_ZERO)) {
                for (uint32_t j = 0; j < length; j++) {
                    if (data[j] != 0) { mismatch = 1; break; }
                }
            } else if (!mismatch) {
                blake3_hash(data, length, actual);
                mismatch = memcmp(actual, expected, sizeof(actual)) != 0;
            }
            if (mismatch) {
                failed = 1;
                bad_offset = offset;
                if (read_failed) io_bad = 1;
                else if (sample_flags & VERIFY_SAMPLE_EXPECT_ZERO) zero_bad = 1;
                else data_bad = 1;
            }
        }
    }
    if (d.error || d.off != d.len) {
        failed = 1;
        log_verify_error(VERIFY_ERR_MALFORMED, path, EINVAL);
    }

    if (check_metadata && !io_bad) {
        const char *meta_field = metadata_mismatch_stat(
            &st, uid, gid, mode, at_s, at_ns, mt_s, mt_ns, is_dir,
            check_times);
        if (meta_field) {
            failed = 1;
            metadata_bad = 1;
            if (!field) field = meta_field;
        }
    }
    close(fd);
    if (failed) {
        char diagnostic[PATH_MAX];
        if (bad_offset >= 0) {
            snprintf(diagnostic, sizeof(diagnostic), "%.*s at offset %" PRId64,
                     PATH_MAX - 48, path, bad_offset);
        } else if (field) {
            snprintf(diagnostic, sizeof(diagnostic), "%.*s (%s)",
                     PATH_MAX - 32, path, field);
        } else {
            snprintf(diagnostic, sizeof(diagnostic), "%s", path);
        }
        if (metadata_bad) log_verify_error(VERIFY_ERR_METADATA, diagnostic, EIO);
        if (data_bad) log_verify_error(VERIFY_ERR_DATA, diagnostic, EIO);
        if (zero_bad) log_verify_error(VERIFY_ERR_ZERO, diagnostic, EIO);
        if (io_bad) log_verify_error(VERIFY_ERR_IO, diagnostic, EIO);
    }
}

/*
 * Drain point: all prior fire-and-forget frames have been processed by now
 * (single-threaded, in-order). Optionally flush, then report the cumulative
 * error count and the first failing path so the client can surface failures.
 */
static int handle_barrier(uint64_t id, const uint8_t *payload, uint32_t plen)
{
    pdec_t d; pdec_init(&d, payload, plen);
    uint8_t flush = pdec_u8(&d);

    if (flush) {
        sync();
    }

    uint8_t buf[PATH_MAX + 80];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, (uint32_t)(g_err_count ? g_err_first : 0));
    penc_u32(&e, (uint32_t)(g_err_count > 0xffffffffULL ? 0xffffffffU : g_err_count));
    penc_str(&e, g_err_first_path);
    penc_u64(&e, g_verify_metadata_mismatches);
    penc_u64(&e, g_verify_data_mismatches);
    penc_u64(&e, g_verify_zero_mismatches);
    penc_u64(&e, g_verify_io_failures);
    penc_u64(&e, g_verify_malformed_batches);
    return frame_write(STDOUT_FILENO, MSG_STATUS, id, buf, (uint32_t)e.len);
}

static void handle_unlink(const uint8_t *payload, uint32_t plen)
{
    char in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, in, sizeof(in)) != 0) return;
    char path[PATH_MAX];
    if (resolve_path(in, path, sizeof(path)) != 0) return;
    (void)unlink(path);
}

/* -------------------- apply pool -------------------- */

/*
 * A pool of worker threads that applies fire-and-forget PUTFILE, MKDIR, and
 * SETMETA operations in parallel. Over NFS every one of these blocks on
 * synchronous RPC latency, so issuing many in flight at once hides it.
 *
 * The reader thread submits those three message types. Other frames
 * (STAT, OPEN/WRITE/COMMIT, BARRIER, ...) drain the pool, then run inline:
 *   - a streamed OPEN sees its parent directory created;
 *   - a BARRIER's error aggregate and flush cover all prior work.
 * The client sends a barrier before directory finalization and between depth
 * levels, so pooled SETMETAs cannot overtake file writes or let a child update
 * its parent after the parent's final timestamp was applied.
 */
typedef struct pool_job {
    uint8_t type;
    uint8_t *payload;
    uint32_t plen;
    struct pool_job *next;
} pool_job_t;

static pool_job_t *g_job_head, *g_job_tail;

static void pool_run_job(pool_job_t *j)
{
    if (j->type == MSG_PUTFILE) {
        handle_putfile(j->payload, j->plen);
    } else if (j->type == MSG_MKDIR) {
        handle_mkdir(j->payload, j->plen);
    } else if (j->type == MSG_SETMETA) {
        handle_setmeta(j->payload, j->plen);
    } else if (j->type == MSG_VERIFY_PATH) {
        handle_verify_path(j->payload, j->plen);
    }
    free(j->payload);
    free(j);
}

static void *pool_worker(void *arg)
{
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&g_pool_lock);
        while (!g_job_head && !g_pool_stop) {
            pthread_cond_wait(&g_pool_work_cv, &g_pool_lock);
        }
        if (!g_job_head) {              /* stop requested and queue empty */
            pthread_mutex_unlock(&g_pool_lock);
            break;
        }
        pool_job_t *j = g_job_head;
        g_job_head = j->next;
        if (!g_job_head) g_job_tail = NULL;
        g_job_queued--;
        g_job_active++;
        pthread_mutex_unlock(&g_pool_lock);

        pool_run_job(j);

        pthread_mutex_lock(&g_pool_lock);
        g_job_active--;
        pthread_cond_signal(&g_pool_space_cv);
        if (g_job_queued == 0 && g_job_active == 0) {
            pthread_cond_broadcast(&g_pool_drain_cv);
        }
        pthread_mutex_unlock(&g_pool_lock);
    }
    return NULL;
}

/* Hand a PUTFILE/MKDIR/SETMETA frame to the pool; takes ownership of payload. Blocks
 * (back-pressuring the reader, hence the SSH channel) when too many are in
 * flight, bounding memory to roughly g_pool_limit whole small files. */
static void pool_submit(uint8_t type, uint8_t *payload, uint32_t plen)
{
    pool_job_t *j = malloc(sizeof(*j));
    if (!j) {                           /* out of memory: just run it inline */
        if (type == MSG_PUTFILE) handle_putfile(payload, plen);
        else if (type == MSG_MKDIR) handle_mkdir(payload, plen);
        else if (type == MSG_SETMETA) handle_setmeta(payload, plen);
        else if (type == MSG_VERIFY_PATH) handle_verify_path(payload, plen);
        free(payload);
        return;
    }
    j->type = type; j->payload = payload; j->plen = plen; j->next = NULL;

    pthread_mutex_lock(&g_pool_lock);
    while (g_job_queued + g_job_active >= g_pool_limit) {
        pthread_cond_wait(&g_pool_space_cv, &g_pool_lock);
    }
    if (g_job_tail) g_job_tail->next = j; else g_job_head = j;
    g_job_tail = j;
    g_job_queued++;
    pthread_cond_signal(&g_pool_work_cv);
    pthread_mutex_unlock(&g_pool_lock);
}

/* Block until every submitted job has finished. */
static void pool_drain(void)
{
    pthread_mutex_lock(&g_pool_lock);
    while (g_job_queued > 0 || g_job_active > 0) {
        pthread_cond_wait(&g_pool_drain_cv, &g_pool_lock);
    }
    pthread_mutex_unlock(&g_pool_lock);
}

/* Spin up n worker threads (n <= 1 keeps the inline single-threaded path). */
static void pool_start(int n)
{
    if (n <= 1) { g_pool_n = 0; return; }
    g_pool = calloc((size_t)n, sizeof(*g_pool));
    if (!g_pool) { g_pool_n = 0; return; }
    g_pool_limit = n * 4;               /* outstanding whole-file cap */
    int started = 0;
    for (int i = 0; i < n; i++) {
        if (pthread_create(&g_pool[i], NULL, pool_worker, NULL) == 0) started++;
        else break;
    }
    g_pool_n = started;
    if (started == 0) { free(g_pool); g_pool = NULL; }
}

static void pool_shutdown(void)
{
    if (g_pool_n <= 0) return;
    pool_drain();
    pthread_mutex_lock(&g_pool_lock);
    g_pool_stop = 1;
    pthread_cond_broadcast(&g_pool_work_cv);
    pthread_mutex_unlock(&g_pool_lock);
    for (int i = 0; i < g_pool_n; i++) {
        pthread_join(g_pool[i], NULL);
    }
    free(g_pool);
    g_pool = NULL;
    g_pool_n = 0;
}

/* -------------------- main loop -------------------- */

int server_main(const char *root, int read_only)
{
    if (!root || root[0] != '/') {
        fprintf(stderr, "ecopy --server: root must be an absolute path\n");
        return 1;
    }
    /* Remember our identity so apply_meta can skip no-op chown RPCs, and clear
     * the umask so O_CREAT lands the exact mode (no follow-up fchmod needed). */
    g_euid = geteuid();
    g_egid = getegid();
    umask(0);
    {
        struct stat rst;
        g_root_present = (lstat(root, &rst) == 0 && S_ISDIR(rst.st_mode)) ? 1 : 0;
    }
    if (read_only) {
        if (!g_root_present) {
            fprintf(stderr,
                    "ecopy --server-readonly: target root must already exist: %s\n",
                    root);
            return 1;
        }
    } else if (mkdir_p(root) != 0) {
        fprintf(stderr, "ecopy --server: cannot create root %s: %s\n",
                root, strerror(errno));
        return 1;
    }
    if (normalize_abs(root, g_root_norm, sizeof(g_root_norm)) != 0) {
        fprintf(stderr, "ecopy --server: bad root path\n");
        return 1;
    }
    if (dir_cache_init() != 0) {
        fprintf(stderr, "ecopy --server: cannot initialize directory cache\n");
        return 1;
    }

    for (;;) {
        uint8_t type;
        uint64_t id;
        uint32_t plen;
        if (frame_read_header(STDIN_FILENO, &type, &id, &plen) != 0) {
            break; /* EOF: client closed the channel */
        }
        if (plen > ECOPY_MAX_FRAME) {
            fprintf(stderr, "ecopy --server: oversized frame\n");
            break;
        }
        uint8_t *payload = NULL;
        if (plen) {
            payload = malloc(plen);
            if (!payload || io_read_all(STDIN_FILENO, payload, plen) != 0) {
                free(payload);
                break;
            }
        }

        /*
         * PUTFILE/MKDIR/SETMETA go to the apply pool. The client brackets
         * directory SETMETA depth groups with barriers to preserve ordering.
         * Everything else is an ordering/consistency point: drain the pool so
         * all prior fire-and-forget work is on disk, then handle inline.
         */
        if ((type == MSG_PUTFILE || type == MSG_MKDIR || type == MSG_SETMETA ||
             type == MSG_VERIFY_PATH) &&
            g_pool_n > 0) {
            pool_submit(type, payload, plen);   /* takes ownership of payload */
            continue;
        }
        if (g_pool_n > 0) {
            pool_drain();
        }

        int rc = 0;
        switch (type) {
        case MSG_HELLO:     rc = handle_hello(id, payload, plen, root); break;
        case MSG_OPEN:      handle_open(payload, plen); break;
        case MSG_WRITE:     handle_write(payload, plen); break;
        case MSG_FTRUNCATE: handle_ftruncate(payload, plen); break;
        case MSG_COMMIT:    rc = handle_commit(id, payload, plen); break;
        case MSG_ABORT:     handle_abort(payload, plen); break;
        case MSG_PUTFILE:   handle_putfile(payload, plen); break;
        case MSG_MKDIR:     handle_mkdir(payload, plen); break;
        case MSG_STAT:      rc = handle_stat(id, payload, plen); break;
        case MSG_STAT_BULK: rc = handle_stat_bulk(id, payload, plen); break;
        case MSG_SETMETA:   handle_setmeta(payload, plen); break;
        case MSG_VERIFY_PATH: handle_verify_path(payload, plen); break;
        case MSG_BARRIER:   rc = handle_barrier(id, payload, plen); break;
        case MSG_UNLINK:    handle_unlink(payload, plen); break;
        case MSG_BYE:
            free(payload);
            pool_shutdown();
            dir_cache_destroy();
            return 0;
        default:            break; /* ignore unknown */
        }
        free(payload);
        if (rc != 0) {
            /* Failed to write a reply: the channel is gone. */
            break;
        }
    }

    pool_shutdown();
    dir_cache_destroy();

    /* Clean up any dangling temp files from an interrupted transfer. */
    while (g_files) {
        open_file_t *f = g_files;
        g_files = f->next;
        if (f->fd >= 0) close(f->fd);
        if (!f->inplace && f->tmp_path[0]) (void)unlink(f->tmp_path);
        free(f);
    }
    return 0;
}
