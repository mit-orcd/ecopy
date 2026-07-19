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
#include <sys/resource.h>
#include <sys/utsname.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdatomic.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/*
 * O_PATH yields a lightweight directory handle usable only as the dirfd of *at
 * calls (openat/renameat/unlinkat/...), which is all the dir-fd cache needs. If
 * the platform lacks it, fall back to a normal directory open (0 => O_RDONLY),
 * which also works as a dirfd, just a touch heavier.
 */
#ifndef O_PATH
#define O_PATH 0
#endif

/*
 * Block alignment for O_DIRECT on the destination. 4096 is a multiple of every
 * common logical block size (512/4096), so buffers/offsets/lengths aligned to it
 * satisfy O_DIRECT everywhere. The client already sends streamed dense writes at
 * 4096-aligned offsets and lengths (chunk size is a MiB multiple) except the
 * final tail, which we handle by dropping to buffered for that one write.
 */
#define ECOPY_DIRECT_ALIGN 4096u

/* -------------------- open-file table -------------------- */

typedef struct open_file {
    uint64_t id;
    int fd;
    int inplace;
    int direct;       /* fd currently open with O_DIRECT (streamed dense files) */
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
static gid_t g_groups[64];            /* supplementary groups (for settable-gid test) */
static int g_ngroups;
static int g_preserve_times = 1;     /* client HELLO option: apply atime/mtime? */
static int g_server_direct_io = 1;   /* open streamed dense files with O_DIRECT */

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
static _Atomic int g_chown_permission_warned;
static uint64_t g_verify_metadata_mismatches;
static uint64_t g_verify_data_mismatches;
static uint64_t g_verify_zero_mismatches;
static uint64_t g_verify_io_failures;
static uint64_t g_verify_malformed_batches;
static uint64_t g_verify_ownership_unpreserved;

/*
 * Remote drain telemetry: how many payload bytes we wrote and how long we spent
 * in the write + fsync syscalls that consume the stream. Reported at the barrier
 * so the client can show whether the remote peer's storage was the bottleneck.
 * Updated from the reader thread (streamed writes/commits) and the apply pool
 * (PUTFILE), hence atomic.
 */
static _Atomic uint64_t g_drain_bytes;
static _Atomic uint64_t g_drain_ns;

static uint64_t server_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

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
 * A uid/gid difference this unprivileged server could not have preserved. It is
 * counted separately and, unlike log_verify_error, does NOT bump g_err_count,
 * so it never turns the barrier (and thus the run) into a failure.
 */
static void note_verify_ownership_unpreserved(void)
{
    pthread_mutex_lock(&g_err_lock);
    g_verify_ownership_unpreserved++;
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

/* -------------------- directory fd cache (autofs path-walk amortizer) --------
 *
 * On automounted (autofs) destinations every open()/utimensat()/rename() by an
 * absolute path re-walks the whole path from '/', and autofs re-checks each
 * component for a mountpoint -- profiles show ~60% of the busy server process's
 * CPU there. This LRU keeps a bounded set of open O_PATH directory handles and
 * lets the file handlers use openat()/renameat()/unlinkat()/utimensat() with a
 * single trailing component, collapsing the per-file path walk to O(1) after
 * the directory is first opened. A miss opens the directory relative to its
 * (cached) parent, so warming a subtree costs one component per level total.
 *
 * fds are a scarce resource, so the cache is bounded (default derived from
 * RLIMIT_NOFILE, override DIRECT_COPY_SSH_SERVER_DIRFD_CACHE) and entries in use
 * are refcount-pinned so eviction never closes a handle a worker is mid-syscall
 * on.
 */
typedef struct dfd_entry {
    struct dfd_entry *hnext;                 /* hash chain */
    struct dfd_entry *lru_prev, *lru_next;   /* MRU at head, LRU at tail */
    int fd;
    int refcount;                            /* pinned while > 0 */
    uint32_t hash;
    char path[];
} dfd_entry_t;

#define DFD_BUCKETS (1u << 16)
static dfd_entry_t *g_dfd_buckets[DFD_BUCKETS];
static dfd_entry_t *g_dfd_head;              /* MRU */
static dfd_entry_t *g_dfd_tail;              /* LRU */
static int g_dfd_count;
static int g_dfd_cap;
static pthread_mutex_t g_dfd_lock = PTHREAD_MUTEX_INITIALIZER;

static void dir_fd_cache_init(void)
{
    long cur = -1;
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY) {
        cur = (long)rl.rlim_cur;
    }
    /* Leave generous headroom for streamed-file fds, pool, and the SSH pipe. */
    int cap;
    if (cur < 0) {
        cap = 8192;                          /* unlimited: pick a sane ceiling */
    } else if (cur > 1024) {
        cap = (int)(cur - 512);
    } else {
        cap = (int)(cur / 2);
    }
    const char *env = getenv("DIRECT_COPY_SSH_SERVER_DIRFD_CACHE");
    if (env && *env) {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end && !*end && v >= 0) cap = (int)v;
    }
    if (cap < 16) cap = 16;
    if (cap > 262144) cap = 262144;
    g_dfd_cap = cap;
}

/* Unlink from the LRU list (caller holds g_dfd_lock). */
static void dfd_lru_unlink(dfd_entry_t *e)
{
    if (e->lru_prev) e->lru_prev->lru_next = e->lru_next;
    else g_dfd_head = e->lru_next;
    if (e->lru_next) e->lru_next->lru_prev = e->lru_prev;
    else g_dfd_tail = e->lru_prev;
    e->lru_prev = e->lru_next = NULL;
}

/* Insert at MRU head (caller holds g_dfd_lock). */
static void dfd_lru_push_front(dfd_entry_t *e)
{
    e->lru_prev = NULL;
    e->lru_next = g_dfd_head;
    if (g_dfd_head) g_dfd_head->lru_prev = e;
    g_dfd_head = e;
    if (!g_dfd_tail) g_dfd_tail = e;
}

/* Find + pin + promote to MRU (caller holds g_dfd_lock). */
static dfd_entry_t *dfd_lookup_pin_locked(const char *path, uint32_t hash)
{
    dfd_entry_t *e = g_dfd_buckets[hash & (DFD_BUCKETS - 1u)];
    for (; e; e = e->hnext) {
        if (e->hash == hash && strcmp(e->path, path) == 0) {
            e->refcount++;
            dfd_lru_unlink(e);
            dfd_lru_push_front(e);
            return e;
        }
    }
    return NULL;
}

/* Close unpinned tail entries until we are back under the cap (caller holds
 * g_dfd_lock). Entries in use (refcount > 0) are skipped; if the whole cache is
 * momentarily pinned we simply run slightly over cap. */
static void dfd_evict_locked(void)
{
    dfd_entry_t *e = g_dfd_tail;
    while (g_dfd_count > g_dfd_cap && e) {
        dfd_entry_t *prev = e->lru_prev;
        if (e->refcount == 0) {
            /* unlink from hash */
            dfd_entry_t **pp = &g_dfd_buckets[e->hash & (DFD_BUCKETS - 1u)];
            while (*pp && *pp != e) pp = &(*pp)->hnext;
            if (*pp) *pp = e->hnext;
            dfd_lru_unlink(e);
            close(e->fd);
            free(e);
            g_dfd_count--;
        }
        e = prev;
    }
}

static void dir_fd_release(dfd_entry_t *e)
{
    if (!e) return;
    pthread_mutex_lock(&g_dfd_lock);
    if (e->refcount > 0) e->refcount--;
    pthread_mutex_unlock(&g_dfd_lock);
}

/*
 * Acquire a pinned O_PATH handle for directory `dir` (which must already exist,
 * e.g. via ensure_dir_cached_mode). Returns the fd and stores the entry in
 * *out; release with dir_fd_release(*out). Returns -1 (errno set) on failure,
 * with *out == NULL. A miss opens `dir` relative to its cached parent so
 * warming a deep path costs one component per level rather than a full walk.
 */
static int dir_fd_acquire(const char *dir, dfd_entry_t **out)
{
    *out = NULL;
    if (g_dfd_cap <= 0) { errno = EINVAL; return -1; }

    uint32_t hash = dir_cache_hash(dir);
    pthread_mutex_lock(&g_dfd_lock);
    dfd_entry_t *e = dfd_lookup_pin_locked(dir, hash);
    pthread_mutex_unlock(&g_dfd_lock);
    if (e) { *out = e; return e->fd; }

    /* Miss: open, preferring a parent-relative single-component open. */
    int fd = -1;
    dfd_entry_t *pe = NULL;
    char parent[PATH_MAX], base[PATH_MAX];
    split_dir_base(dir, parent, sizeof(parent), base, sizeof(base));
    int use_parent = strcmp(dir, g_root_norm) != 0 &&
                     strcmp(parent, dir) != 0 &&
                     base[0] && strcmp(base, "/") != 0 &&
                     path_under_root(parent);
    if (use_parent) {
        int pfd = dir_fd_acquire(parent, &pe);
        if (pfd >= 0) {
            fd = openat(pfd, base,
                        O_PATH | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        }
    }
    if (fd < 0) {
        fd = open(dir, O_PATH | O_DIRECTORY | O_CLOEXEC);
    }
    if (pe) dir_fd_release(pe);
    if (fd < 0) return -1;

    size_t len = strlen(dir);
    pthread_mutex_lock(&g_dfd_lock);
    e = dfd_lookup_pin_locked(dir, hash);   /* someone may have raced us */
    if (e) {
        pthread_mutex_unlock(&g_dfd_lock);
        close(fd);
        *out = e;
        return e->fd;
    }
    e = malloc(sizeof(*e) + len + 1);
    if (!e) {
        pthread_mutex_unlock(&g_dfd_lock);
        close(fd);
        errno = ENOMEM;
        return -1;
    }
    e->fd = fd;
    e->refcount = 1;
    e->hash = hash;
    memcpy(e->path, dir, len + 1);
    e->hnext = g_dfd_buckets[hash & (DFD_BUCKETS - 1u)];
    g_dfd_buckets[hash & (DFD_BUCKETS - 1u)] = e;
    dfd_lru_push_front(e);
    g_dfd_count++;
    dfd_evict_locked();
    pthread_mutex_unlock(&g_dfd_lock);
    *out = e;
    return fd;
}

static void dir_fd_cache_destroy(void)
{
    pthread_mutex_lock(&g_dfd_lock);
    dfd_entry_t *e = g_dfd_head;
    while (e) {
        dfd_entry_t *next = e->lru_next;
        close(e->fd);
        free(e);
        e = next;
    }
    g_dfd_head = g_dfd_tail = NULL;
    g_dfd_count = 0;
    for (size_t i = 0; i < DFD_BUCKETS; i++) g_dfd_buckets[i] = NULL;
    pthread_mutex_unlock(&g_dfd_lock);
}

/*
 * Apply uid/gid/mode/times to an open fd, minimizing SETATTR RPCs (each of
 * fchown/fchmod/futimens is a separate round trip on NFS):
 *   - skip fchown when the target owner already matches ours (the file was
 *     created by us, so it already has these ids -- a no-op RPC otherwise);
 *   - skip fchmod when the caller created the file with the final mode already
 *     (mode_is_set); directories and re-used temps still pass 0.
 *   - skip futimens entirely when the client asked not to preserve times.
 *
 * fchown is best effort: an unprivileged peer cannot change ownership on most
 * filesystems (EPERM/EACCES), exactly as the local copy path tolerates in
 * fs_util.c. Those are warned once and not treated as hard failures; every
 * other syscall error is reported so silent metadata loss cannot happen.
 */
static int chown_permission_errno(int err)
{
    return err == EPERM || err == EACCES;
}

/*
 * True when a chown to the requested uid is guaranteed to fail: an unprivileged
 * peer (euid != 0) can never change a file's owner uid, so re-chowning an object
 * we just created (owned by g_euid) to a foreign uid always returns EPERM. On
 * NFS each such SETATTR is a synchronous RPC, and profiling shows it dominates
 * large metadata-heavy trees, so we skip the doomed call up front. A differing
 * gid alone (uid == ours) may still succeed if we belong to the group, so only
 * the unchangeable-uid case is short-circuited. Mirrors fs_util.c's local path.
 */
static int chown_uid_doomed(uint32_t uid)
{
    return g_euid != 0 && (uid_t)uid != g_euid;
}

/*
 * True when we can set an object's group to gid: root sets any group, and an
 * unprivileged peer can set a group it belongs to (its egid or a supplementary
 * group). This lets a settable gid still be applied when the owner uid is not
 * changeable, instead of being dropped along with the doomed uid SETATTR (e.g.
 * a forced --gid the peer belongs to). Non-member groups return false so we do
 * not issue a guaranteed-EPERM SETATTR, preserving the metadata-cost saving.
 */
static int chown_gid_settable(uint32_t gid)
{
    if (g_euid == 0) return 1;
    if ((gid_t)gid == g_egid) return 1;
    for (int i = 0; i < g_ngroups; i++) {
        if (g_groups[i] == (gid_t)gid) return 1;
    }
    return 0;
}

static int apply_meta(int fd, uint32_t uid, uint32_t gid, uint32_t mode,
                      int64_t at_s, int64_t at_ns, int64_t mt_s, int64_t mt_ns,
                      int mode_is_set, const char **failed_op)
{
    int first_err = 0;
    const char *first_op = NULL;
    int force_eperm = getenv("ECOPY_TEST_FORCE_CHOWN_EPERM") != NULL;
    if (force_eperm || (uid_t)uid != g_euid || (gid_t)gid != g_egid) {
        /*
         * Apply owner and group independently: a uid we cannot change must not
         * drag down a group change we are allowed to make (e.g. a forced --gid
         * the peer belongs to). uid==-1 / gid==-1 leave that component untouched,
         * so we still issue at most one SETATTR and skip it entirely when neither
         * part is both needed and permitted.
         */
        int chown_err = 0;
        int unpreserved = 0;
        if (force_eperm) {
            unpreserved = 1;   /* test hook: emulate an unprivileged target */
        } else {
            uid_t want_uid = chown_uid_doomed(uid) ? (uid_t)-1 : (uid_t)uid;
            gid_t want_gid = chown_gid_settable(gid) ? (gid_t)gid : (gid_t)-1;
            if (want_uid == g_euid) want_uid = (uid_t)-1;   /* no-op owner */
            if (want_gid == g_egid) want_gid = (gid_t)-1;   /* no-op group */
            if (want_uid != (uid_t)-1 || want_gid != (gid_t)-1) {
                if (fchown(fd, want_uid, want_gid) != 0) chown_err = errno;
            }
            /* Ownership we could not apply: an unchangeable owner uid, or a group
             * we do not belong to. Reported once as a warning, not a failure. */
            unpreserved = (chown_uid_doomed(uid) && (uid_t)uid != g_euid) ||
                          (!chown_gid_settable(gid) && (gid_t)gid != g_egid);
        }
        if (chown_err != 0 && !chown_permission_errno(chown_err)) {
            first_err = chown_err;
            first_op = "fchown";
        } else if (unpreserved || chown_err != 0) {
            if (atomic_exchange(&g_chown_permission_warned, 1) == 0) {
                fprintf(stderr,
                        "ecopy: remote chown not fully permitted; continuing "
                        "without preserving some uid/gid ownership\n");
            }
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
    /*
     * Check genuine fields (type/mode/times) before ownership so a pure uid/gid
     * difference is only reported when everything else matches; that lets the
     * caller safely downgrade an unpreservable-ownership mismatch to a warning.
     */
    if ((!is_dir && !S_ISREG(st->st_mode)) ||
        (is_dir && !S_ISDIR(st->st_mode))) return "type";
    if ((st->st_mode & 07777) != (mode_t)(mode & 07777)) return "mode";
    if (check_times &&
        ((int64_t)st->st_atim.tv_sec != at_s ||
         (int64_t)st->st_atim.tv_nsec != at_ns)) return "atime";
    if (check_times &&
        ((int64_t)st->st_mtim.tv_sec != mt_s ||
         (int64_t)st->st_mtim.tv_nsec != mt_ns)) return "mtime";
    if (st->st_uid != (uid_t)uid) return "uid";
    if (st->st_gid != (gid_t)gid) return "gid";
    if (getenv("ECOPY_TEST_FORCE_OWNERSHIP_MISMATCH")) return "uid";
    return NULL;
}

/*
 * True when the only metadata difference is ownership (uid/gid) that this
 * server, running unprivileged, could never have preserved. Such differences
 * are reported as a warning category, not a failure.
 */
static int metadata_ownership_unpreservable(const char *field)
{
    if (!field) return 0;
    int unprivileged = (g_euid != 0) ||
                       (getenv("ECOPY_TEST_FORCE_OWNERSHIP_MISMATCH") != NULL);
    return unprivileged &&
           (strcmp(field, "uid") == 0 || strcmp(field, "gid") == 0);
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
    /* Default: open streamed dense files with O_DIRECT. The client sets the
     * SERVER_BUFFERED bit to turn it off; a missing bit keeps direct on. */
    if (!d.error && (opts & ECOPY_OPT_SERVER_BUFFERED)) g_server_direct_io = 0;

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

/* True if an O_DIRECT open should be retried as a plain buffered open. */
static int direct_open_fallback_errno(int e)
{
    return e == EINVAL || e == EOPNOTSUPP || e == ENOTSUP || e == ENOSYS ||
           e == EPERM;
}

/*
 * Aligned bounce buffer for O_DIRECT writes. handle_write runs only on the
 * single reader thread (WRITE is never pooled), so this needs no locking. Grown
 * on demand and freed in server_main cleanup.
 */
static uint8_t *g_wbuf;
static size_t g_wbuf_cap;

static uint8_t *wbuf_ensure(size_t need)
{
    if (g_wbuf_cap >= need) return g_wbuf;
    size_t want = ECOPY_DIRECT_ALIGN;
    while (want < need) want <<= 1;
    void *p = NULL;
    if (posix_memalign(&p, ECOPY_DIRECT_ALIGN, want) != 0) return NULL;
    free(g_wbuf);
    g_wbuf = (uint8_t *)p;
    g_wbuf_cap = want;
    return g_wbuf;
}

/* Turn off O_DIRECT on an open file's fd so subsequent writes go buffered. */
static void drop_direct(open_file_t *f)
{
    int fl = fcntl(f->fd, F_GETFL);
    if (fl >= 0) (void)fcntl(f->fd, F_SETFL, fl & ~O_DIRECT);
    f->direct = 0;
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

    /*
     * Streamed dense files get O_DIRECT (bypass the page cache) when the client
     * left it enabled. Sparse files stay buffered: their write offsets/lengths
     * are hole-derived and not block-aligned. If the filesystem rejects
     * O_DIRECT at open time, retry buffered so the transfer still succeeds.
     */
    int want_direct = g_server_direct_io && !(flags & ECOPY_OPEN_SPARSE);
    int base_flags = f->inplace
                         ? (O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC)
                         : (O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC);
    mode_t create_mode = copy_data_mode((mode_t)mode);

    if (f->inplace) {
        snprintf(f->tmp_path, sizeof(f->tmp_path), "%s", finalp);
    } else {
        int need = snprintf(f->tmp_path, sizeof(f->tmp_path), "%s/.ecopy.tmp.%u.%llu",
                            dir, (unsigned)getpid(), (unsigned long long)fid);
        if (need < 0 || need >= (int)sizeof(f->tmp_path)) {
            f->failed = 1; f->err = -ENAMETOOLONG;
            f->next = g_files; g_files = f;
            return;
        }
    }

    int fd = -1;
    if (want_direct) {
        fd = open(f->tmp_path, base_flags | O_DIRECT, create_mode);
        if (fd >= 0) {
            f->direct = 1;
        } else if (!direct_open_fallback_errno(errno)) {
            f->failed = 1; f->err = -errno;
            f->next = g_files; g_files = f;
            return;
        }
    }
    if (fd < 0) {
        fd = open(f->tmp_path, base_flags, create_mode);
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
                /*
                 * Best-effort preallocation fallback; if even ftruncate fails
                 * the subsequent writes still extend the file, so ignore it.
                 */
                if (ftruncate(fd, (off_t)expected) != 0) {
                    /* intentionally ignored */
                }
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

    uint64_t t0 = server_ns();

    /*
     * Direct path: full aligned blocks are written from an aligned bounce
     * buffer with O_DIRECT. The single unaligned tail (always the last write of
     * a dense file) and any surprise EINVAL drop the fd to buffered. A
     * misaligned offset (shouldn't happen for dense) also falls back.
     */
    if (f->direct) {
        int aligned = ((offset % ECOPY_DIRECT_ALIGN) == 0) &&
                      ((len % ECOPY_DIRECT_ALIGN) == 0);
        if (!aligned) {
            drop_direct(f);
        } else {
            uint8_t *ab = wbuf_ensure(len);
            if (!ab) {
                drop_direct(f);         /* no aligned buffer: fall back */
            } else {
                memcpy(ab, data, len);
                size_t done = 0;
                int fell_back = 0;
                while (done < len) {
                    ssize_t w = pwrite(f->fd, ab + done, len - done,
                                       offset + (off_t)done);
                    if (w < 0) {
                        if (errno == EINTR) continue;
                        if (errno == EINVAL && done == 0) {
                            drop_direct(f); /* alignment surprise: go buffered */
                            fell_back = 1;
                            break;
                        }
                        f->failed = 1; f->err = -errno;
                        break;
                    }
                    if (w == 0) { f->failed = 1; f->err = -EIO; break; }
                    done += (size_t)w;
                }
                if (!fell_back) {
                    atomic_fetch_add(&g_drain_bytes, (uint64_t)done);
                    atomic_fetch_add(&g_drain_ns, server_ns() - t0);
                    return;
                }
            }
        }
    }

    /* Buffered path (also the fallback target from above): write directly from
     * the frame payload; no alignment constraints. */
    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(f->fd, data + done, len - done, offset + (off_t)done);
        if (w < 0) {
            if (errno == EINTR) continue;
            f->failed = 1; f->err = -errno;
            break;
        }
        if (w == 0) { f->failed = 1; f->err = -EIO; break; }
        done += (size_t)w;
    }
    atomic_fetch_add(&g_drain_bytes, (uint64_t)done);
    atomic_fetch_add(&g_drain_ns, server_ns() - t0);
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
        uint64_t t0 = server_ns();
        if (fsync(f->fd) != 0) status = -errno;
        atomic_fetch_add(&g_drain_ns, server_ns() - t0);
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

    /*
     * Open the target through its parent's cached directory handle so the
     * per-object metadata pass does not re-walk the full (autofs) path. Falls
     * back to an absolute open for the confinement root or a cache miss.
     */
    char sdir[PATH_MAX], sbase[PATH_MAX];
    split_dir_base(path, sdir, sizeof(sdir), sbase, sizeof(sbase));
    int parent_ok = strcmp(path, g_root_norm) != 0 &&
                    strcmp(sdir, path) != 0 &&
                    sbase[0] && strcmp(sbase, "/") != 0 &&
                    path_under_root(sdir);
    int fd = -1;
    dfd_entry_t *de = NULL;
    if (parent_ok) {
        int dfd = dir_fd_acquire(sdir, &de);
        if (dfd >= 0) {
            fd = openat(dfd, sbase, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
            if (fd < 0) {
                fd = openat(dfd, sbase,
                            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            }
        }
    }
    if (fd < 0) {
        fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0) {
            /* Directories may need O_DIRECTORY on some systems; retry. */
            fd = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        }
    }
    if (de) dir_fd_release(de);
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

    /*
     * Open/create/rename relative to a cached directory handle so autofs does
     * not re-walk the whole path per file. `tmp_name` is the basename created in
     * `dir`; the final rename is dir-relative too.
     */
    dfd_entry_t *de = NULL;
    int dfd = dir_fd_acquire(dir, &de);
    if (dfd < 0) { log_op_error(dir, errno); return; }

    char tmp_name[PATH_MAX];
    int fd;
    if (inplace) {
        snprintf(tmp_name, sizeof(tmp_name), "%s", base);
        fd = openat(dfd, base, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, create_mode);
    } else {
        static _Atomic uint64_t put_seq;
        uint64_t seq = atomic_fetch_add(&put_seq, 1) + 1;
        int need = snprintf(tmp_name, sizeof(tmp_name), ".ecopy.tmp.%u.p%llu",
                            (unsigned)getpid(), (unsigned long long)seq);
        if (need < 0 || need >= (int)sizeof(tmp_name)) { dir_fd_release(de); log_op_error(finalp, ENAMETOOLONG); return; }
        fd = openat(dfd, tmp_name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, create_mode);
    }
    if (fd < 0) { dir_fd_release(de); log_op_error(finalp, errno); return; }

    int err = 0;
    const char *failed_op = NULL;
    size_t done = 0;
    uint64_t t0 = server_ns();
    while (done < data_len) {
        ssize_t w = write(fd, data + done, data_len - done);
        if (w < 0) { if (errno == EINTR) continue; err = errno; break; }
        if (w == 0) { err = EIO; break; }
        done += (size_t)w;
    }
    atomic_fetch_add(&g_drain_bytes, (uint64_t)done);
    atomic_fetch_add(&g_drain_ns, server_ns() - t0);
    if (err == 0) {
        int status = apply_meta(fd, uid, gid, mode,
                                at_s, at_ns, mt_s, mt_ns, mode_is_set,
                                &failed_op);
        if (status != 0) err = -status;
    }
    if (close(fd) != 0 && err == 0) err = errno;
    if (err == 0 && !inplace) {
        if (renameat(dfd, tmp_name, dfd, base) != 0) err = errno;
    }
    if (err != 0 && !inplace) (void)unlinkat(dfd, tmp_name, 0);
    dir_fd_release(de);
    if (err != 0) {
        char diagnostic[PATH_MAX];
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
        if (field && metadata_ownership_unpreservable(field)) {
            note_verify_ownership_unpreserved();
        } else if (field) {
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
        if (meta_field && metadata_ownership_unpreservable(meta_field)) {
            note_verify_ownership_unpreserved();
        } else if (meta_field) {
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

    uint8_t buf[PATH_MAX + 96];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, (uint32_t)(g_err_count ? g_err_first : 0));
    penc_u32(&e, (uint32_t)(g_err_count > 0xffffffffULL ? 0xffffffffU : g_err_count));
    penc_str(&e, g_err_first_path);
    penc_u64(&e, g_verify_metadata_mismatches);
    penc_u64(&e, g_verify_data_mismatches);
    penc_u64(&e, g_verify_zero_mismatches);
    penc_u64(&e, g_verify_io_failures);
    penc_u64(&e, g_verify_malformed_batches);
    penc_u64(&e, g_verify_ownership_unpreserved);
    penc_u64(&e, atomic_load(&g_drain_bytes));
    penc_u64(&e, atomic_load(&g_drain_ns));
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

/*
 * Recreate a symlink verbatim (never dereferenced). Ownership is best-effort:
 * an unprivileged server cannot lchown to a foreign uid, so EPERM/EACCES is
 * downgraded to a one-time warning like regular-file chown.
 */
static void handle_symlink(const uint8_t *payload, uint32_t plen)
{
    char target[PATH_MAX];
    char link_in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, target, sizeof(target)) != 0) return;
    if (pdec_str(&d, link_in, sizeof(link_in)) != 0) return;
    uint32_t uid = pdec_u32(&d);
    uint32_t gid = pdec_u32(&d);
    int64_t at_s = pdec_i64(&d), at_ns = pdec_i64(&d);
    int64_t mt_s = pdec_i64(&d), mt_ns = pdec_i64(&d);
    if (d.error) return;

    char link_path[PATH_MAX];
    if (resolve_path(link_in, link_path, sizeof(link_path)) != 0) {
        log_op_error(link_in, errno);
        return;
    }

    char dir[PATH_MAX], base[PATH_MAX];
    split_dir_base(link_path, dir, sizeof(dir), base, sizeof(base));
    if (ensure_dir_cached_mode(dir, 0755) != 0) {
        log_op_error(dir, errno);
        return;
    }

    dfd_entry_t *de = NULL;
    int dfd = dir_fd_acquire(dir, &de);
    if (dfd < 0) { log_op_error(dir, errno); return; }

    static _Atomic uint64_t sym_seq;
    uint64_t seq = atomic_fetch_add(&sym_seq, 1) + 1;
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), ".ecopy.tmp.sym.%u.%llu",
                 (unsigned)getpid(), (unsigned long long)seq) >= (int)sizeof(tmp)) {
        dir_fd_release(de);
        log_op_error(link_path, ENAMETOOLONG);
        return;
    }
    if (symlinkat(target, dfd, tmp) != 0) {
        dir_fd_release(de);
        log_op_error(link_path, errno);
        return;
    }
    if ((uid_t)uid != g_euid || (gid_t)gid != g_egid) {
        /* Apply owner and group independently (see apply_meta): a settable group
         * is honored even when the owner uid cannot be changed. */
        uid_t want_uid = chown_uid_doomed(uid) ? (uid_t)-1 : (uid_t)uid;
        gid_t want_gid = chown_gid_settable(gid) ? (gid_t)gid : (gid_t)-1;
        if (want_uid == g_euid) want_uid = (uid_t)-1;
        if (want_gid == g_egid) want_gid = (gid_t)-1;
        int cerr = 0;
        if (want_uid != (uid_t)-1 || want_gid != (gid_t)-1) {
            if (fchownat(dfd, tmp, want_uid, want_gid, AT_SYMLINK_NOFOLLOW) != 0)
                cerr = errno;
        }
        int unpreserved = (chown_uid_doomed(uid) && (uid_t)uid != g_euid) ||
                          (!chown_gid_settable(gid) && (gid_t)gid != g_egid);
        if (cerr != 0 && !chown_permission_errno(cerr)) {
            log_op_error(link_path, cerr);
        } else if (unpreserved || cerr != 0) {
            if (atomic_exchange(&g_chown_permission_warned, 1) == 0) {
                fprintf(stderr,
                        "ecopy: remote chown not fully permitted; continuing "
                        "without preserving some uid/gid ownership\n");
            }
        }
    }
    if (g_preserve_times) {
        struct timespec ts[2];
        ts[0].tv_sec = (time_t)at_s; ts[0].tv_nsec = (long)at_ns;
        ts[1].tv_sec = (time_t)mt_s; ts[1].tv_nsec = (long)mt_ns;
        (void)utimensat(dfd, tmp, ts, AT_SYMLINK_NOFOLLOW);
    }
    if (renameat(dfd, tmp, dfd, base) != 0) {
        log_op_error(link_path, errno);
        (void)unlinkat(dfd, tmp, 0);
    }
    dir_fd_release(de);
}

/*
 * Create a hard link. This message is non-pooled, so the reader has already
 * drained the apply pool before we run: the primary file it references is
 * guaranteed materialized. Overwrites an existing entry atomically.
 */
static void handle_link(const uint8_t *payload, uint32_t plen)
{
    char pri_in[PATH_MAX], link_in[PATH_MAX];
    pdec_t d; pdec_init(&d, payload, plen);
    if (pdec_str(&d, pri_in, sizeof(pri_in)) != 0) return;
    if (pdec_str(&d, link_in, sizeof(link_in)) != 0) return;
    if (d.error) return;

    char pri[PATH_MAX], link_path[PATH_MAX];
    if (resolve_path(pri_in, pri, sizeof(pri)) != 0) { log_op_error(pri_in, errno); return; }
    if (resolve_path(link_in, link_path, sizeof(link_path)) != 0) { log_op_error(link_in, errno); return; }

    char dir[PATH_MAX], base[PATH_MAX];
    split_dir_base(link_path, dir, sizeof(dir), base, sizeof(base));
    if (ensure_dir_cached_mode(dir, 0755) != 0) { log_op_error(dir, errno); return; }

    dfd_entry_t *de = NULL;
    int dfd = dir_fd_acquire(dir, &de);
    if (dfd < 0) { log_op_error(dir, errno); return; }

    /* oldpath (primary) is absolute; newpath is created in the cached dir. */
    if (linkat(AT_FDCWD, pri, dfd, base, 0) == 0) { dir_fd_release(de); return; }
    if (errno != EEXIST) { dir_fd_release(de); log_op_error(link_path, errno); return; }

    /*
     * The name already exists. If it is already a link to the same inode as the
     * primary (a re-copy of an unchanged hard link), we are done. This also
     * avoids the temp+rename path below, where renaming two links to the same
     * inode is a POSIX no-op that returns success WITHOUT removing the temp,
     * orphaning it.
     */
    struct stat pst, lst;
    if (fstatat(AT_FDCWD, pri, &pst, 0) == 0 &&
        fstatat(dfd, base, &lst, AT_SYMLINK_NOFOLLOW) == 0 &&
        pst.st_dev == lst.st_dev && pst.st_ino == lst.st_ino) {
        dir_fd_release(de);
        return;
    }

    static _Atomic uint64_t link_seq;
    uint64_t seq = atomic_fetch_add(&link_seq, 1) + 1;
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), ".ecopy.tmp.lnk.%u.%llu",
                 (unsigned)getpid(), (unsigned long long)seq) >= (int)sizeof(tmp)) {
        dir_fd_release(de);
        log_op_error(link_path, ENAMETOOLONG);
        return;
    }
    if (linkat(AT_FDCWD, pri, dfd, tmp, 0) != 0) {
        dir_fd_release(de);
        log_op_error(link_path, errno);
        return;
    }
    if (renameat(dfd, tmp, dfd, base) != 0) {
        log_op_error(link_path, errno);
        (void)unlinkat(dfd, tmp, 0);
    }
    dir_fd_release(de);
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
    {
        int n = getgroups((int)(sizeof(g_groups) / sizeof(g_groups[0])), g_groups);
        g_ngroups = (n > 0) ? n : 0;
    }
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
    dir_fd_cache_init();

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
        case MSG_SYMLINK:   handle_symlink(payload, plen); break;
        case MSG_LINK:      handle_link(payload, plen); break;
        case MSG_BYE:
            free(payload);
            pool_shutdown();
            dir_fd_cache_destroy();
            dir_cache_destroy();
            free(g_wbuf);
            g_wbuf = NULL;
            g_wbuf_cap = 0;
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
    dir_fd_cache_destroy();
    dir_cache_destroy();

    /* Clean up any dangling temp files from an interrupted transfer. */
    while (g_files) {
        open_file_t *f = g_files;
        g_files = f->next;
        if (f->fd >= 0) close(f->fd);
        if (!f->inplace && f->tmp_path[0]) (void)unlink(f->tmp_path);
        free(f);
    }
    free(g_wbuf);
    g_wbuf = NULL;
    g_wbuf_cap = 0;
    return 0;
}
