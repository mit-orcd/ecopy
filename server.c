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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <time.h>
#include <pthread.h>
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

/*
 * Deferred error log for fire-and-forget operations (PUTFILE/MKDIR/SETMETA).
 * These get no per-op reply; the client learns about failures at the next
 * BARRIER, which reports the cumulative count and the first failing path.
 */
static uint64_t g_err_count;
static int32_t  g_err_first;         /* negative errno of the first failure */
static char     g_err_first_path[PATH_MAX];
static pthread_mutex_t g_err_lock = PTHREAD_MUTEX_INITIALIZER;  /* pool workers log here */

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

/* Cache the last directory we ensured exists, so a run of PUTFILEs into the
 * same directory does not re-walk mkdir -p every time. Thread-local so each
 * apply-pool worker keeps its own cache without locking. */
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

/*
 * Ensure a (confined) directory exists, creating missing components. Caches the
 * last directory so a stream of PUTFILEs into the same directory does not
 * re-walk mkdir -p on every file. dir must already be resolved under the root.
 */
static int ensure_dir_cached(const char *dir)
{
    if (strcmp(dir, g_last_dir) == 0) {
        return 0;
    }
    if (mkdir_p(dir) != 0) {
        return -1;
    }
    snprintf(g_last_dir, sizeof(g_last_dir), "%s", dir);
    return 0;
}

/*
 * Apply uid/gid/mode/times to an open fd, minimizing SETATTR RPCs (each of
 * fchown/fchmod/futimens is a separate round trip on NFS):
 *   - skip fchown when the target owner already matches ours (the file was
 *     created by us, so it already has these ids -- a no-op RPC otherwise);
 *   - skip fchmod when the caller created the file with the final mode already
 *     (mode_is_set); directories and re-used temps still pass 0.
 * futimens is always issued when we want to preserve times.
 */
static void apply_meta(int fd, uint32_t uid, uint32_t gid, uint32_t mode,
                       int64_t at_s, int64_t at_ns, int64_t mt_s, int64_t mt_ns,
                       int mode_is_set)
{
    struct timespec ts[2];
    if ((uid_t)uid != g_euid || (gid_t)gid != g_egid) {
        (void)fchown(fd, (uid_t)uid, (gid_t)gid);   /* best effort, like local path */
    }
    if (!mode_is_set) {
        (void)fchmod(fd, (mode_t)(mode & 07777));
    }
    ts[0].tv_sec = (time_t)at_s; ts[0].tv_nsec = (long)at_ns;
    ts[1].tv_sec = (time_t)mt_s; ts[1].tv_nsec = (long)mt_ns;
    (void)futimens(fd, ts);
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

    int fd;
    if (f->inplace) {
        snprintf(f->tmp_path, sizeof(f->tmp_path), "%s", finalp);
        fd = open(finalp, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC,
                  copy_data_mode((mode_t)mode));
    } else {
        char dir[PATH_MAX], base[PATH_MAX];
        split_dir_base(finalp, dir, sizeof(dir), base, sizeof(base));
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
        if (status == 0) apply_meta(f->fd, uid, gid, mode, at_s, at_ns, mt_s, mt_ns, 0);
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

    struct stat st;
    if (lstat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) log_op_error(path, EEXIST);
        return;
    }
    /*
     * MKDIRs are applied in parallel and out of order by the pool, so a nested
     * directory can arrive before its parent. Create the whole chain (parents
     * default, owner-writable) rather than a single component; the final mode
     * is applied later at finalize via SETMETA.
     */
    (void)mode;
    if (mkdir_p(path) != 0 && errno != EEXIST) {
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

/* Fire-and-forget in v2: apply metadata, record failures in the log. */
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
    (void)pdec_u8(&d); /* is_dir; open works for both */
    if (d.error) return;

    char path[PATH_MAX];
    if (resolve_path(in, path, sizeof(path)) != 0) { log_op_error(in, errno); return; }

    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        /* Directories may need O_DIRECTORY on some systems; retry. */
        fd = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    }
    if (fd < 0) { log_op_error(path, errno); return; }
    apply_meta(fd, uid, gid, mode, at_s, at_ns, mt_s, mt_ns, 0);
    close(fd);
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
    char final_in[PATH_MAX];
    if (pdec_str(&d, final_in, sizeof(final_in)) != 0 || d.error) return;

    const uint8_t *data = payload + d.off;
    size_t data_len = plen - d.off;

    char finalp[PATH_MAX];
    if (resolve_path(final_in, finalp, sizeof(finalp)) != 0) { log_op_error(final_in, errno); return; }

    char dir[PATH_MAX], base[PATH_MAX];
    split_dir_base(finalp, dir, sizeof(dir), base, sizeof(base));
    if (ensure_dir_cached(dir) != 0) { log_op_error(dir, errno); return; }

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
    size_t done = 0;
    while (done < data_len) {
        ssize_t w = write(fd, data + done, data_len - done);
        if (w < 0) { if (errno == EINTR) continue; err = errno; break; }
        if (w == 0) { err = EIO; break; }
        done += (size_t)w;
    }
    if (err == 0) {
        apply_meta(fd, uid, gid, mode, at_s, at_ns, mt_s, mt_ns, mode_is_set);
    }
    if (close(fd) != 0 && err == 0) err = errno;
    if (err == 0 && !inplace) {
        if (rename(tmp_path, finalp) != 0) err = errno;
    }
    if (err != 0) {
        if (!inplace) (void)unlink(tmp_path);
        log_op_error(finalp, err);
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

    uint8_t buf[PATH_MAX + 32];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, (uint32_t)(g_err_count ? g_err_first : 0));
    penc_u32(&e, (uint32_t)(g_err_count > 0xffffffffULL ? 0xffffffffU : g_err_count));
    penc_str(&e, g_err_first_path);
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
 * A pool of worker threads that apply the fire-and-forget operations (PUTFILE
 * and MKDIR) in parallel. Over NFS every one of these blocks on synchronous
 * RPC latency, so a single-threaded server sat ~90% idle in D-state; issuing
 * many in flight at once hides that latency.
 *
 * The reader thread (server_main) only ever *submits* PUTFILE/MKDIR jobs. Any
 * other frame (SETMETA, STAT, OPEN/WRITE/COMMIT, BARRIER, ...) first drains the
 * pool, then runs inline, so ordering that matters is preserved:
 *   - a directory's final SETMETA lands after its files were written;
 *   - a streamed OPEN sees its parent directory created;
 *   - a BARRIER's error aggregate and flush cover all prior work.
 * PUTFILE self-creates its parent dirs and MKDIR is idempotent, so those two
 * need no ordering relative to each other.
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

/* Hand a PUTFILE/MKDIR frame to the pool; takes ownership of payload. Blocks
 * (back-pressuring the reader, hence the SSH channel) when too many are in
 * flight, bounding memory to roughly g_pool_limit whole small files. */
static void pool_submit(uint8_t type, uint8_t *payload, uint32_t plen)
{
    pool_job_t *j = malloc(sizeof(*j));
    if (!j) {                           /* out of memory: just run it inline */
        if (type == MSG_PUTFILE) handle_putfile(payload, plen);
        else if (type == MSG_MKDIR) handle_mkdir(payload, plen);
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

int server_main(const char *root)
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
    if (mkdir_p(root) != 0) {
        fprintf(stderr, "ecopy --server: cannot create root %s: %s\n", root, strerror(errno));
        return 1;
    }
    if (normalize_abs(root, g_root_norm, sizeof(g_root_norm)) != 0) {
        fprintf(stderr, "ecopy --server: bad root path\n");
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
         * PUTFILE/MKDIR go to the apply pool (parallel, order-independent).
         * Everything else is an ordering/consistency point: drain the pool so
         * all prior fire-and-forget work is on disk, then handle inline.
         */
        if ((type == MSG_PUTFILE || type == MSG_MKDIR) && g_pool_n > 0) {
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
        case MSG_BARRIER:   rc = handle_barrier(id, payload, plen); break;
        case MSG_UNLINK:    handle_unlink(payload, plen); break;
        case MSG_BYE:       free(payload); pool_shutdown(); return 0;
        default:            break; /* ignore unknown */
        }
        free(payload);
        if (rc != 0) {
            /* Failed to write a reply: the channel is gone. */
            break;
        }
    }

    pool_shutdown();

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
