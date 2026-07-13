/*
 * ssh_transport.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Client transport for ecopy SSH targets. Spawns `ssh host ecopy --server`,
 * performs a version/capability handshake (bootstrapping the remote binary if
 * needed), and multiplexes pipelined destination operations over the single
 * SSH channel. Data frames are fire-and-forget; control operations use a small
 * request table matched by the receiver thread.
 */

#define _GNU_SOURCE
#include "ssh_transport.h"
#include "copy_policy.h"
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/utsname.h>
#include <ctype.h>

/* -------------------- pending request table -------------------- */

typedef struct pending {
    uint64_t id;
    int done;
    int32_t status;
    uint8_t *resp;
    uint32_t resp_len;
    struct pending *next;
} pending_t;

typedef struct {
    int active;
    int failed;
    pid_t pid;
    int in_fd;    /* we write -> ssh stdin -> remote stdin */
    int out_fd;   /* remote stdout -> ssh stdout -> we read */
    uint32_t caps;

    pthread_mutex_t send_lock;
    pthread_mutex_t pend_lock;
    pthread_cond_t  pend_cond;
    pending_t *pending;

    pthread_t rx_thread;
    int rx_running;

    _Atomic uint64_t next_id;
    _Atomic uint64_t next_file_id;

    _Atomic uint64_t ops_since_barrier;  /* fire-and-forget ops awaiting a drain */
    uint64_t barrier_ops;                /* periodic-barrier threshold */
    int remote_failed;                   /* a barrier reported remote errors */
    pthread_mutex_t barrier_lock;        /* only one periodic barrier in flight */

    char remote_root[PATH_MAX];
    int remote_root_present;             /* dst root existed before this run */
} conn_t;

static conn_t g_conn;

struct sshx_file {
    uint64_t file_id;
    char final_path[PATH_MAX];
    int failed;
};

/* -------------------- small helpers -------------------- */

static void conn_mark_failed(void)
{
    pthread_mutex_lock(&g_conn.pend_lock);
    g_conn.failed = 1;
    pthread_cond_broadcast(&g_conn.pend_cond);
    pthread_mutex_unlock(&g_conn.pend_lock);
}

static uint64_t next_request_id(void)
{
    return atomic_fetch_add(&g_conn.next_id, 1) + 1;
}

/* Send a fully-built control frame under the send lock. */
static int conn_send(uint8_t type, uint64_t id, const void *payload, uint32_t plen)
{
    int rc;
    pthread_mutex_lock(&g_conn.send_lock);
    rc = frame_write(g_conn.in_fd, type, id, payload, plen);
    pthread_mutex_unlock(&g_conn.send_lock);
    if (rc != 0) {
        conn_mark_failed();
    }
    return rc;
}

static pending_t *pending_add(uint64_t id)
{
    pending_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;
    p->id = id;
    pthread_mutex_lock(&g_conn.pend_lock);
    p->next = g_conn.pending;
    g_conn.pending = p;
    pthread_mutex_unlock(&g_conn.pend_lock);
    return p;
}

static void pending_remove(pending_t *p)
{
    pthread_mutex_lock(&g_conn.pend_lock);
    pending_t **pp = &g_conn.pending;
    while (*pp) {
        if (*pp == p) { *pp = p->next; break; }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&g_conn.pend_lock);
    free(p->resp);
    free(p);
}

/*
 * Wait for a reply to a pending request. On return, *status and (optionally)
 * the response bytes are available in p. Returns 0 if a reply arrived, -1 if
 * the connection failed first.
 */
static int pending_wait(pending_t *p)
{
    int rc = 0;
    pthread_mutex_lock(&g_conn.pend_lock);
    while (!p->done && !g_conn.failed) {
        pthread_cond_wait(&g_conn.pend_cond, &g_conn.pend_lock);
    }
    if (!p->done) rc = -1;
    pthread_mutex_unlock(&g_conn.pend_lock);
    return rc;
}

/* -------------------- receiver thread -------------------- */

static void *rx_main(void *arg)
{
    (void)arg;
    for (;;) {
        uint8_t type;
        uint64_t id;
        uint32_t plen;
        uint8_t *payload = NULL;

        if (frame_read_header(g_conn.out_fd, &type, &id, &plen) != 0) {
            break;
        }
        if (plen > ECOPY_MAX_FRAME) {
            break;
        }
        if (plen) {
            payload = malloc(plen);
            if (!payload || io_read_all(g_conn.out_fd, payload, plen) != 0) {
                free(payload);
                break;
            }
        }

        /* Match to a pending request. */
        pthread_mutex_lock(&g_conn.pend_lock);
        pending_t *p = g_conn.pending;
        while (p && p->id != id) p = p->next;
        if (p) {
            int32_t status = 0;
            if (type == MSG_STATUS) {
                pdec_t d; pdec_init(&d, payload, plen);
                status = (int32_t)pdec_u32(&d);
            }
            p->status = status;
            p->resp = payload;      /* transfer ownership */
            p->resp_len = plen;
            payload = NULL;
            p->done = 1;
            pthread_cond_broadcast(&g_conn.pend_cond);
        }
        pthread_mutex_unlock(&g_conn.pend_lock);
        free(payload);
    }

    conn_mark_failed();
    return NULL;
}

/* -------------------- URL parsing -------------------- */

int ssh_target_is_url(const char *s)
{
    return s && strncmp(s, "ssh://", 6) == 0;
}

int ssh_target_parse(const char *s, ssh_target_t *out)
{
    if (!ssh_target_is_url(s)) return -1;
    memset(out, 0, sizeof(*out));

    const char *p = s + 6;              /* after ssh:// */
    const char *slash = strchr(p, '/'); /* start of path */
    if (!slash) return -1;

    char authority[512];
    size_t alen = (size_t)(slash - p);
    if (alen == 0 || alen >= sizeof(authority)) return -1;
    memcpy(authority, p, alen);
    authority[alen] = '\0';

    /* path is everything from the slash (absolute on the remote host) */
    if (snprintf(out->path, sizeof(out->path), "%s", slash) >= (int)sizeof(out->path)) {
        return -1;
    }

    char *hostpart = authority;
    char *at = strchr(authority, '@');
    if (at) {
        *at = '\0';
        if (snprintf(out->user, sizeof(out->user), "%s", authority) >= (int)sizeof(out->user)) {
            return -1;
        }
        hostpart = at + 1;
    }

    char *colon = strrchr(hostpart, ':');
    if (colon) {
        *colon = '\0';
        char *endp = NULL;
        long port = strtol(colon + 1, &endp, 10);
        if (!endp || *endp != '\0' || port <= 0 || port > 65535) return -1;
        out->port = (int)port;
    }
    if (*hostpart == '\0') return -1;
    if (snprintf(out->host, sizeof(out->host), "%s", hostpart) >= (int)sizeof(out->host)) {
        return -1;
    }
    return 0;
}

/* -------------------- ssh process spawn -------------------- */

/* Single-quote a string for a POSIX shell. */
static int shell_squote(const char *in, char *out, size_t outsz)
{
    size_t o = 0;
    if (o + 1 >= outsz) return -1;
    out[o++] = '\'';
    for (const char *p = in; *p; p++) {
        if (*p == '\'') {
            if (o + 4 >= outsz) return -1;
            out[o++] = '\''; out[o++] = '\\'; out[o++] = '\''; out[o++] = '\'';
        } else {
            if (o + 1 >= outsz) return -1;
            out[o++] = *p;
        }
    }
    if (o + 2 > outsz) return -1;
    out[o++] = '\'';
    out[o] = '\0';
    return 0;
}

static const char *ssh_binary(void)
{
    const char *e = getenv("ECOPY_SSH");
    return (e && *e) ? e : "ssh";
}

static const char *remote_ecopy_cmd(void)
{
    const char *e = getenv("ECOPY_REMOTE_CMD");
    return (e && *e) ? e : "ecopy";
}

/*
 * Spawn `ssh [-p port] [user@]host <remote_command>` with the child's
 * stdin/stdout connected to the in_fd/out_fd out-parameters. remote_command is
 * a single string passed to the remote shell. Returns 0 on success.
 */
static int spawn_ssh(const ssh_target_t *t, const char *remote_command,
                     int *in_fd, int *out_fd, pid_t *pid_out)
{
    int inpipe[2];   /* parent writes inpipe[1] -> child stdin inpipe[0] */
    int outpipe[2];  /* child stdout outpipe[1] -> parent reads outpipe[0] */

    if (pipe(inpipe) != 0) { perror("pipe"); return -1; }
    if (pipe(outpipe) != 0) { perror("pipe"); close(inpipe[0]); close(inpipe[1]); return -1; }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        close(inpipe[0]); close(inpipe[1]); close(outpipe[0]); close(outpipe[1]);
        return -1;
    }
    if (pid == 0) {
        /* child */
        dup2(inpipe[0], STDIN_FILENO);
        dup2(outpipe[1], STDOUT_FILENO);
        close(inpipe[0]); close(inpipe[1]);
        close(outpipe[0]); close(outpipe[1]);

        char host_arg[512];
        if (t->user[0]) {
            snprintf(host_arg, sizeof(host_arg), "%s@%s", t->user, t->host);
        } else {
            snprintf(host_arg, sizeof(host_arg), "%s", t->host);
        }

        char portbuf[16];
        char *argv[10];
        int ai = 0;
        argv[ai++] = (char *)ssh_binary();
        if (t->port > 0) {
            snprintf(portbuf, sizeof(portbuf), "%d", t->port);
            argv[ai++] = "-p";
            argv[ai++] = portbuf;
        }
        argv[ai++] = "-o";
        argv[ai++] = "BatchMode=no";
        argv[ai++] = host_arg;
        argv[ai++] = (char *)remote_command;
        argv[ai] = NULL;

        execvp(argv[0], argv);
        perror("execvp ssh");
        _exit(127);
    }

    /* parent */
    close(inpipe[0]);
    close(outpipe[1]);
    *in_fd = inpipe[1];
    *out_fd = outpipe[0];
    *pid_out = pid;
    return 0;
}

/* -------------------- handshake + bootstrap -------------------- */

static void local_uname(char *out, size_t outsz)
{
    struct utsname u;
    if (uname(&u) == 0) {
        snprintf(out, outsz, "%s %s", u.sysname, u.machine);
    } else {
        snprintf(out, outsz, "unknown unknown");
    }
}

/* Build the "ecopy --server '<path>'" remote command into out. */
static int build_server_command(const char *remote_bin, const char *path,
                                 char *out, size_t outsz)
{
    char qpath[PATH_MAX + 16];
    if (shell_squote(path, qpath, sizeof(qpath)) != 0) return -1;
    if (snprintf(out, outsz, "%s --server %s", remote_bin, qpath) >= (int)outsz) {
        return -1;
    }
    return 0;
}

/*
 * Perform the HELLO handshake synchronously (receiver thread not yet running).
 * Returns 0 on success (fills g_conn.caps and remote_root confirmation), 1 if
 * the handshake failed in a way that warrants a bootstrap attempt, -1 fatal.
 */
static int handshake(const char *expect_path)
{
    uint8_t buf[1024];
    penc_t e;
    char lname[160];
    local_uname(lname, sizeof(lname));

    /* How many apply threads the remote should run (hides per-op NFS RPC
     * latency). ssh env usually does not propagate, so we pass it in HELLO. */
    int want_threads;
    {
        const char *s = getenv("DIRECT_COPY_SSH_SERVER_THREADS");
        long v = (s && *s) ? strtol(s, NULL, 10) : 0;
        if (v <= 0) v = 16;             /* default */
        if (v > 256) v = 256;
        want_threads = (int)v;
    }

    uint32_t options = copy_policy_preserve_times()
                           ? ECOPY_OPT_PRESERVE_TIMES
                           : 0;

    penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, ECOPY_PROTO_VERSION);
    penc_u32(&e, ECOPY_CAP_FALLOCATE | ECOPY_CAP_SPARSE);
    penc_str(&e, lname);
    penc_str(&e, expect_path);
    penc_u32(&e, (uint32_t)want_threads);
    penc_u32(&e, options);
    if (e.overflow) return -1;

    if (frame_write(g_conn.in_fd, MSG_HELLO, 1, buf, (uint32_t)e.len) != 0) {
        return 1; /* pipe broke immediately: likely command not found */
    }

    uint8_t type;
    uint64_t id;
    uint32_t plen;
    if (frame_read_header(g_conn.out_fd, &type, &id, &plen) != 0) {
        return 1; /* no reply: remote ecopy missing or failed to start */
    }
    if (type != MSG_HELLO_OK || plen > sizeof(buf)) {
        /* Drain and treat as incompatible. */
        if (plen) { uint8_t tmp[1024]; uint32_t left = plen; while (left) { uint32_t c = left > sizeof(tmp) ? sizeof(tmp) : left; if (io_read_all(g_conn.out_fd, tmp, c) != 0) break; left -= c; } }
        return 1;
    }

    uint8_t payload[1024];
    if (io_read_all(g_conn.out_fd, payload, plen) != 0) return 1;

    pdec_t d; pdec_init(&d, payload, plen);
    uint32_t sver = pdec_u32(&d);
    uint32_t scaps = pdec_u32(&d);
    char sname[128];
    char sroot[PATH_MAX];
    if (pdec_str(&d, sname, sizeof(sname)) != 0) return 1;
    if (pdec_str(&d, sroot, sizeof(sroot)) != 0) return 1;
    if (d.error) return 1;

    if (sver != ECOPY_PROTO_VERSION) {
        return 1; /* version mismatch: bootstrap a matching binary */
    }
    /* v2 appends a root-present flag; absent bytes decode as 0 (fresh). */
    uint8_t root_present = pdec_u8(&d);
    g_conn.caps = scaps;
    g_conn.remote_root_present = (!d.error && root_present) ? 1 : 0;
    snprintf(g_conn.remote_root, sizeof(g_conn.remote_root), "%s", sroot);
    return 0;
}

/* Capture stdout of `ssh host uname -sm` (best effort). Returns 0 on success. */
static int remote_uname(const ssh_target_t *t, char *out, size_t outsz)
{
    int in_fd, out_fd;
    pid_t pid;
    if (spawn_ssh(t, "uname -sm", &in_fd, &out_fd, &pid) != 0) return -1;
    close(in_fd);

    size_t total = 0;
    char buf[160];
    ssize_t r;
    while ((r = read(out_fd, buf + total, sizeof(buf) - 1 - total)) > 0) {
        total += (size_t)r;
        if (total >= sizeof(buf) - 1) break;
    }
    close(out_fd);
    int status;
    waitpid(pid, &status, 0);

    buf[total] = '\0';
    /* trim trailing whitespace/newline */
    while (total > 0 && isspace((unsigned char)buf[total - 1])) buf[--total] = '\0';
    if (total == 0) return -1;
    snprintf(out, outsz, "%s", buf);
    return 0;
}

/*
 * Stream the local ecopy binary to a fixed remote path and mark it executable.
 * Returns the remote path (into out) on success.
 */
static int bootstrap_upload_binary(const ssh_target_t *t, char *out, size_t outsz)
{
    /* Confirm architecture/OS match before shipping a binary. */
    char lname[160], rname[160];
    local_uname(lname, sizeof(lname));
    if (remote_uname(t, rname, sizeof(rname)) != 0) {
        fprintf(stderr, "ecopy: cannot determine remote platform for bootstrap\n");
        return -1;
    }
    if (strcmp(lname, rname) != 0) {
        fprintf(stderr,
                "ecopy: remote ecopy missing and platform differs (local '%s' vs remote '%s'); "
                "install a compatible ecopy on the remote host.\n",
                lname, rname);
        return -1;
    }

    int self_fd = open("/proc/self/exe", O_RDONLY | O_CLOEXEC);
    if (self_fd < 0) {
        perror("open /proc/self/exe");
        return -1;
    }

    char remote_path[PATH_MAX];
    snprintf(remote_path, sizeof(remote_path), "/tmp/.ecopy-boot-%u-v%u",
             (unsigned)getuid(), (unsigned)ECOPY_PROTO_VERSION);

    char qpath[PATH_MAX + 16];
    if (shell_squote(remote_path, qpath, sizeof(qpath)) != 0) { close(self_fd); return -1; }

    char cmd[5 * PATH_MAX];
    snprintf(cmd, sizeof(cmd), "cat > %s.tmp && chmod +x %s.tmp && mv %s.tmp %s",
             qpath, qpath, qpath, qpath);

    int in_fd, out_fd;
    pid_t pid;
    if (spawn_ssh(t, cmd, &in_fd, &out_fd, &pid) != 0) { close(self_fd); return -1; }
    close(out_fd);

    fprintf(stderr, "ecopy: bootstrapping remote binary to %s ...\n", remote_path);

    char io[1 << 16];
    ssize_t r;
    int ok = 1;
    while ((r = read(self_fd, io, sizeof(io))) > 0) {
        if (io_write_all(in_fd, io, (size_t)r) != 0) { ok = 0; break; }
    }
    if (r < 0) ok = 0;
    close(self_fd);
    close(in_fd);

    int status;
    waitpid(pid, &status, 0);
    if (!ok || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "ecopy: bootstrap upload failed\n");
        return -1;
    }

    snprintf(out, outsz, "%s", remote_path);
    return 0;
}

/* -------------------- connect / disconnect -------------------- */

static void conn_close_child(void)
{
    if (g_conn.in_fd >= 0) { close(g_conn.in_fd); g_conn.in_fd = -1; }
    if (g_conn.out_fd >= 0) { close(g_conn.out_fd); g_conn.out_fd = -1; }
    if (g_conn.pid > 0) {
        int status;
        waitpid(g_conn.pid, &status, 0);
        g_conn.pid = -1;
    }
}

int sshx_connect(const ssh_target_t *t)
{
    memset(&g_conn, 0, sizeof(g_conn));
    g_conn.in_fd = -1;
    g_conn.out_fd = -1;
    g_conn.pid = -1;
    pthread_mutex_init(&g_conn.send_lock, NULL);
    pthread_mutex_init(&g_conn.pend_lock, NULL);
    pthread_cond_init(&g_conn.pend_cond, NULL);
    pthread_mutex_init(&g_conn.barrier_lock, NULL);
    atomic_store(&g_conn.next_id, 1);
    atomic_store(&g_conn.next_file_id, 1);
    atomic_store(&g_conn.ops_since_barrier, 0);

    {
        const char *s = getenv("DIRECT_COPY_SSH_BARRIER_OPS");
        long v = (s && *s) ? strtol(s, NULL, 10) : 0;
        if (v < 256) v = 8192;          /* default / floor */
        if (v > 1000000) v = 1000000;
        g_conn.barrier_ops = (uint64_t)v;
    }

    /* SIGPIPE would kill us if ssh dies mid-write; treat write errors instead. */
    signal(SIGPIPE, SIG_IGN);

    char cmd[PATH_MAX + 128];
    if (build_server_command(remote_ecopy_cmd(), t->path, cmd, sizeof(cmd)) != 0) {
        fprintf(stderr, "ecopy: remote command too long\n");
        return -1;
    }

    if (spawn_ssh(t, cmd, &g_conn.in_fd, &g_conn.out_fd, &g_conn.pid) != 0) {
        return -1;
    }

    int hs = handshake(t->path);
    if (hs == 1) {
        /* Attempt to bootstrap a matching binary and retry once. */
        conn_close_child();
        char remote_path[PATH_MAX];
        if (bootstrap_upload_binary(t, remote_path, sizeof(remote_path)) != 0) {
            return -1;
        }
        if (build_server_command(remote_path, t->path, cmd, sizeof(cmd)) != 0) {
            return -1;
        }
        if (spawn_ssh(t, cmd, &g_conn.in_fd, &g_conn.out_fd, &g_conn.pid) != 0) {
            return -1;
        }
        hs = handshake(t->path);
    }
    if (hs != 0) {
        fprintf(stderr, "ecopy: SSH handshake with remote failed\n");
        conn_close_child();
        return -1;
    }

    if (pthread_create(&g_conn.rx_thread, NULL, rx_main, NULL) != 0) {
        perror("pthread_create rx");
        conn_close_child();
        return -1;
    }
    g_conn.rx_running = 1;
    g_conn.active = 1;
    return 0;
}

int sshx_active(void)
{
    return g_conn.active;
}

const char *sshx_remote_root(void)
{
    return g_conn.remote_root;
}

int sshx_remote_root_present(void)
{
    return g_conn.remote_root_present;
}

void sshx_disconnect(void)
{
    if (!g_conn.active) return;

    /* Best-effort clean BYE, then close our write end so the server sees EOF. */
    (void)conn_send(MSG_BYE, next_request_id(), NULL, 0);
    if (g_conn.in_fd >= 0) { close(g_conn.in_fd); g_conn.in_fd = -1; }

    if (g_conn.rx_running) {
        pthread_join(g_conn.rx_thread, NULL);
        g_conn.rx_running = 0;
    }
    if (g_conn.out_fd >= 0) { close(g_conn.out_fd); g_conn.out_fd = -1; }
    if (g_conn.pid > 0) {
        int status;
        waitpid(g_conn.pid, &status, 0);
        g_conn.pid = -1;
    }
    g_conn.active = 0;
}

/* -------------------- control operations -------------------- */

/* Send a control request that expects a MSG_STATUS reply; returns 0/-1. */
static int request_status(uint8_t type, const uint8_t *payload, uint32_t plen)
{
    uint64_t id = next_request_id();
    pending_t *p = pending_add(id);
    if (!p) return -1;

    if (conn_send(type, id, payload, plen) != 0) {
        pending_remove(p);
        return -1;
    }
    if (pending_wait(p) != 0) {
        pending_remove(p);
        return -1;
    }
    int32_t status = p->status;
    pending_remove(p);
    if (status != 0) {
        errno = (status < 0) ? -status : status;
        return -1;
    }
    return 0;
}

/*
 * Count one fire-and-forget op and, once enough have accumulated, issue a
 * periodic BARRIER to bound the outstanding window and surface remote errors
 * mid-run. Guarded so only one periodic barrier is ever in flight.
 */
static void maybe_periodic_barrier(void)
{
    uint64_t n = atomic_fetch_add(&g_conn.ops_since_barrier, 1) + 1;
    if (n < g_conn.barrier_ops) {
        return;
    }
    if (pthread_mutex_trylock(&g_conn.barrier_lock) != 0) {
        return;
    }
    if (atomic_load(&g_conn.ops_since_barrier) >= g_conn.barrier_ops) {
        atomic_store(&g_conn.ops_since_barrier, 0);
        (void)sshx_barrier(0);
    }
    pthread_mutex_unlock(&g_conn.barrier_lock);
}

int sshx_mkdir(const char *path, mode_t mode)
{
    uint8_t buf[PATH_MAX + 16];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_str(&e, path);
    penc_u32(&e, (uint32_t)mode);
    if (e.overflow) { errno = ENAMETOOLONG; return -1; }
    /* Fire-and-forget: errors surface at the next barrier. */
    if (conn_send(MSG_MKDIR, next_request_id(), buf, (uint32_t)e.len) != 0) {
        return -1;
    }
    maybe_periodic_barrier();
    return 0;
}

static void decode_stat_entry(pdec_t *d, int *present, struct stat *st)
{
    uint8_t pr = pdec_u8(d);
    uint32_t mode = pdec_u32(d);
    int64_t size = pdec_i64(d);
    int64_t mt_sec = pdec_i64(d);
    int64_t mt_nsec = pdec_i64(d);
    *present = pr ? 1 : 0;
    if (pr) {
        memset(st, 0, sizeof(*st));
        st->st_mode = (mode_t)mode;
        st->st_size = (off_t)size;
        st->st_mtim.tv_sec = (time_t)mt_sec;
        st->st_mtim.tv_nsec = (long)mt_nsec;
    }
}

int sshx_stat(const char *path, struct stat *st)
{
    uint8_t buf[PATH_MAX + 8];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_str(&e, path);
    if (e.overflow) { errno = ENAMETOOLONG; return -1; }

    uint64_t id = next_request_id();
    pending_t *p = pending_add(id);
    if (!p) return -1;
    if (conn_send(MSG_STAT, id, buf, (uint32_t)e.len) != 0) { pending_remove(p); return -1; }
    if (pending_wait(p) != 0) { pending_remove(p); return -1; }

    int present = 0;
    pdec_t d; pdec_init(&d, p->resp, p->resp_len);
    decode_stat_entry(&d, &present, st);
    int err = d.error;
    pending_remove(p);
    if (err) return -1;
    return present ? 1 : 0;
}

int sshx_stat_bulk(const char *base, const char *const *names, int n,
                   int *results, struct stat *st)
{
    /* Build request: base, count, names. */
    size_t cap = (size_t)PATH_MAX + 16;
    for (int i = 0; i < n; i++) cap += strlen(names[i]) + 4;
    uint8_t *buf = malloc(cap);
    if (!buf) return -1;
    penc_t e; penc_init(&e, buf, cap);
    penc_str(&e, base);
    penc_u32(&e, (uint32_t)n);
    for (int i = 0; i < n; i++) penc_str(&e, names[i]);
    if (e.overflow) { free(buf); return -1; }

    uint64_t id = next_request_id();
    pending_t *p = pending_add(id);
    if (!p) { free(buf); return -1; }
    int sent = conn_send(MSG_STAT_BULK, id, buf, (uint32_t)e.len);
    free(buf);
    if (sent != 0) { pending_remove(p); return -1; }
    if (pending_wait(p) != 0) { pending_remove(p); return -1; }

    pdec_t d; pdec_init(&d, p->resp, p->resp_len);
    uint32_t cnt = pdec_u32(&d);
    int rc = 0;
    if ((int)cnt != n) {
        rc = -1;
    } else {
        for (int i = 0; i < n; i++) {
            decode_stat_entry(&d, &results[i], &st[i]);
        }
        if (d.error) rc = -1;
    }
    pending_remove(p);
    return rc;
}

static void encode_meta(penc_t *e, const struct stat *s)
{
    penc_u32(e, (uint32_t)s->st_uid);
    penc_u32(e, (uint32_t)s->st_gid);
    penc_u32(e, (uint32_t)(s->st_mode & 07777));
    penc_i64(e, (int64_t)s->st_atim.tv_sec);
    penc_i64(e, (int64_t)s->st_atim.tv_nsec);
    penc_i64(e, (int64_t)s->st_mtim.tv_sec);
    penc_i64(e, (int64_t)s->st_mtim.tv_nsec);
}

int sshx_setmeta(const char *path, const struct stat *src_st, int is_dir)
{
    uint8_t buf[PATH_MAX + 64];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_str(&e, path);
    encode_meta(&e, src_st);
    penc_u8(&e, (uint8_t)(is_dir ? 1 : 0));
    penc_u8(&e, (uint8_t)(verify_metadata_enabled() ? 1 : 0));
    if (e.overflow) { errno = ENAMETOOLONG; return -1; }
    /* Fire-and-forget: errors surface at the next barrier. */
    if (conn_send(MSG_SETMETA, next_request_id(), buf, (uint32_t)e.len) != 0) {
        return -1;
    }
    maybe_periodic_barrier();
    return 0;
}

int sshx_putfile(const char *final_path, const struct stat *src_st,
                 mode_t parent_mode, const void *buf, size_t len, int inplace)
{
    uint8_t head[PATH_MAX + 64];
    penc_t e; penc_init(&e, head, sizeof(head));
    penc_u32(&e, (uint32_t)(src_st->st_mode & 07777));
    penc_u32(&e, (uint32_t)src_st->st_uid);
    penc_u32(&e, (uint32_t)src_st->st_gid);
    penc_i64(&e, (int64_t)src_st->st_atim.tv_sec);
    penc_i64(&e, (int64_t)src_st->st_atim.tv_nsec);
    penc_i64(&e, (int64_t)src_st->st_mtim.tv_sec);
    penc_i64(&e, (int64_t)src_st->st_mtim.tv_nsec);
    penc_u32(&e, (uint32_t)(inplace ? ECOPY_OPEN_INPLACE : 0));
    penc_u32(&e, (uint32_t)(parent_mode & 07777));
    penc_str(&e, final_path);
    if (e.overflow) { errno = ENAMETOOLONG; return -1; }

    int rc;
    pthread_mutex_lock(&g_conn.send_lock);
    rc = frame_write_parts(g_conn.in_fd, MSG_PUTFILE, next_request_id(),
                           head, (uint32_t)e.len, buf, len);
    pthread_mutex_unlock(&g_conn.send_lock);
    if (rc != 0) {
        conn_mark_failed();
        return -1;
    }
    maybe_periodic_barrier();
    return 0;
}

int sshx_barrier(int flush)
{
    uint8_t buf[1];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u8(&e, (uint8_t)(flush ? 1 : 0));

    uint64_t id = next_request_id();
    pending_t *p = pending_add(id);
    if (!p) return -1;
    if (conn_send(MSG_BARRIER, id, buf, (uint32_t)e.len) != 0) { pending_remove(p); return -1; }
    if (pending_wait(p) != 0) { pending_remove(p); return -1; }

    pdec_t d; pdec_init(&d, p->resp, p->resp_len);
    int32_t status = (int32_t)pdec_u32(&d);
    uint32_t err_count = pdec_u32(&d);
    char first[PATH_MAX];
    if (pdec_str(&d, first, sizeof(first)) != 0) first[0] = '\0';
    pending_remove(p);

    if (err_count > 0) {
        if (!g_conn.remote_failed) {
            fprintf(stderr, "ecopy: remote reported %u error(s); first: %s (%s)\n",
                    err_count, first[0] ? first : "(unknown)",
                    strerror(status < 0 ? -status : (status ? status : EIO)));
        }
        g_conn.remote_failed = 1;
        return -1;
    }
    return 0;
}

int sshx_flush(void)
{
    int rc = sshx_barrier(1);
    return (rc != 0 || g_conn.remote_failed) ? -1 : 0;
}

int sshx_verify_batch(const char *path, const struct stat *src_st,
                      const verify_digest_t *digests, size_t count, int final)
{
    size_t cap;
    uint8_t *buf;
    penc_t e;
    if (!g_conn.active || count > VERIFY_BATCH_MAX) {
        errno = EINVAL;
        return -1;
    }
    cap = strlen(path) + 128 + count * (8 + 4 + VERIFY_DIGEST_SIZE);
    if (cap > ECOPY_MAX_FRAME) {
        errno = E2BIG;
        return -1;
    }
    buf = malloc(cap);
    if (!buf) return -1;
    penc_init(&e, buf, cap);
    penc_str(&e, path);
    encode_meta(&e, src_st);
    penc_i64(&e, (int64_t)src_st->st_size);
    penc_u8(&e, (uint8_t)((final ? 1 : 0) |
                           (verify_metadata_enabled() ? 2 : 0)));
    penc_u32(&e, (uint32_t)count);
    for (size_t i = 0; i < count; i++) {
        penc_i64(&e, digests[i].offset);
        penc_u32(&e, digests[i].length);
        penc_bytes(&e, digests[i].digest, VERIFY_DIGEST_SIZE);
    }
    int rc = e.overflow
                 ? -1
                 : conn_send(MSG_VERIFY_PATH, next_request_id(), buf,
                             (uint32_t)e.len);
    free(buf);
    return rc;
}

/* -------------------- per-file streaming -------------------- */

sshx_file_t *sshx_file_begin(const char *final_path, mode_t mode,
                             mode_t parent_mode, off_t expected_size,
                             int sparse, int inplace)
{
    sshx_file_t *f = calloc(1, sizeof(*f));
    if (!f) return NULL;
    f->file_id = atomic_fetch_add(&g_conn.next_file_id, 1) + 1;
    snprintf(f->final_path, sizeof(f->final_path), "%s", final_path);

    uint8_t buf[PATH_MAX + 32];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u64(&e, f->file_id);
    penc_u32(&e, (uint32_t)(mode & 07777));
    penc_i64(&e, (int64_t)expected_size);
    uint32_t flags = 0;
    if (sparse) flags |= ECOPY_OPEN_SPARSE;
    if (inplace) flags |= ECOPY_OPEN_INPLACE;
    penc_u32(&e, flags);
    penc_u32(&e, (uint32_t)(parent_mode & 07777));
    penc_str(&e, final_path);
    if (e.overflow) { free(f); return NULL; }

    if (conn_send(MSG_OPEN, next_request_id(), buf, (uint32_t)e.len) != 0) {
        free(f);
        return NULL;
    }
    return f;
}

int sshx_file_write(sshx_file_t *f, const void *buf, size_t len, off_t offset)
{
    if (f->failed) return -1;
    int rc;
    pthread_mutex_lock(&g_conn.send_lock);
    rc = frame_write_data(g_conn.in_fd, next_request_id(), f->file_id, (int64_t)offset, buf, len);
    pthread_mutex_unlock(&g_conn.send_lock);
    if (rc != 0) {
        f->failed = 1;
        conn_mark_failed();
    }
    return rc;
}

int sshx_file_ftruncate(sshx_file_t *f, off_t size)
{
    if (f->failed) return -1;
    uint8_t buf[16];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u64(&e, f->file_id);
    penc_i64(&e, (int64_t)size);
    if (conn_send(MSG_FTRUNCATE, next_request_id(), buf, (uint32_t)e.len) != 0) {
        f->failed = 1;
        return -1;
    }
    return 0;
}

int sshx_file_commit(sshx_file_t *f, const struct stat *src_st)
{
    int rc;
    uint8_t buf[64];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u64(&e, f->file_id);
    encode_meta(&e, src_st);

    if (f->failed) {
        sshx_file_abort(f);
        return -1;
    }
    rc = request_status(MSG_COMMIT, buf, (uint32_t)e.len);
    free(f);
    return rc;
}

void sshx_file_abort(sshx_file_t *f)
{
    uint8_t buf[16];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u64(&e, f->file_id);
    (void)conn_send(MSG_ABORT, next_request_id(), buf, (uint32_t)e.len);
    free(f);
}
