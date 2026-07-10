/*
 * protocol.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Wire protocol for ecopy's native SSH target support. The local "client" and
 * the remote "ecopy --server" peer exchange length-prefixed, id-tagged binary
 * frames over a single SSH channel (the server's stdin/stdout). The design is
 * pipelined: hot data frames (OPEN/WRITE/FTRUNCATE/ABORT/UNLINK) are
 * fire-and-forget and rely on the OS pipe + SSH window for backpressure, so no
 * per-write round-trip is paid. Only control operations that must be confirmed
 * (HELLO, STAT, STAT_BULK, MKDIR, SETMETA, COMMIT) get a reply, and COMMIT is
 * where any deferred write error for a file surfaces.
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <sys/types.h>

#define ECOPY_PROTO_VERSION 2u

/* Upper bound on a single control-frame payload we are willing to read. Data
 * frames (WRITE) carry their own length and are streamed, so this only bounds
 * path lists and metadata. */
#define ECOPY_MAX_FRAME (16u * 1024u * 1024u)

typedef enum {
    MSG_HELLO          = 1,  /* client->server: version + caps + uname */
    MSG_HELLO_OK       = 2,  /* reply: version + caps + uname */
    MSG_MKDIR          = 3,  /* path, mode ; reply STATUS */
    MSG_STAT           = 4,  /* path ; reply STAT_RESP */
    MSG_STAT_RESP      = 5,  /* present, mode, size, mtime */
    MSG_STAT_BULK      = 6,  /* base, count, names ; reply STAT_BULK_RESP */
    MSG_STAT_BULK_RESP = 7,  /* count x {present, mode, size, mtime} */
    MSG_OPEN           = 8,  /* file_id, mode, size, flags, final_path ; fire */
    MSG_WRITE          = 9,  /* file_id, offset, data ; fire */
    MSG_FTRUNCATE      = 10, /* file_id, size ; fire */
    MSG_COMMIT         = 11, /* file_id, meta ; reply STATUS */
    MSG_ABORT          = 12, /* file_id ; fire */
    MSG_SETMETA        = 13, /* path, meta, is_dir ; reply STATUS */
    MSG_UNLINK         = 14, /* path ; fire */
    MSG_STATUS         = 15, /* status(i32) [+ barrier aggregate] */
    MSG_BYE            = 16, /* client->server: clean shutdown ; fire */
    MSG_PUTFILE        = 17, /* meta + path + data (whole small file) ; fire */
    MSG_BARRIER        = 18  /* flush + drain ; reply STATUS w/ aggregate */
} msg_type_t;

/*
 * In protocol v2 the small-file and directory paths are fire-and-forget to
 * remove per-item round trips:
 *   - MSG_PUTFILE carries an entire small file (metadata + path + data) in one
 *     frame and gets no reply.
 *   - MSG_MKDIR / MSG_SETMETA no longer reply; failures are accumulated in the
 *     server's error log.
 *   - MSG_BARRIER is the only synchronization point: the server drains all
 *     prior frames, optionally flushes, and replies MSG_STATUS whose payload is
 *     [status(i32), error_count(u32), first_failing_path(str)].
 * The streamed OPEN/WRITE/FTRUNCATE/COMMIT path (large + sparse files) is
 * unchanged and still confirms via COMMIT.
 */

/* OPEN / PUTFILE flags */
#define ECOPY_OPEN_SPARSE   0x1u  /* file is sparse; do not preallocate */
#define ECOPY_OPEN_INPLACE  0x2u  /* write straight to final name (no temp+rename) */

/* Capability bits exchanged in HELLO/HELLO_OK */
#define ECOPY_CAP_FALLOCATE 0x1u
#define ECOPY_CAP_SPARSE    0x2u

/*
 * Client option bits appended to HELLO (after the requested thread count).
 * Absent (older clients) decodes as 0; the server treats a missing
 * PRESERVE_TIMES bit as "preserve" so behavior is unchanged by default.
 */
#define ECOPY_OPT_PRESERVE_TIMES 0x1u  /* apply atime/mtime (else skip the SETATTR) */

/* Fixed frame header: type(u8), request id(u64 LE), payload length(u32 LE). */
#define ECOPY_HDR_LEN 13u

/* ---- little-endian cursor encode/decode helpers ---- */

typedef struct {
    uint8_t *buf;
    size_t   cap;
    size_t   len;
    int      overflow;
} penc_t;

typedef struct {
    const uint8_t *buf;
    size_t         len;
    size_t         off;
    int            error;
} pdec_t;

static inline void penc_init(penc_t *e, uint8_t *buf, size_t cap) {
    e->buf = buf; e->cap = cap; e->len = 0; e->overflow = 0;
}

static inline void penc_bytes(penc_t *e, const void *p, size_t n) {
    if (e->len + n > e->cap) { e->overflow = 1; return; }
    if (n) memcpy(e->buf + e->len, p, n);
    e->len += n;
}

static inline void penc_u8(penc_t *e, uint8_t v) { penc_bytes(e, &v, 1); }

static inline void penc_u32(penc_t *e, uint32_t v) {
    uint8_t b[4] = { (uint8_t)v, (uint8_t)(v >> 8), (uint8_t)(v >> 16), (uint8_t)(v >> 24) };
    penc_bytes(e, b, 4);
}

static inline void penc_u64(penc_t *e, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; i++) b[i] = (uint8_t)(v >> (8 * i));
    penc_bytes(e, b, 8);
}

static inline void penc_i64(penc_t *e, int64_t v) { penc_u64(e, (uint64_t)v); }

/* Length-prefixed string (u32 length then bytes, no NUL). */
static inline void penc_str(penc_t *e, const char *s) {
    uint32_t n = (uint32_t)(s ? strlen(s) : 0);
    penc_u32(e, n);
    penc_bytes(e, s, n);
}

static inline void pdec_init(pdec_t *d, const void *buf, size_t len) {
    d->buf = (const uint8_t *)buf; d->len = len; d->off = 0; d->error = 0;
}

static inline int pdec_bytes(pdec_t *d, void *out, size_t n) {
    if (d->error || d->off + n > d->len) { d->error = 1; return -1; }
    if (n) memcpy(out, d->buf + d->off, n);
    d->off += n;
    return 0;
}

static inline uint8_t pdec_u8(pdec_t *d) {
    uint8_t v = 0; pdec_bytes(d, &v, 1); return v;
}

static inline uint32_t pdec_u32(pdec_t *d) {
    uint8_t b[4] = {0}; if (pdec_bytes(d, b, 4) != 0) return 0;
    return (uint32_t)b[0] | ((uint32_t)b[1] << 8) | ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}

static inline uint64_t pdec_u64(pdec_t *d) {
    uint8_t b[8] = {0}; if (pdec_bytes(d, b, 8) != 0) return 0;
    uint64_t v = 0; for (int i = 0; i < 8; i++) v |= ((uint64_t)b[i]) << (8 * i);
    return v;
}

static inline int64_t pdec_i64(pdec_t *d) { return (int64_t)pdec_u64(d); }

/*
 * Decode a length-prefixed string into out (NUL-terminated, bounded by outsz).
 * Returns 0 on success, -1 on decode error or if it would not fit.
 */
static inline int pdec_str(pdec_t *d, char *out, size_t outsz) {
    uint32_t n = pdec_u32(d);
    if (d->error || n >= outsz || d->off + n > d->len) { d->error = 1; return -1; }
    if (n) memcpy(out, d->buf + d->off, n);
    out[n] = '\0';
    d->off += n;
    return 0;
}

/* ---- blocking full read/write and framing (implemented in protocol.c) ---- */

int io_write_all(int fd, const void *buf, size_t len);
int io_read_all(int fd, void *buf, size_t len);

/* Write one framed message (header + payload) to fd. Not internally locked;
 * the caller serializes writers. Returns 0 on success, -1 on I/O error. */
int frame_write(int fd, uint8_t type, uint64_t id, const void *payload, uint32_t plen);

/*
 * Write a WRITE frame without copying the bulk data: emits the header, the
 * fixed prefix (file_id, offset), then the data buffer directly. The payload
 * length encoded in the header is (16 + len).
 */
int frame_write_data(int fd, uint64_t id, uint64_t file_id, int64_t offset,
                     const void *data, size_t len);

/*
 * Write a frame in two segments without an intermediate copy of the bulk data:
 * the header (payload length = head_len + data_len), then head, then data. Used
 * by MSG_PUTFILE (head = metadata + path, data = file contents). Returns 0 on
 * success, -1 on I/O error.
 */
int frame_write_parts(int fd, uint8_t type, uint64_t id,
                      const void *head, uint32_t head_len,
                      const void *data, size_t data_len);

/* Read one frame header. Returns 0 on success, -1 on EOF/error. */
int frame_read_header(int fd, uint8_t *type, uint64_t *id, uint32_t *plen);

#endif
