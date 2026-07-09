/*
 * protocol_test.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Unit tests for the ecopy SSH wire protocol: encode/decode round-trips and
 * frame read/write over a pipe. Build with `make protocol_test` and run the
 * resulting ./protocol_test binary (exit 0 == all passed).
 */

#define _GNU_SOURCE
#include "../protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

static int failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s\n", msg); failures++; } \
} while (0)

static void test_int_roundtrip(void)
{
    uint8_t buf[128];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u8(&e, 0xAB);
    penc_u32(&e, 0x11223344u);
    penc_u64(&e, 0x1122334455667788ULL);
    penc_i64(&e, -1234567890123LL);
    CHECK(!e.overflow, "encode did not overflow");

    pdec_t d; pdec_init(&d, buf, e.len);
    CHECK(pdec_u8(&d) == 0xAB, "u8 roundtrip");
    CHECK(pdec_u32(&d) == 0x11223344u, "u32 roundtrip");
    CHECK(pdec_u64(&d) == 0x1122334455667788ULL, "u64 roundtrip");
    CHECK(pdec_i64(&d) == -1234567890123LL, "i64 roundtrip");
    CHECK(!d.error, "decode had no error");
    CHECK(d.off == e.len, "decode consumed exactly all bytes");
}

static void test_str_roundtrip(void)
{
    uint8_t buf[512];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_str(&e, "/remote/path/with spaces and 'quotes'");
    penc_str(&e, "");
    CHECK(!e.overflow, "str encode did not overflow");

    pdec_t d; pdec_init(&d, buf, e.len);
    char out[256];
    CHECK(pdec_str(&d, out, sizeof(out)) == 0, "str decode ok");
    CHECK(strcmp(out, "/remote/path/with spaces and 'quotes'") == 0, "str content matches");
    CHECK(pdec_str(&d, out, sizeof(out)) == 0, "empty str decode ok");
    CHECK(out[0] == '\0', "empty str is empty");
    CHECK(!d.error, "no decode error");
}

static void test_str_too_long(void)
{
    uint8_t buf[64];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_str(&e, "abcdefghijklmnop");

    pdec_t d; pdec_init(&d, buf, e.len);
    char tiny[4];
    CHECK(pdec_str(&d, tiny, sizeof(tiny)) == -1, "oversized string rejected");
    CHECK(d.error, "decode flagged error on overflow");
}

static void test_overflow_guard(void)
{
    uint8_t buf[4];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u64(&e, 1);   /* needs 8, only 4 available */
    CHECK(e.overflow, "encode overflow detected");
}

struct frame_expect {
    uint8_t type;
    uint64_t id;
    uint32_t plen;
    uint8_t payload[64];
};

static void *writer_thread(void *arg)
{
    int fd = *(int *)arg;
    uint8_t p1[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    frame_write(fd, MSG_MKDIR, 0x42, p1, sizeof(p1));
    frame_write(fd, MSG_STATUS, 0x43, NULL, 0);
    frame_write_data(fd, 0x44, 0x99, 4096, "DATA", 4);
    close(fd);
    return NULL;
}

static void test_frame_pipe(void)
{
    int fds[2];
    if (pipe(fds) != 0) { CHECK(0, "pipe created"); return; }

    pthread_t t;
    pthread_create(&t, NULL, writer_thread, &fds[1]);

    uint8_t type; uint64_t id; uint32_t plen; uint8_t pl[64];

    CHECK(frame_read_header(fds[0], &type, &id, &plen) == 0, "read frame 1 header");
    CHECK(type == MSG_MKDIR && id == 0x42 && plen == 8, "frame 1 header fields");
    CHECK(io_read_all(fds[0], pl, plen) == 0, "read frame 1 payload");
    CHECK(pl[0] == 1 && pl[7] == 8, "frame 1 payload bytes");

    CHECK(frame_read_header(fds[0], &type, &id, &plen) == 0, "read frame 2 header");
    CHECK(type == MSG_STATUS && id == 0x43 && plen == 0, "frame 2 header fields (empty)");

    CHECK(frame_read_header(fds[0], &type, &id, &plen) == 0, "read frame 3 header");
    CHECK(type == MSG_WRITE && id == 0x44 && plen == 20, "frame 3 (data) header fields");
    CHECK(io_read_all(fds[0], pl, plen) == 0, "read frame 3 payload");
    {
        pdec_t d; pdec_init(&d, pl, plen);
        CHECK(pdec_u64(&d) == 0x99, "data frame file_id");
        CHECK(pdec_i64(&d) == 4096, "data frame offset");
        CHECK(memcmp(pl + 16, "DATA", 4) == 0, "data frame bytes");
    }

    /* Writer closed after 3 frames: next header read must fail. */
    CHECK(frame_read_header(fds[0], &type, &id, &plen) == -1, "EOF after last frame");

    pthread_join(t, NULL);
    close(fds[0]);
}

int main(void)
{
    test_int_roundtrip();
    test_str_roundtrip();
    test_str_too_long();
    test_overflow_guard();
    test_frame_pipe();

    if (failures == 0) {
        printf("protocol_test: all checks passed\n");
        return 0;
    }
    fprintf(stderr, "protocol_test: %d check(s) failed\n", failures);
    return 1;
}
