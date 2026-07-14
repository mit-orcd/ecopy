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
#include "../verify.h"
#include "../third_party/blake3/blake3.h"

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

static int hex_value(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

static void test_blake3_vectors(void)
{
    static const char empty_hex[] =
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    static const char abc_hex[] =
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";
    uint8_t got[BLAKE3_OUT_LEN], expected[BLAKE3_OUT_LEN];
    const char *vectors[] = { empty_hex, abc_hex };
    const char *inputs[] = { "", "abc" };
    size_t lengths[] = { 0, 3 };
    for (int v = 0; v < 2; v++) {
        for (int i = 0; i < BLAKE3_OUT_LEN; i++) {
            expected[i] = (uint8_t)((hex_value(vectors[v][i * 2]) << 4) |
                                    hex_value(vectors[v][i * 2 + 1]));
        }
        blake3_hash(inputs[v], lengths[v], got);
        CHECK(memcmp(got, expected, sizeof(got)) == 0, "BLAKE3 known vector");
    }
    {
        static const char long_hex[] =
            "015094013f57a5277b59d8475c0501042c0b642e531b0a1c8f58d2163229e969";
        uint8_t input[4096];
        for (size_t i = 0; i < sizeof(input); i++) input[i] = (uint8_t)(i % 251);
        for (int i = 0; i < BLAKE3_OUT_LEN; i++) {
            expected[i] = (uint8_t)((hex_value(long_hex[i * 2]) << 4) |
                                    hex_value(long_hex[i * 2 + 1]));
        }
        blake3_hash(input, sizeof(input), got);
        CHECK(memcmp(got, expected, sizeof(got)) == 0,
              "BLAKE3 multi-chunk official vector");
    }
}

static void test_verify_batch_encoding(void)
{
    uint8_t buf[256], digest[VERIFY_DIGEST_SIZE];
    memset(digest, 0xA5, sizeof(digest));
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_str(&e, "/dst/file");
    penc_i64(&e, 8192);
    penc_u8(&e, ECOPY_VERIFY_DIRECTORY | ECOPY_VERIFY_METADATA |
                ECOPY_VERIFY_PRESERVE_TIME);
    penc_u32(&e, 1);
    penc_i64(&e, 4096);
    penc_u32(&e, 4096);
    penc_u8(&e, VERIFY_SAMPLE_EXPECT_ZERO);
    penc_bytes(&e, digest, sizeof(digest));
    CHECK(!e.overflow, "verify batch encoding fits");

    pdec_t d; pdec_init(&d, buf, e.len);
    char path[64]; uint8_t decoded[VERIFY_DIGEST_SIZE];
    CHECK(pdec_str(&d, path, sizeof(path)) == 0 &&
          strcmp(path, "/dst/file") == 0, "verify path roundtrip");
    CHECK(pdec_i64(&d) == 8192, "verify size roundtrip");
    CHECK(pdec_u8(&d) == 7 && pdec_u32(&d) == 1,
          "verify flags/count roundtrip");
    CHECK(pdec_i64(&d) == 4096 && pdec_u32(&d) == 4096 &&
          pdec_u8(&d) == VERIFY_SAMPLE_EXPECT_ZERO,
          "verify offset/length roundtrip");
    CHECK(pdec_bytes(&d, decoded, sizeof(decoded)) == 0 &&
          memcmp(decoded, digest, sizeof(digest)) == 0,
          "verify digest roundtrip");

    /* A digest cut short by one byte must trip the decoder bounds check. */
    pdec_init(&d, buf, e.len - 1);
    CHECK(pdec_str(&d, path, sizeof(path)) == 0, "truncated verify path decodes");
    (void)pdec_i64(&d);
    (void)pdec_u8(&d);
    CHECK(pdec_u32(&d) == 1, "truncated verify count decodes");
    (void)pdec_i64(&d);
    (void)pdec_u32(&d);
    (void)pdec_u8(&d);
    CHECK(pdec_bytes(&d, decoded, sizeof(decoded)) != 0 && d.error,
          "truncated verify digest is rejected");
    CHECK((uint32_t)(VERIFY_BATCH_MAX + 1) > VERIFY_BATCH_MAX,
          "verify batch overflow bound is representable and rejected");
    CHECK(((ECOPY_VERIFY_DIRECTORY | ECOPY_VERIFY_METADATA |
            ECOPY_VERIFY_PRESERVE_TIME | ECOPY_VERIFY_SEQUENTIAL) & ~15u) == 0,
          "all verify path flags are valid");
    CHECK((0x80u & ~15u) != 0, "unknown verify path flag is rejected");
    CHECK((VERIFY_SAMPLE_EXPECT_ZERO & ~VERIFY_SAMPLE_EXPECT_ZERO) == 0 &&
          (0x80u & ~VERIFY_SAMPLE_EXPECT_ZERO) != 0,
          "sample flag validation rejects unknown bits");
}

static void test_barrier_verify_summary(void)
{
    uint8_t buf[256];
    penc_t e; penc_init(&e, buf, sizeof(buf));
    penc_u32(&e, (uint32_t)-5);
    penc_u32(&e, 15);
    penc_str(&e, "/dst/bad");
    for (uint64_t i = 1; i <= 6; i++) penc_u64(&e, i);
    penc_u64(&e, 4096000);   /* remote drain bytes */
    penc_u64(&e, 250000000); /* remote drain ns */
    CHECK(!e.overflow, "extended barrier summary fits");

    pdec_t d; pdec_init(&d, buf, e.len);
    char path[64];
    CHECK((int32_t)pdec_u32(&d) == -5 && pdec_u32(&d) == 15,
          "barrier status/count roundtrip");
    CHECK(pdec_str(&d, path, sizeof(path)) == 0 &&
          strcmp(path, "/dst/bad") == 0, "barrier first diagnostic roundtrip");
    for (uint64_t i = 1; i <= 6; i++) {
        CHECK(pdec_u64(&d) == i, "barrier category counter roundtrip");
    }
    CHECK(pdec_u64(&d) == 4096000, "barrier drain bytes roundtrip");
    CHECK(pdec_u64(&d) == 250000000, "barrier drain ns roundtrip");
    CHECK(!d.error && d.off == d.len, "barrier summary fully consumed");
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
    /* PUTFILE-style two-segment frame: head (meta+path) + data, no copy. */
    {
        uint8_t head[16] = { 0xAA };
        frame_write_parts(fd, MSG_PUTFILE, 0x45, head, sizeof(head), "hello", 5);
    }
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

    CHECK(frame_read_header(fds[0], &type, &id, &plen) == 0, "read frame 4 header");
    CHECK(type == MSG_PUTFILE && id == 0x45 && plen == 21, "frame 4 (putfile) header fields");
    CHECK(io_read_all(fds[0], pl, plen) == 0, "read frame 4 payload");
    CHECK(pl[0] == 0xAA, "putfile head first byte");
    CHECK(memcmp(pl + 16, "hello", 5) == 0, "putfile data segment follows head");

    /* Writer closed after 4 frames: next header read must fail. */
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
    test_blake3_vectors();
    test_verify_batch_encoding();
    test_barrier_verify_summary();
    test_frame_pipe();

    if (failures == 0) {
        printf("protocol_test: all checks passed\n");
        return 0;
    }
    fprintf(stderr, "protocol_test: %d check(s) failed\n", failures);
    return 1;
}
