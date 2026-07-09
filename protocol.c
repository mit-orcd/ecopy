/*
 * protocol.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Framing and blocking full read/write helpers for the ecopy SSH protocol.
 */

#define _GNU_SOURCE
#include "protocol.h"

#include <errno.h>
#include <unistd.h>

int io_write_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t done = 0;

    while (done < len) {
        ssize_t w = write(fd, p + done, len - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) {
            return -1;
        }
        done += (size_t)w;
    }
    return 0;
}

int io_read_all(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t done = 0;

    while (done < len) {
        ssize_t r = read(fd, p + done, len - done);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) {
            /* EOF before the full frame arrived. */
            return -1;
        }
        done += (size_t)r;
    }
    return 0;
}

static void encode_header(uint8_t *hdr, uint8_t type, uint64_t id, uint32_t plen)
{
    hdr[0] = type;
    for (int i = 0; i < 8; i++) hdr[1 + i] = (uint8_t)(id >> (8 * i));
    hdr[9]  = (uint8_t)plen;
    hdr[10] = (uint8_t)(plen >> 8);
    hdr[11] = (uint8_t)(plen >> 16);
    hdr[12] = (uint8_t)(plen >> 24);
}

int frame_write(int fd, uint8_t type, uint64_t id, const void *payload, uint32_t plen)
{
    uint8_t hdr[ECOPY_HDR_LEN];

    encode_header(hdr, type, id, plen);
    if (io_write_all(fd, hdr, ECOPY_HDR_LEN) != 0) {
        return -1;
    }
    if (plen && io_write_all(fd, payload, plen) != 0) {
        return -1;
    }
    return 0;
}

int frame_write_data(int fd, uint64_t id, uint64_t file_id, int64_t offset,
                     const void *data, size_t len)
{
    uint8_t hdr[ECOPY_HDR_LEN];
    uint8_t prefix[16];
    uint32_t plen = (uint32_t)(16 + len);

    encode_header(hdr, MSG_WRITE, id, plen);
    for (int i = 0; i < 8; i++) prefix[i] = (uint8_t)(file_id >> (8 * i));
    for (int i = 0; i < 8; i++) prefix[8 + i] = (uint8_t)((uint64_t)offset >> (8 * i));

    if (io_write_all(fd, hdr, ECOPY_HDR_LEN) != 0) return -1;
    if (io_write_all(fd, prefix, sizeof(prefix)) != 0) return -1;
    if (len && io_write_all(fd, data, len) != 0) return -1;
    return 0;
}

int frame_write_parts(int fd, uint8_t type, uint64_t id,
                      const void *head, uint32_t head_len,
                      const void *data, size_t data_len)
{
    uint8_t hdr[ECOPY_HDR_LEN];
    uint32_t plen = (uint32_t)(head_len + data_len);

    encode_header(hdr, type, id, plen);
    if (io_write_all(fd, hdr, ECOPY_HDR_LEN) != 0) return -1;
    if (head_len && io_write_all(fd, head, head_len) != 0) return -1;
    if (data_len && io_write_all(fd, data, data_len) != 0) return -1;
    return 0;
}

int frame_read_header(int fd, uint8_t *type, uint64_t *id, uint32_t *plen)
{
    uint8_t hdr[ECOPY_HDR_LEN];

    if (io_read_all(fd, hdr, ECOPY_HDR_LEN) != 0) {
        return -1;
    }
    *type = hdr[0];
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; i++) v |= ((uint64_t)hdr[1 + i]) << (8 * i);
        *id = v;
    }
    *plen = (uint32_t)hdr[9] | ((uint32_t)hdr[10] << 8) |
            ((uint32_t)hdr[11] << 16) | ((uint32_t)hdr[12] << 24);
    return 0;
}
