/*
 * Portable, unkeyed BLAKE3 one-shot implementation.
 *
 * Derived directly from the BLAKE3 specification. The algorithm and reference
 * implementation are CC0 / public domain: https://github.com/BLAKE3-team/BLAKE3
 */
#include "blake3.h"

#include <string.h>

enum {
    CHUNK_START = 1,
    CHUNK_END = 2,
    PARENT = 4,
    ROOT = 8
};

static const uint32_t iv[8] = {
    0x6A09E667u, 0xBB67AE85u, 0x3C6EF372u, 0xA54FF53Au,
    0x510E527Fu, 0x9B05688Cu, 0x1F83D9ABu, 0x5BE0CD19u
};

static const uint8_t schedule[7][16] = {
    { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15 },
    { 2, 6, 3,10, 7, 0, 4,13, 1,11,12, 5, 9,14,15, 8 },
    { 3, 4,10,12,13, 2, 7,14, 6, 5, 9, 0,11,15, 8, 1 },
    {10, 7,12, 9,14, 3,13,15, 4, 0,11, 2, 5, 8, 1, 6 },
    {12,13, 9,11,15,10,14, 8, 7, 2, 5, 3, 0, 1, 6, 4 },
    { 9,14,11, 5, 8,12,15, 1,13, 3, 0,10, 2, 6, 4, 7 },
    {11,15, 5, 0, 1, 9, 8, 6,14,10, 2,12, 3, 4, 7,13 }
};

typedef struct {
    uint32_t cv[8];
    uint32_t block[16];
    uint64_t counter;
    uint32_t block_len;
    uint32_t flags;
} output_t;

static uint32_t rotr32(uint32_t x, unsigned n)
{
    return (x >> n) | (x << (32 - n));
}

static uint32_t load32(const uint8_t *p)
{
    return (uint32_t)p[0] |
           ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static void store32(uint8_t *p, uint32_t x)
{
    p[0] = (uint8_t)x;
    p[1] = (uint8_t)(x >> 8);
    p[2] = (uint8_t)(x >> 16);
    p[3] = (uint8_t)(x >> 24);
}

static void g(uint32_t s[16], int a, int b, int c, int d,
              uint32_t x, uint32_t y)
{
    s[a] = s[a] + s[b] + x;
    s[d] = rotr32(s[d] ^ s[a], 16);
    s[c] += s[d];
    s[b] = rotr32(s[b] ^ s[c], 12);
    s[a] = s[a] + s[b] + y;
    s[d] = rotr32(s[d] ^ s[a], 8);
    s[c] += s[d];
    s[b] = rotr32(s[b] ^ s[c], 7);
}

static void compress(const uint32_t cv[8], const uint32_t block[16],
                     uint64_t counter, uint32_t block_len, uint32_t flags,
                     uint32_t out[16])
{
    uint32_t s[16];
    memcpy(s, cv, 8 * sizeof(uint32_t));
    memcpy(s + 8, iv, 4 * sizeof(uint32_t));
    s[12] = (uint32_t)counter;
    s[13] = (uint32_t)(counter >> 32);
    s[14] = block_len;
    s[15] = flags;

    for (int r = 0; r < 7; r++) {
        const uint8_t *m = schedule[r];
        g(s, 0, 4, 8,12, block[m[0]], block[m[1]]);
        g(s, 1, 5, 9,13, block[m[2]], block[m[3]]);
        g(s, 2, 6,10,14, block[m[4]], block[m[5]]);
        g(s, 3, 7,11,15, block[m[6]], block[m[7]]);
        g(s, 0, 5,10,15, block[m[8]], block[m[9]]);
        g(s, 1, 6,11,12, block[m[10]], block[m[11]]);
        g(s, 2, 7, 8,13, block[m[12]], block[m[13]]);
        g(s, 3, 4, 9,14, block[m[14]], block[m[15]]);
    }
    for (int i = 0; i < 8; i++) {
        out[i] = s[i] ^ s[i + 8];
        out[i + 8] = s[i + 8] ^ cv[i];
    }
}

static void output_cv(const output_t *o, uint32_t cv[8])
{
    uint32_t words[16];
    compress(o->cv, o->block, o->counter, o->block_len, o->flags, words);
    memcpy(cv, words, 8 * sizeof(uint32_t));
}

static output_t parent_output(const uint32_t left[8], const uint32_t right[8])
{
    output_t o;
    memcpy(o.cv, iv, sizeof(o.cv));
    memcpy(o.block, left, 8 * sizeof(uint32_t));
    memcpy(o.block + 8, right, 8 * sizeof(uint32_t));
    o.counter = 0;
    o.block_len = 64;
    o.flags = PARENT;
    return o;
}

static output_t chunk_output(const uint8_t *input, size_t len, uint64_t counter)
{
    output_t o;
    uint32_t cv[8];
    size_t blocks = len ? (len + 63) / 64 : 1;
    memcpy(cv, iv, sizeof(cv));

    for (size_t b = 0; b < blocks; b++) {
        uint8_t bytes[64] = {0};
        size_t off = b * 64;
        size_t n = off < len ? len - off : 0;
        if (n > 64) n = 64;
        if (n) memcpy(bytes, input + off, n);

        memcpy(o.cv, cv, sizeof(o.cv));
        for (int i = 0; i < 16; i++) o.block[i] = load32(bytes + i * 4);
        o.counter = counter;
        o.block_len = (uint32_t)n;
        o.flags = (b == 0 ? CHUNK_START : 0) |
                  (b + 1 == blocks ? CHUNK_END : 0);
        if (b + 1 != blocks) output_cv(&o, cv);
    }
    return o;
}

void blake3_hash(const void *input, size_t input_len,
                 uint8_t out[BLAKE3_OUT_LEN])
{
    const uint8_t *p = (const uint8_t *)input;
    uint32_t stack[64][8];
    size_t stack_len = 0;
    uint64_t chunks = input_len ? (input_len + 1023) / 1024 : 1;
    output_t current = {0};

    for (uint64_t chunk = 0; chunk < chunks; chunk++) {
        size_t off = (size_t)chunk * 1024;
        size_t n = off < input_len ? input_len - off : 0;
        if (n > 1024) n = 1024;
        current = chunk_output(p + off, n, chunk);

        if (chunk + 1 != chunks) {
            uint32_t cv[8];
            uint64_t total = chunk + 1;
            output_cv(&current, cv);
            while ((total & 1) == 0) {
                current = parent_output(stack[--stack_len], cv);
                output_cv(&current, cv);
                total >>= 1;
            }
            memcpy(stack[stack_len++], cv, sizeof(cv));
        }
    }

    while (stack_len) {
        uint32_t right[8];
        output_cv(&current, right);
        current = parent_output(stack[--stack_len], right);
    }

    uint32_t words[16];
    compress(current.cv, current.block, 0, current.block_len,
             current.flags | ROOT, words);
    for (int i = 0; i < 8; i++) store32(out + i * 4, words[i]);
}
