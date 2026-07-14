/*
 * ecopy compatibility helpers around the official BLAKE3 C API.
 * Upstream sources are pinned at BLAKE3 1.8.2.
 */
#include "blake3.h"

void blake3_hash(const void *data, size_t len, uint8_t out[BLAKE3_OUT_LEN])
{
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, out, BLAKE3_OUT_LEN);
}

const char *blake3_backend(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx512f") &&
        __builtin_cpu_supports("avx512vl")) return "AVX-512";
    if (__builtin_cpu_supports("avx2")) return "AVX2";
    if (__builtin_cpu_supports("sse4.1")) return "SSE4.1";
    if (__builtin_cpu_supports("sse2")) return "SSE2";
#endif
    return "portable";
}
