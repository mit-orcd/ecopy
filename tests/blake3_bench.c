#define _POSIX_C_SOURCE 200809L
#include "../third_party/blake3/blake3.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

int main(int argc, char **argv)
{
    enum { BLOCK = 4096, ITERATIONS = 250000 };
    uint8_t input[BLOCK], digest[BLAKE3_OUT_LEN];
    struct timespec start, finish;
    memset(input, 0x5a, sizeof(input));
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < ITERATIONS; i++) {
        input[i & (BLOCK - 1)] ^= (uint8_t)i;
        blake3_hash(input, sizeof(input), digest);
    }
    clock_gettime(CLOCK_MONOTONIC, &finish);
    double sec = (double)(finish.tv_sec - start.tv_sec) +
                 (double)(finish.tv_nsec - start.tv_nsec) / 1e9;
    double gib = (double)BLOCK * ITERATIONS / (1024.0 * 1024.0 * 1024.0);
    printf("%s: %.2f GiB/s (digest %02x%02x)\n",
           argc > 1 ? argv[1] : blake3_backend(), gib / sec,
           digest[0], digest[1]);
    return 0;
}
