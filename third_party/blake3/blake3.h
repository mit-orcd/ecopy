/*
 * Minimal portable BLAKE3 one-shot interface.
 *
 * This implementation follows the BLAKE3 specification and is intentionally
 * limited to the unkeyed 32-byte digest used by ecopy's 4 KiB verification
 * blocks. BLAKE3 is CC0 / public domain.
 */
#ifndef ECOPY_BLAKE3_H
#define ECOPY_BLAKE3_H

#include <stddef.h>
#include <stdint.h>

#define BLAKE3_OUT_LEN 32

void blake3_hash(const void *input, size_t input_len,
                 uint8_t out[BLAKE3_OUT_LEN]);

#endif
