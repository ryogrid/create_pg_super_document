# sha1_result

## Location
[src/common/sha1.c:276-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha1.c#L276-L315)

## Overview
Extracts the final 20-byte SHA-1 hash digest from the context's hash state, handling endianness conversion to produce the standard big-endian output format.

## Definition

```c
static void
sha1_result(uint8 *digest0, pg_sha1_ctx *ctx)
```
## Detailed Description
The  function copies the computed SHA-1 hash value from the internal context structure to the output buffer. Since SHA-1 produces a 160-bit (20-byte) hash, the function transfers exactly 20 bytes from the context's hash state array to the destination buffer.

The function handles endianness conversion to ensure the output digest conforms to the SHA-1 standard big-endian format:
- **Big-endian systems**: Direct memory copy from hash state to output
- **Little-endian systems**: Byte-wise reversal of each 32-bit hash word to convert from internal little-endian representation to standard big-endian output

This ensures the SHA-1 digest is identical across different architectures and matches standard test vectors.

## Parameters / Member Variables
- : Pointer to the output buffer where the 20-byte SHA-1 digest will be stored
- : Pointer to the SHA-1 context structure containing:
  - : Hash state array accessed as individual bytes for endianness handling

## Dependencies
- Functions called/Symbols referenced:
  - : Used on big-endian systems for direct hash state copy
  - : Context structure type containing hash state

- Called from:
  - : After message padding is complete, to extract the final hash

## Notes and Other Information
- This is a static function, only accessible within the sha1.c compilation unit
- Produces exactly 20 bytes of output (160-bit SHA-1 hash)
- Output format is always big-endian regardless of host architecture
- The function assumes the hash computation is complete and the context contains valid hash state
- Does not modify the context structure, only reads the hash state
- The output buffer must be pre-allocated with at least 20 bytes of space
- Critical for ensuring SHA-1 compatibility across different hardware platforms

## Simplified Source

```c
static void
sha1_result(uint8 *digest0, pg_sha1_ctx *ctx)
{
    uint8 *digest;

    digest = (uint8 *) digest0;

    // Extract 20-byte SHA-1 digest with proper endianness
#ifdef WORDS_BIGENDIAN
    // Big-endian: direct copy of 20 bytes
    memmove(digest, &ctx->h.b8[0], 20);
#else
    // Little-endian: reverse each 32-bit word for big-endian output
    // Word 1 (H0)
    digest[0] = ctx->h.b8[3];
    digest[1] = ctx->h.b8[2];
    digest[2] = ctx->h.b8[1];
    digest[3] = ctx->h.b8[0];
    // Word 2 (H1)
    digest[4] = ctx->h.b8[7];
    digest[5] = ctx->h.b8[6];
    digest[6] = ctx->h.b8[5];
    digest[7] = ctx->h.b8[4];
    // Word 3 (H2)
    digest[8] = ctx->h.b8[11];
    digest[9] = ctx->h.b8[10];
    digest[10] = ctx->h.b8[9];
    digest[11] = ctx->h.b8[8];
    // Word 4 (H3)
    digest[12] = ctx->h.b8[15];
    digest[13] = ctx->h.b8[14];
    digest[14] = ctx->h.b8[13];
    digest[15] = ctx->h.b8[12];
    // Word 5 (H4)
    digest[16] = ctx->h.b8[19];
    digest[17] = ctx->h.b8[18];
    digest[18] = ctx->h.b8[17];
    digest[19] = ctx->h.b8[16];
#endif
}
```