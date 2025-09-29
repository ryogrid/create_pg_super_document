# md5_calc

## Location
[src/common/md5.c:154-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5.c#L154-L309)

## Overview
Performs the core MD5 algorithm computation on a single 64-byte block of data, implementing the four rounds of MD5 transformation operations.

## Definition
```c
static void md5_calc(const uint8 *b64, pg_md5_ctx *ctx)
```

## Detailed Description
The `md5_calc` function is the heart of the MD5 hashing algorithm implementation in PostgreSQL. It processes a single 512-bit (64-byte) block of input data through the MD5 algorithm's four transformation rounds (16 operations each). The function updates the MD5 context state variables (A, B, C, D) by applying the MD5 mathematical operations defined in RFC 1321.

The implementation handles endianness differences: on little-endian systems it directly uses the input bytes as 32-bit words, while on big-endian systems it performs byte swapping to ensure correct word ordering. After processing all four rounds (64 total operations), it adds the computed values back to the context state to maintain the running hash.

## Parameters / Member Variables
- `b64`: Pointer to a 64-byte block of input data to be processed
- `ctx`: Pointer to the MD5 context structure containing the current hash state (md5_sta, md5_stb, md5_stc, md5_std)

## Dependencies
- Functions called/Symbols referenced:
  - ROUND1 (macro for MD5 round 1 operations)
  - ROUND2 (macro for MD5 round 2 operations)
  - ROUND3 (macro for MD5 round 3 operations)
  - ROUND4 (macro for MD5 round 4 operations)
  - Sa, Sb, Sc, Sd (shift constants for round 1)
  - Se, Sf, Sg, Sh (shift constants for round 2)
  - Si, Sj, Sk, Sl (shift constants for round 3)
  - Sm, Sn, So, Sp (shift constants for round 4)
  - [pg_md5_ctx](../p/pg_md5_ctx.md) (MD5 context structure type)
- Called from (representative examples):
  - [md5_pad](md5_pad.md)
  - [pg_md5_update](../p/pg_md5_update.md)

## Notes and Other Information
- This function is static and only used internally within the MD5 implementation
- The function implements the exact MD5 algorithm as specified in RFC 1321
- Big-endian systems require explicit byte swapping due to MD5's little-endian word processing requirements
- Each round applies 16 operations with specific mathematical transformations and rotation amounts
- The function maintains the cumulative hash state by adding the round results to the existing context values

## Simplified Source

```c
static void
md5_calc(const uint8 *b64, pg_md5_ctx *ctx)
{
    // Load current hash state into working variables
    uint32 A = ctx->md5_sta;
    uint32 B = ctx->md5_stb;
    uint32 C = ctx->md5_stc;
    uint32 D = ctx->md5_std;

    // Handle endianness - convert 64 bytes to 16 32-bit words
#ifndef WORDS_BIGENDIAN
    const uint32 *X = (const uint32 *) b64;  // Little endian: direct cast
#else
    uint32 X[16];  // Big endian: need byte swapping
    uint8 *y = (uint8 *) X;
    // Swap bytes for each 32-bit word (code simplified for readability)
    for (int i = 0; i < 16; i++) {
        y[i*4+0] = b64[i*4+3];
        y[i*4+1] = b64[i*4+2];
        y[i*4+2] = b64[i*4+1];
        y[i*4+3] = b64[i*4+0];
    }
#endif

    // MD5 Algorithm: 4 rounds of 16 operations each
    // Round 1: F(B,C,D) = (B & C) | (~B & D)
    for (int i = 0; i < 16; i++) {
        ROUND1(A, B, C, D, i, shift_amounts[i], i+1);
        // Rotate variables: A->D, B->A, C->B, D->C
    }

    // Round 2: G(B,C,D) = (B & D) | (C & ~D)
    for (int i = 0; i < 16; i++) {
        ROUND2(A, B, C, D, message_schedule[i], shift_amounts[i], i+17);
        // Rotate variables
    }

    // Round 3: H(B,C,D) = B ^ C ^ D
    for (int i = 0; i < 16; i++) {
        ROUND3(A, B, C, D, message_schedule[i], shift_amounts[i], i+33);
        // Rotate variables
    }

    // Round 4: I(B,C,D) = C ^ (B | ~D)
    for (int i = 0; i < 16; i++) {
        ROUND4(A, B, C, D, message_schedule[i], shift_amounts[i], i+49);
        // Rotate variables
    }

    // Add computed values back to context state
    ctx->md5_sta += A;
    ctx->md5_stb += B;
    ctx->md5_stc += C;
    ctx->md5_std += D;
}
```