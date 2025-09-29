# sha1_step

## Location
[src/common/sha1.c:90-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha1.c#L90-L232)

## Overview
Performs the core SHA-1 compression function, processing a single 512-bit (64-byte) block of data through 80 rounds of operations according to the SHA-1 algorithm specification.

## Definition

```c
static void
sha1_step(pg_sha1_ctx *ctx)
```
## Detailed Description
The  function implements the heart of the SHA-1 algorithm by processing one message block (512 bits) through four rounds of 20 operations each. It performs endianness conversion for little-endian systems, then executes the SHA-1 compression function using five working variables (a, b, c, d, e) and the message schedule array W[].

The function operates in four distinct phases:
1. **Rounds 0-19**: Uses function F0 and constant K0
2. **Rounds 20-39**: Uses function F1 and constant K1  
3. **Rounds 40-59**: Uses function F2 and constant K2
4. **Rounds 60-79**: Uses function F3 and constant K3

Each round updates the five working variables through rotation, addition, and logical operations. The message schedule W[] is expanded from the initial 16 32-bit words to 80 words using XOR and rotation operations for rounds 16-79.

## Parameters / Member Variables
- : Pointer to the SHA-1 context structure containing:
  - : 64-byte message block buffer (byte access)
  - : Same buffer accessed as 32-bit words
  - : Five 32-bit hash state variables (H0-H4)

## Dependencies
- Functions called/Symbols referenced:
  - : For endianness conversion on little-endian systems
  - : To clear the message buffer after processing
  - F0, F1, F2, F3: SHA-1 logical function macros
  - K: Constant table macro for round constants
  - S: Left rotation macro
  - H: Hash state access macro
  - W: Message word access macro

- Called from:
  -  macro: When padding bytes and block boundary reached
  - : During final message padding
  - : When processing complete 64-byte blocks

## Notes and Other Information
- This is a static function, only accessible within the sha1.c compilation unit
- Handles endianness conversion automatically for little-endian systems through explicit byte swapping
- Clears the message buffer after processing for security
- Implements the SHA-1 specification from FIPS PUB 180-1
- Each call processes exactly one 512-bit block and updates the hash state
- The function modifies the context's hash state (H0-H4) in place

## Simplified Source

```c
static void
sha1_step(pg_sha1_ctx *ctx)
{
    uint32 a, b, c, d, e;
    size_t t, s;
    uint32 tmp;

#ifndef WORDS_BIGENDIAN
    // Little-endian systems: swap bytes for each 32-bit word
    pg_sha1_ctx tctx;
    memmove(&tctx.m.b8[0], &ctx->m.b8[0], 64);
    // Reverse byte order for all 16 words (simplified representation)
    for (int i = 0; i < 16; i++) {
        for (int j = 0; j < 4; j++) {
            ctx->m.b8[i*4 + j] = tctx.m.b8[i*4 + (3-j)];
        }
    }
#endif

    // Initialize working variables with current hash state
    a = H(0); b = H(1); c = H(2); d = H(3); e = H(4);

    // SHA-1 algorithm: 80 rounds in 4 groups of 20

    // Rounds 0-19: F0 function
    for (t = 0; t < 20; t++) {
        s = t & 0x0f;
        if (t >= 16) {
            // Extend message schedule
            W(s) = S(1, W((s + 13) & 0x0f) ^ W((s + 8) & 0x0f) ^
                      W((s + 2) & 0x0f) ^ W(s));
        }
        tmp = S(5, a) + F0(b, c, d) + e + W(s) + K(t);
        // Rotate variables: e=d, d=c, c=rotate(b,30), b=a, a=tmp
        e = d; d = c; c = S(30, b); b = a; a = tmp;
    }

    // Rounds 20-39: F1 function
    for (t = 20; t < 40; t++) {
        s = t & 0x0f;
        W(s) = S(1, W((s + 13) & 0x0f) ^ W((s + 8) & 0x0f) ^
                  W((s + 2) & 0x0f) ^ W(s));
        tmp = S(5, a) + F1(b, c, d) + e + W(s) + K(t);
        e = d; d = c; c = S(30, b); b = a; a = tmp;
    }

    // Rounds 40-59: F2 function
    for (t = 40; t < 60; t++) {
        s = t & 0x0f;
        W(s) = S(1, W((s + 13) & 0x0f) ^ W((s + 8) & 0x0f) ^
                  W((s + 2) & 0x0f) ^ W(s));
        tmp = S(5, a) + F2(b, c, d) + e + W(s) + K(t);
        e = d; d = c; c = S(30, b); b = a; a = tmp;
    }

    // Rounds 60-79: F3 function
    for (t = 60; t < 80; t++) {
        s = t & 0x0f;
        W(s) = S(1, W((s + 13) & 0x0f) ^ W((s + 8) & 0x0f) ^
                  W((s + 2) & 0x0f) ^ W(s));
        tmp = S(5, a) + F3(b, c, d) + e + W(s) + K(t);
        e = d; d = c; c = S(30, b); b = a; a = tmp;
    }

    // Add results back to hash state
    H(0) = H(0) + a;
    H(1) = H(1) + b;
    H(2) = H(2) + c;
    H(3) = H(3) + d;
    H(4) = H(4) + e;

    // Clear message buffer for security
    memset(&ctx->m.b8[0], 0, 64);
}
```