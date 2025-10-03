# SHA512_Transform

## Location
[src/common/sha2.c:644-712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L644-L712)

## Overview
Performs the core SHA-512 compression function on a single 128-byte block of data, updating the hash context state.

## Definition

```c
static void
SHA512_Transform(pg_sha512_ctx *context, const uint8 *data)
```
## Detailed Description
The  function implements the SHA-512 compression algorithm as specified in FIPS 180-4. It processes exactly one 128-byte block of input data through 80 rounds of cryptographic operations:

1. **Initialization**: Loads the current hash state (8 64-bit words) into working variables a through h
2. **Message Schedule**: Processes the input data in two phases:
   - Rounds 0-15: Directly converts input bytes to 64-bit words (big-endian) and applies compression
   - Rounds 16-79: Generates additional words using message schedule expansion with sigma functions
3. **Compression**: Each round applies the SHA-512 compression function using:
   - Majority and Choice logical functions
   - Sigma rotation/shift functions  
   - Round constants from the K512 table
   - Modular addition operations
4. **State Update**: Adds the working variables back to the context state
5. **Cleanup**: Zeros all temporary variables for security

The function uses unrolled loops and optimized macros (ROUND512_0_TO_15, ROUND512) for performance.

## Parameters / Member Variables
- `*context`: Pointer to SHA-512 context containing the current hash state and working buffer
- `*data`: Pointer to exactly 128 bytes of input data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - ROUND512_0_TO_15 (macro for rounds 0-15)
  - ROUND512 (macro for rounds 16-79)
  - Sigma0_512, Sigma1_512, sigma0_512, sigma1_512 (SHA-512 logical functions)
  - Ch, Maj (Choice and Majority functions)
  - K512 (round constants array)
- Called from (representative examples):
  - [pg_sha512_update](../p/pg_sha512_update.md)
  - [SHA512_Last](SHA512_Last.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside the sha2.c module
- Processes exactly one 128-byte block - callers must handle message padding and length encoding
- Uses big-endian byte order for input data conversion regardless of host architecture
- Implements the standard SHA-512 80-round compression function with message schedule expansion
- All temporary variables are explicitly zeroed after use for security
- Part of the critical path for SHA-512 performance - heavily optimized with unrolled loops and macros

## Simplified Source

```c
static void
SHA512_Transform(pg_sha512_ctx *context, const uint8 *data)
{
    uint64 a, b, c, d, e, f, g, h;
    uint64 s0, s1, T1;
    uint64 *W512 = (uint64 *) context->buffer;
    int j;

    // Initialize working variables with current hash state
    a = context->state[0];
    b = context->state[1];
    c = context->state[2];
    d = context->state[3];
    e = context->state[4];
    f = context->state[5];
    g = context->state[6];
    h = context->state[7];

    j = 0;

    // Rounds 0-15: Process input data directly
    do {
        // Unrolled loop: 8 rounds per iteration
        ROUND512_0_TO_15(a, b, c, d, e, f, g, h);
        ROUND512_0_TO_15(h, a, b, c, d, e, f, g);
        ROUND512_0_TO_15(g, h, a, b, c, d, e, f);
        ROUND512_0_TO_15(f, g, h, a, b, c, d, e);
        ROUND512_0_TO_15(e, f, g, h, a, b, c, d);
        ROUND512_0_TO_15(d, e, f, g, h, a, b, c);
        ROUND512_0_TO_15(c, d, e, f, g, h, a, b);
        ROUND512_0_TO_15(b, c, d, e, f, g, h, a);
    } while (j < 16);

    // Rounds 16-79: Use extended message schedule
    do {
        // Unrolled loop: 8 rounds per iteration
        ROUND512(a, b, c, d, e, f, g, h);
        ROUND512(h, a, b, c, d, e, f, g);
        ROUND512(g, h, a, b, c, d, e, f);
        ROUND512(f, g, h, a, b, c, d, e);
        ROUND512(e, f, g, h, a, b, c, d);
        ROUND512(d, e, f, g, h, a, b, c);
        ROUND512(c, d, e, f, g, h, a, b);
        ROUND512(b, c, d, e, f, g, h, a);
    } while (j < 80);

    // Add working variables back to hash state
    context->state[0] += a;
    context->state[1] += b;
    context->state[2] += c;
    context->state[3] += d;
    context->state[4] += e;
    context->state[5] += f;
    context->state[6] += g;
    context->state[7] += h;

    // Clear working variables for security
    a = b = c = d = e = f = g = h = T1 = 0;
}
```