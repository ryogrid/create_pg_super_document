# sha1_pad

## Location
[src/common/sha1.c:233-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha1.c#L233-L275)

## Overview
Applies the required padding to the SHA-1 message according to the algorithm specification, ensuring the final message length is congruent to 448 modulo 512 bits before appending the 64-bit message length.

## Definition

```c
static void
sha1_pad(pg_sha1_ctx *ctx)
```
## Detailed Description
The  function implements the SHA-1 padding scheme as specified in FIPS PUB 180-1. It performs the final steps of message preparation before computing the hash digest:

1. **Initial padding**: Appends a single '1' bit (0x80 byte) to the message
2. **Zero padding**: Adds zero bytes to make room for the 64-bit length field
3. **Length appending**: Adds the original message length in bits as a 64-bit big-endian integer

The padding ensures the total message length becomes a multiple of 512 bits (64 bytes). If there isn't enough space in the current block for both padding and the 8-byte length field, it fills the current block with zeros, processes it, and continues padding in a new block.

The function handles endianness by appending the message length bytes in the correct order depending on the target architecture.

## Parameters / Member Variables
- : Pointer to the SHA-1 context structure containing:
  - : Message block buffer for padding operations
  - : 64-bit message length counter accessed as bytes
  - Message count and position tracking fields

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to append padding bytes and trigger block processing
  - : Macro for current position in message block
  - : Called when a block becomes full during padding
  - : To zero-fill padding regions

- Called from:
  - : During hash finalization to complete message processing

## Notes and Other Information
- This is a static function, only accessible within the sha1.c compilation unit  
- Handles both cases where padding fits in current block or requires an additional block
- Properly handles endianness for the 64-bit length field (big-endian in the hash)
- The padding scheme is critical for security - ensures no two different messages produce the same padded input
- After padding, the message length is always a multiple of 512 bits
- The 8-byte length field represents the original message length in bits, not the padded length

## Simplified Source

```c
static void
sha1_pad(pg_sha1_ctx *ctx)
{
    size_t padlen;      // pad length in bytes
    size_t padstart;

    // Step 1: Append mandatory '1' bit (0x80)
    PUTPAD(0x80);

    // Step 2: Calculate padding needed to leave 8 bytes for length
    padstart = COUNT % 64;
    padlen = 64 - padstart;

    if (padlen < 8)
    {
        // Not enough space in current block - fill and process
        memset(&ctx->m.b8[padstart], 0, padlen);
        COUNT += padlen;
        COUNT %= 64;
        sha1_step(ctx);  // Process current block

        padstart = COUNT % 64;  // Should be 0
        padlen = 64 - padstart; // Should be 64
    }

    // Fill with zeros, leaving 8 bytes for length
    memset(&ctx->m.b8[padstart], 0, padlen - 8);
    COUNT += (padlen - 8);
    COUNT %= 64;

    // Step 3: Append 64-bit message length in big-endian format
#ifdef WORDS_BIGENDIAN
    // Big-endian system: append length bytes in order
    PUTPAD(ctx->c.b8[0]);
    PUTPAD(ctx->c.b8[1]);
    PUTPAD(ctx->c.b8[2]);
    PUTPAD(ctx->c.b8[3]);
    PUTPAD(ctx->c.b8[4]);
    PUTPAD(ctx->c.b8[5]);
    PUTPAD(ctx->c.b8[6]);
    PUTPAD(ctx->c.b8[7]);
#else
    // Little-endian system: reverse byte order for big-endian output
    PUTPAD(ctx->c.b8[7]);
    PUTPAD(ctx->c.b8[6]);
    PUTPAD(ctx->c.b8[5]);
    PUTPAD(ctx->c.b8[4]);
    PUTPAD(ctx->c.b8[3]);
    PUTPAD(ctx->c.b8[2]);
    PUTPAD(ctx->c.b8[1]);
    PUTPAD(ctx->c.b8[0]);
#endif
}
```