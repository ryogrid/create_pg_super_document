# SHA512_Last

## Location
[src/common/sha2.c:855-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L855-L904)

## Overview
Completes SHA-512 hash computation by applying message padding and processing the final block(s) according to the SHA-512 specification.

## Definition

```c
static void
SHA512_Last(pg_sha512_ctx *context)
```
## Detailed Description
The  function implements the SHA-512 message padding and finalization process as specified in FIPS 180-4:

1. **Bit Count Conversion**: Converts the 128-bit message length counter from host byte order to big-endian format for consistent cross-platform results
2. **Message Padding**: Applies SHA-512 padding rules:
   - Appends a single '1' bit (0x80 byte) immediately after the message
   - Pads with zeros to achieve proper block alignment
   - Reserves space for the 128-bit message length at the end
3. **Block Processing Logic**:
   - If current data fits in one block (≤ 112 bytes): pads and processes single final block
   - If current data requires two blocks (> 112 bytes): processes current block, then a second padded block
   - For empty buffer: creates a new block starting with the padding bit
4. **Length Encoding**: Encodes the total message length in bits as a 128-bit big-endian integer in the final 16 bytes
5. **Final Transform**: Processes the final padded block through SHA512_Transform to complete the hash computation

The function ensures that the message length modulo 1024 bits equals 896 bits, leaving exactly 128 bits for the length field.

## Parameters / Member Variables
- `*context`: Pointer to SHA-512 context containing the current state, buffer, and bit count
## Dependencies
- Functions called/Symbols referenced:
  - [SHA512_Transform](SHA512_Transform.md)
  - REVERSE64
  - memset
- Constants used:
  - PG_SHA512_BLOCK_LENGTH (128 bytes)
  - PG_SHA512_SHORT_BLOCK_LENGTH (112 bytes)
- Called from (representative examples):
  - [pg_sha512_final](../p/pg_sha512_final.md)
  - [pg_sha384_final](../p/pg_sha384_final.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside the sha2.c module
- Implements the standard SHA-512 padding scheme: message + '1' bit + zeros + 128-bit length
- Handles both single-block and two-block padding scenarios automatically
- Length encoding is always in big-endian format regardless of host architecture
- After this function completes, the context->state array contains the final SHA-512 hash value
- Must be called exactly once per hash computation, typically by pg_sha512_final()
- The 128-bit length counter allows for messages up to 2^128-1 bits (2^125-1 bytes) in length

## Simplified Source

```c
static void
SHA512_Last(pg_sha512_ctx *context)
{
    unsigned int usedspace;

    // Calculate bytes used in current 128-byte block
    usedspace = (context->bitcount[0] >> 3) % PG_SHA512_BLOCK_LENGTH;

    // Convert 128-bit length counter to big-endian format
#ifndef WORDS_BIGENDIAN
    REVERSE64(context->bitcount[0], context->bitcount[0]);
    REVERSE64(context->bitcount[1], context->bitcount[1]);
#endif

    if (usedspace > 0)
    {
        // Add mandatory '1' bit (0x80)
        context->buffer[usedspace++] = 0x80;

        if (usedspace <= PG_SHA512_SHORT_BLOCK_LENGTH)
        {
            // Padding fits in current block (≤ 112 bytes)
            memset(&context->buffer[usedspace], 0, PG_SHA512_SHORT_BLOCK_LENGTH - usedspace);
        }
        else
        {
            // Need additional block for padding (> 112 bytes)
            if (usedspace < PG_SHA512_BLOCK_LENGTH)
            {
                memset(&context->buffer[usedspace], 0, PG_SHA512_BLOCK_LENGTH - usedspace);
            }
            // Process current block
            SHA512_Transform(context, context->buffer);

            // Prepare new block for 128-bit length field
            memset(context->buffer, 0, PG_SHA512_BLOCK_LENGTH - 2);
        }
    }
    else
    {
        // Empty block - start with zero padding
        memset(context->buffer, 0, PG_SHA512_SHORT_BLOCK_LENGTH);
        // Add mandatory '1' bit at beginning
        *context->buffer = 0x80;
    }

    // Append 128-bit message length (high 64 bits, then low 64 bits)
    *(uint64 *) &context->buffer[PG_SHA512_SHORT_BLOCK_LENGTH] = context->bitcount[1];
    *(uint64 *) &context->buffer[PG_SHA512_SHORT_BLOCK_LENGTH + 8] = context->bitcount[0];

    // Process final block
    SHA512_Transform(context, context->buffer);
}
```