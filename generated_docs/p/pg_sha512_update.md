# pg_sha512_update

## Location
[src/common/sha2.c:802-854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L802-L854)

## Overview
Incrementally processes input data for SHA-512 hash computation, handling arbitrary-length data by buffering partial blocks.

## Definition

```c
void
pg_sha512_update(pg_sha512_ctx *context, const uint8 *data, size_t len)
```
## Detailed Description
The  function processes input data of any length for SHA-512 hashing by:

1. **Buffer Management**: Uses the context's internal 128-byte buffer to handle data that doesn't align with block boundaries
2. **Partial Block Handling**: If there's existing data in the buffer:
   - Fills the buffer to complete a 128-byte block if possible
   - Processes the complete block via SHA512_Transform
   - Continues with remaining data
3. **Complete Block Processing**: Processes as many complete 128-byte blocks as possible directly from the input data
4. **Leftover Data**: Stores any remaining bytes (< 128 bytes) in the context buffer for the next update call
5. **Bit Count Tracking**: Maintains a 128-bit counter of total bits processed using the ADDINC128 macro

This streaming approach allows hashing of data larger than memory by processing it incrementally in multiple update calls.

## Parameters / Member Variables
- `*context`: Pointer to the SHA-512 context structure containing state and buffer
- `*data`: Pointer to input data to be processed (can be NULL if len is 0)
- `len`: Number of bytes to process from the data buffer
## Dependencies
- Functions called/Symbols referenced:
  - [SHA512_Transform](../S/SHA512_Transform.md)
  - [ADDINC128](../A/ADDINC128.md)
  - memcpy
- Constants used:
  - PG_SHA512_BLOCK_LENGTH (128 bytes)
- Called from (representative examples):
  - [pg_cryptohash_update](pg_cryptohash_update.md)
  - [pg_sha384_update](pg_sha384_update.md)

## Notes and Other Information
- Safe to call with len=0 (no-op)
- Can be called multiple times to process large amounts of data incrementally
- Maintains bit count as a 128-bit integer to handle messages up to 2^128-1 bits in length
- Efficiently processes complete blocks directly without unnecessary copying
- Buffer management ensures no data is lost between calls
- Part of PostgreSQL's streaming hash interface supporting both small and very large inputs
- Must be followed by pg_sha512_final() to complete the hash and obtain the digest

## Simplified Source

```c
void pg_sha512_update(pg_sha512_ctx *context, const uint8 *data, size_t len) {
    size_t freespace, usedspace;

    // Early return for empty input
    if (len == 0)
        return;

    // Check if there's data already in the buffer
    usedspace = (context->bitcount[0] >> 3) % PG_SHA512_BLOCK_LENGTH;

    if (usedspace > 0) {
        // Fill existing buffer if possible
        freespace = PG_SHA512_BLOCK_LENGTH - usedspace;

        if (len >= freespace) {
            // Complete the buffer and process it
            memcpy(&context->buffer[usedspace], data, freespace);
            ADDINC128(context->bitcount, freespace << 3);
            len -= freespace;
            data += freespace;
            SHA512_Transform(context, context->buffer);
        } else {
            // Just store the data, not enough for a full block
            memcpy(&context->buffer[usedspace], data, len);
            ADDINC128(context->bitcount, len << 3);
            return;
        }
    }

    // Process complete blocks directly from input
    while (len >= PG_SHA512_BLOCK_LENGTH) {
        SHA512_Transform(context, data);
        ADDINC128(context->bitcount, PG_SHA512_BLOCK_LENGTH << 3);
        len -= PG_SHA512_BLOCK_LENGTH;
        data += PG_SHA512_BLOCK_LENGTH;
    }

    // Store any leftover data for next call
    if (len > 0) {
        memcpy(context->buffer, data, len);
        ADDINC128(context->bitcount, len << 3);
    }
}
```