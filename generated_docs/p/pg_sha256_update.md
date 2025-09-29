# pg_sha256_update

## Location
[src/common/sha2.c:476-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L476-L528)

## Overview
Incrementally processes input data for SHA-256 hashing by managing buffering and calling the transformation function when complete blocks are available.

## Definition

```c
void
pg_sha256_update(pg_sha256_ctx *context, const uint8 *data, size_t len)
```
## Detailed Description
pg_sha256_update is the core function for feeding data into a SHA-256 hash computation. It handles the complexity of buffering input data until complete 512-bit (64-byte) blocks are available for processing. The function operates in three phases: first, it fills any partially used buffer from previous calls; second, it processes complete blocks directly from the input data; and third, it buffers any remaining incomplete block for future processing. The function maintains an accurate bit count of all processed data and efficiently handles data of any size, from single bytes to large streams. This design allows for optimal performance while maintaining the strict block-based requirements of the SHA-256 algorithm.

## Parameters / Member Variables
- : Pointer to the pg_sha256_ctx structure maintaining the hash state and buffer
- : Pointer to the input data to be hashed 
- : Number of bytes of input data to process

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sha256_ctx](pg_sha256_ctx.md) (context structure type)
  - PG_SHA256_BLOCK_LENGTH (constant for 64-byte block size)
  - [SHA256_Transform](../S/SHA256_Transform.md) (core transformation function)
  - memcpy (standard library function for copying data)
- Called from (representative examples):
  - [pg_cryptohash_update](pg_cryptohash_update.md) (in src/common/cryptohash.c)
  - [pg_sha224_update](pg_sha224_update.md) (in src/common/sha2.c)

## Notes and Other Information
- Handles zero-length input gracefully by returning immediately
- Calculates used buffer space from the bit count modulo block length
- Processes complete blocks directly from input for optimal performance
- Maintains accurate bit count tracking throughout the operation
- Efficiently manages partial blocks in the internal buffer
- The bit count is maintained in bits (left-shifted by 3) rather than bytes
- Critical for streaming hash operations where data arrives in arbitrary chunks
- [Variables](../V/Variables.md) are explicitly cleared at the end for security purposes

## Simplified Source

```c
void
pg_sha256_update(pg_sha256_ctx *context, const uint8 *data, size_t len)
{
    // Handle empty input
    if (len == 0)
        return;

    // Check if buffer has partial data from previous calls
    size_t used_space = (context->bitcount >> 3) % PG_SHA256_BLOCK_LENGTH;

    if (used_space > 0) {
        size_t free_space = PG_SHA256_BLOCK_LENGTH - used_space;

        if (len >= free_space) {
            // Fill buffer and process it
            memcpy(&context->buffer[used_space], data, free_space);
            context->bitcount += free_space << 3;
            len -= free_space;
            data += free_space;
            SHA256_Transform(context, context->buffer);
        } else {
            // Just add to buffer and return
            memcpy(&context->buffer[used_space], data, len);
            context->bitcount += len << 3;
            return;
        }
    }

    // Process complete 64-byte blocks directly
    while (len >= PG_SHA256_BLOCK_LENGTH) {
        SHA256_Transform(context, data);
        context->bitcount += PG_SHA256_BLOCK_LENGTH << 3;
        len -= PG_SHA256_BLOCK_LENGTH;
        data += PG_SHA256_BLOCK_LENGTH;
    }

    // Buffer any remaining partial block
    if (len > 0) {
        memcpy(context->buffer, data, len);
        context->bitcount += len << 3;
    }
}
```