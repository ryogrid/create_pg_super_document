# pg_sha256_update

## Location
src/common/sha2.c: 476 - 528

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
  - pg_sha256_ctx (context structure type)
  - PG_SHA256_BLOCK_LENGTH (constant for 64-byte block size)
  - SHA256_Transform (core transformation function)
  - memcpy (standard library function for copying data)
- Called from (representative examples):
  - pg_cryptohash_update (in src/common/cryptohash.c)
  - pg_sha224_update (in src/common/sha2.c)

## Notes and Other Information
- Handles zero-length input gracefully by returning immediately
- Calculates used buffer space from the bit count modulo block length
- Processes complete blocks directly from input for optimal performance
- Maintains accurate bit count tracking throughout the operation
- Efficiently manages partial blocks in the internal buffer
- The bit count is maintained in bits (left-shifted by 3) rather than bytes
- Critical for streaming hash operations where data arrives in arbitrary chunks
- Variables are explicitly cleared at the end for security purposes