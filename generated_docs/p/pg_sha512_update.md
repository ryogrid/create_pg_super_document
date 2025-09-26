# pg_sha512_update

## Location
src/common/sha2.c: 802 - 854

## Overview
Incrementally processes input data for SHA-512 hash computation, handling arbitrary-length data by buffering partial blocks.

## Definition


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
- : Pointer to the SHA-512 context structure containing state and buffer
- : Pointer to input data to be processed (can be NULL if len is 0)
- : Number of bytes to process from the data buffer

## Dependencies
- Functions called/Symbols referenced:
  - SHA512_Transform
  - ADDINC128
  - memcpy
- Constants used:
  - PG_SHA512_BLOCK_LENGTH (128 bytes)
- Called from (representative examples):
  - pg_cryptohash_update
  - pg_sha384_update

## Notes and Other Information
- Safe to call with len=0 (no-op)
- Can be called multiple times to process large amounts of data incrementally
- Maintains bit count as a 128-bit integer to handle messages up to 2^128-1 bits in length
- Efficiently processes complete blocks directly without unnecessary copying
- Buffer management ensures no data is lost between calls
- Part of PostgreSQL's streaming hash interface supporting both small and very large inputs
- Must be followed by pg_sha512_final() to complete the hash and obtain the digest