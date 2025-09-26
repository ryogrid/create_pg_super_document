# pg_sha224_update

## Location
src/common/sha2.c: 988 - 993

## Overview
Updates a SHA-224 hash context with new data by delegating to the SHA-256 update function, since SHA-224 is essentially SHA-256 with a truncated output.

## Definition


## Detailed Description
This function serves as a thin wrapper around  to provide SHA-224 hash functionality. SHA-224 and SHA-256 use identical algorithms and processing, with the only difference being that SHA-224 uses different initial hash values and truncates the final output to 224 bits instead of 256 bits. Since the update operation is identical for both algorithms, this function simply casts the SHA-224 context to a SHA-256 context and calls .

The function processes input data incrementally, allowing for streaming hash computation where data can be fed in multiple chunks rather than all at once. This is particularly useful for large datasets or when data arrives in fragments.

## Parameters / Member Variables
- : Pointer to the SHA-224 context structure that maintains the current hash state
- : Pointer to the input data buffer to be processed
- : Number of bytes in the data buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - pg_sha256_update
- Types referenced:
  - pg_sha224_ctx (typedef for pg_sha256_ctx)
  - pg_sha256_ctx
- Called from (representative examples):
  - pg_cryptohash_update (in cryptohash.c)

## Notes and Other Information
- SHA-224 is defined in FIPS 180-4 as a variant of SHA-256
- The context structure  is actually a typedef alias for 
- This implementation leverages the fact that SHA-224 and SHA-256 differ only in initialization values and output truncation
- The function can be called multiple times to process data in chunks
- Calling with  is valid and does nothing (handled by the underlying )
- Part of PostgreSQL's cryptographic hash infrastructure used for various security functions