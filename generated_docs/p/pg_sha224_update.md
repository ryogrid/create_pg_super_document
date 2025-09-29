# pg_sha224_update

## Location
[src/common/sha2.c:988-993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L988-L993)

## Overview
Updates a SHA-224 hash context with new data by delegating to the SHA-256 update function, since SHA-224 is essentially SHA-256 with a truncated output.

## Definition

```c
void
pg_sha224_update(pg_sha224_ctx *context, const uint8 *data, size_t len)
```
## Detailed Description
This function serves as a thin wrapper around  to provide SHA-224 hash functionality. SHA-224 and SHA-256 use identical algorithms and processing, with the only difference being that SHA-224 uses different initial hash values and truncates the final output to 224 bits instead of 256 bits. Since the update operation is identical for both algorithms, this function simply casts the SHA-224 context to a SHA-256 context and calls .

The function processes input data incrementally, allowing for streaming hash computation where data can be fed in multiple chunks rather than all at once. This is particularly useful for large datasets or when data arrives in fragments.

## Parameters / Member Variables
- : Pointer to the SHA-224 context structure that maintains the current hash state
- : Pointer to the input data buffer to be processed
- : Number of bytes in the data buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sha256_update](pg_sha256_update.md)
- Types referenced:
  - [pg_sha224_ctx](pg_sha224_ctx.md) (typedef for pg_sha256_ctx)
  - [pg_sha256_ctx](pg_sha256_ctx.md)
- Called from (representative examples):
  - [pg_cryptohash_update](pg_cryptohash_update.md) (in cryptohash.c)

## Notes and Other Information
- SHA-224 is defined in FIPS 180-4 as a variant of SHA-256
- The context structure  is actually a typedef alias for
- This implementation leverages the fact that SHA-224 and SHA-256 differ only in initialization values and output truncation
- The function can be called multiple times to process data in chunks
- Calling with  is valid and does nothing (handled by the underlying )
- Part of PostgreSQL's cryptographic hash infrastructure used for various security functions

## Simplified Source

```c
void
pg_sha224_update(pg_sha224_ctx *context, const uint8 *data, size_t len)
{
    // SHA-224 uses identical processing to SHA-256
    // Only difference is initial values and output truncation
    pg_sha256_update((pg_sha256_ctx *) context, data, len);
}
```