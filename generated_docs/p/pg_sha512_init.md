# pg_sha512_init

## Location
[src/common/sha2.c:605-617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L605-L617)

## Overview
Initializes a SHA-512 cryptographic hash context structure with the standard initial hash values and resets all state variables.

## Definition

```c
void
pg_sha512_init(pg_sha512_ctx *context)
```
## Detailed Description
The  function prepares a SHA-512 context structure for hash computation by:
1. Validating the context pointer is non-NULL
2. Copying the SHA-512 standard initial hash values (8 64-bit words) into the context's state array
3. Zeroing the internal buffer that holds partial message blocks
4. Resetting the bit counters that track total message length

This function must be called before any SHA-512 update or finalize operations. The initial hash values used are the standard SHA-512 constants as specified in FIPS 180-4.

## Parameters / Member Variables
- : Pointer to a pg_sha512_ctx structure to be initialized. Function returns early if NULL.

## Dependencies
- Functions called/Symbols referenced:
  - memcpy
  - memset
  - sha512_initial_hash_value (static array of initial SHA-512 hash values)
- Constants used:
  - PG_SHA512_DIGEST_LENGTH (64 bytes)
  - PG_SHA512_BLOCK_LENGTH (128 bytes)
- Called from (representative examples):
  - [pg_cryptohash_init](pg_cryptohash_init.md)

## Notes and Other Information
- Safe to call with NULL context pointer - function will return without error
- The initial hash values are the SHA-512 standard constants: 0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL, etc.
- Bitcount is implemented as a 2-element 64-bit array to handle the 128-bit message length counter required by SHA-512
- Part of PostgreSQL's cryptographic infrastructure supporting SHA-512 hashing operations
- Must be paired with pg_sha512_update() calls and pg_sha512_final() to complete hash computation

## Simplified Source

```c
void pg_sha512_init(pg_sha512_ctx *context)
{
    // Check if context pointer is valid
    if (context == NULL)
        return;

    // Copy SHA-512 initial hash values to context state
    memcpy(context->state, sha512_initial_hash_value, PG_SHA512_DIGEST_LENGTH);

    // Clear the input buffer
    memset(context->buffer, 0, PG_SHA512_BLOCK_LENGTH);

    // Reset both bit counters to zero
    context->bitcount[0] = context->bitcount[1] = 0;
}
```