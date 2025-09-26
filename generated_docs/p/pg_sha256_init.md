# pg_sha256_init

## Location
[src/common/sha2.c:279-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L279-L291)

## Overview
Initializes a SHA-256 context structure to prepare it for hashing operations by setting the initial hash values, clearing the buffer, and resetting the bit count.

## Definition

```c
void
pg_sha256_init(pg_sha256_ctx *context)
```
## Detailed Description
pg_sha256_init is the initialization function for SHA-256 hashing in PostgreSQL. It prepares a pg_sha256_ctx structure for use by copying the standard SHA-256 initial hash values into the context's state array, clearing the input buffer, and resetting the bit counter to zero. This function must be called before any SHA-256 hashing operations can begin. The function includes a null pointer check for safety and uses the standard SHA-256 initialization constants defined in the cryptographic specification.

## Parameters / Member Variables
- : Pointer to a pg_sha256_ctx structure that will be initialized for SHA-256 operations

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sha256_ctx](pg_sha256_ctx.md) (context structure type)
  - PG_SHA256_DIGEST_LENGTH (constant for digest length)
  - PG_SHA256_BLOCK_LENGTH (constant for block length)
  - sha256_initial_hash_value (initial hash constants)
  - memcpy (standard library function)
  - memset (standard library function)
- Called from (representative examples):
  - [pg_cryptohash_init](pg_cryptohash_init.md) (in src/common/cryptohash.c)

## Notes and Other Information
- This function performs null pointer validation before initialization
- The initial hash values are the standard SHA-256 constants as defined in FIPS 180-4
- The buffer is cleared to ensure no residual data affects the hash computation
- The bitcount is initialized to zero to track the total number of bits processed
- Part of PostgreSQL's common cryptographic hash implementation used across the system
- Must be called before pg_sha256_update and pg_sha256_final operations