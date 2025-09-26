# pg_sha384_init

## Location
[src/common/sha2.c:934-943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L934-L943)

## Overview
Initializes a SHA-384 context structure by setting the initial hash values, clearing the input buffer, and resetting bit counters to prepare for hash computation.

## Definition
```c
void pg_sha384_init(pg_sha384_ctx *context)
```

## Detailed Description
The `pg_sha384_init` function prepares a SHA-384 context for hash computation by performing the following initialization steps:

1. **Null Check**: Validates that the context pointer is not NULL, returning early if invalid
2. **Initial Hash Values**: Copies the SHA-384 specific initial hash values from `sha384_initial_hash_value` constant array into the context's state array
3. **Buffer Initialization**: Clears the input buffer that will hold partial blocks of data during incremental hashing
4. **Bit Counter Reset**: Initializes both 64-bit counters in the bitcount array to zero to track total bits processed

SHA-384 uses different initial hash values compared to SHA-512, even though both algorithms use the same internal structure and processing logic. The SHA-384 initial values are derived from the fractional parts of square roots of the 9th through 16th prime numbers.

## Parameters / Member Variables
- `context`: Pointer to the SHA-384 context structure to be initialized. Must not be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - `memcpy` - Copies initial hash values to context state
  - `memset` - Clears the input buffer
  - `sha384_initial_hash_value` - Constant array containing SHA-384 initial hash values
- Referenced types/constants:
  - `pg_sha384_ctx` - SHA-384 context structure type
  - `PG_SHA512_DIGEST_LENGTH` - Length constant for copying initial state (64 bytes)  
  - `PG_SHA384_BLOCK_LENGTH` - Block size constant for buffer initialization (128 bytes)
- Called from (representative examples):
  - `pg_cryptohash_init` - Generic cryptographic hash initialization wrapper

## Notes and Other Information
- SHA-384 shares the same context structure type (`pg_sha384_ctx`) with SHA-512 since both algorithms use identical internal processing
- The function performs basic input validation by checking for NULL context pointer
- Initial hash values are algorithm-specific constants defined in the SHA-384 specification (FIPS PUB 180-4)
- The bitcount array uses two 64-bit values to handle input sizes up to 2^128 - 1 bits
- This function must be called before any `pg_sha384_update` or `pg_sha384_final` operations
- Part of PostgreSQL's internal cryptographic library and should not be called directly by user code