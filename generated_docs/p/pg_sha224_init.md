# pg_sha224_init

## Location
[src/common/sha2.c:978-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L978-L987)

## Overview
Initializes a SHA-224 context structure by setting the SHA-224 specific initial hash values, clearing the input buffer, and resetting the bit counter to prepare for hash computation.

## Definition
```c
void pg_sha224_init(pg_sha224_ctx *context)
```

## Detailed Description
The `pg_sha224_init` function prepares a SHA-224 context for hash computation by performing the following initialization steps:

1. **Null Check**: Validates that the context pointer is not NULL, returning early if invalid to prevent segmentation faults
2. **Initial Hash Values**: Copies the SHA-224 specific initial hash values from the `sha224_initial_hash_value` constant array into the context's state array
3. **Buffer Initialization**: Clears the input buffer that will hold partial blocks of data during incremental hashing operations
4. **Bit Counter Reset**: Initializes the single 64-bit counter to zero to track the total number of bits processed

SHA-224 uses the same internal structure and block processing algorithm as SHA-256, but with different initial hash values and a truncated output. The SHA-224 initial values are derived from the fractional parts of square roots of the 9th through 16th prime numbers, similar to SHA-384's relationship to SHA-512.

## Parameters / Member Variables
- `context`: Pointer to the SHA-224 context structure to be initialized. Must not be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - `memcpy` - Copies initial hash values to context state array
  - `memset` - Clears the input buffer to all zeros
  - `sha224_initial_hash_value` - Constant array containing the SHA-224 specific initial hash values
- Referenced types/constants:
  - `pg_sha224_ctx` - SHA-224 context structure type
  - `PG_SHA256_DIGEST_LENGTH` - Length constant for copying initial state (32 bytes, shared with SHA-256)
  - `PG_SHA256_BLOCK_LENGTH` - Block size constant for buffer initialization (64 bytes, shared with SHA-256)
- Called from (representative examples):
  - `pg_cryptohash_init` - Generic cryptographic hash initialization wrapper

## Notes and Other Information
- SHA-224 shares the same context structure type (`pg_sha224_ctx`) with SHA-256 since both algorithms use identical internal processing logic
- The function performs basic input validation by checking for NULL context pointer before proceeding
- Initial hash values are algorithm-specific constants defined in the SHA-224 specification (FIPS PUB 180-4)  
- Unlike SHA-384/SHA-512 which use a two-element bitcount array, SHA-224/SHA-256 use a single 64-bit bitcount value
- The bitcount can handle input sizes up to 2^64 - 1 bits, which is sufficient for practical applications
- This function must be called before any `pg_sha224_update` or `pg_sha224_final` operations
- Part of PostgreSQL's internal cryptographic library and should not be called directly by user code
- The use of SHA-256 constants (PG_SHA256_DIGEST_LENGTH, PG_SHA256_BLOCK_LENGTH) reflects the shared implementation between SHA-224 and SHA-256