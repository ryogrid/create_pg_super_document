# pg_sha224_ctx

## Location
[src/common/sha2_int.h:67-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2_int.h#L67-L67)

## Overview
The pg_sha224_ctx is a type alias for pg_sha256_ctx, representing the context for SHA-224 hash computation in PostgreSQL, which reuses SHA-256's internal structure.

## Definition
```c
typedef struct pg_sha256_ctx pg_sha224_ctx;
```

## Detailed Description
The pg_sha224_ctx is defined as a type alias that directly maps to the pg_sha256_ctx structure. This design reflects the cryptographic relationship between SHA-224 and SHA-256: SHA-224 is essentially SHA-256 with different initial hash values and a truncated final output (224 bits instead of 256 bits). By reusing the same context structure, PostgreSQL efficiently implements SHA-224 without duplicating the underlying hash computation infrastructure.

The SHA-224 algorithm uses the same 32-bit arithmetic, 512-bit block processing, and internal state management as SHA-256, differing only in:
1. Initial hash values used during initialization
2. Final output truncation (224 bits vs 256 bits)
3. Object identifier in standards compliance

This architectural decision demonstrates efficient code reuse while maintaining cryptographic correctness and standards compliance.

## Parameters / Member Variables
Since pg_sha224_ctx is a type alias for pg_sha256_ctx, it inherits all the same members:
- `state[8]`: Array of 8 32-bit words containing the intermediate hash state values (initialized with SHA-224-specific values)
- `bitcount`: 64-bit counter tracking the total number of bits processed
- `buffer[PG_SHA256_BLOCK_LENGTH]`: Internal 64-byte buffer for accumulating input data until a complete 512-bit block can be processed

## Dependencies
- Functions called/Symbols referenced:
  - pg_sha256_ctx (base structure type)
  - PG_SHA256_BLOCK_LENGTH (inherited through pg_sha256_ctx)

- Called from (representative examples):
  - pg_cryptohash_ctx (general cryptographic hash context structure)
  - pg_sha224_init (initializes context with SHA-224-specific initial values)
  - pg_sha224_update (processes data chunks using SHA-256 algorithm)
  - pg_sha224_final (finalizes hash computation with 224-bit output truncation)

## Notes and Other Information
- This type alias elegantly demonstrates the relationship between SHA-224 and SHA-256 algorithms
- The same underlying SHA-256 transformation functions are used for SHA-224 operations
- Only the initialization values and final output length differ between SHA-224 and SHA-256
- This design pattern is also used for the relationship between SHA-384 and SHA-512
- The approach reduces code duplication while maintaining clear semantic separation between different hash algorithms
- SHA-224 provides a shorter hash output (224 bits) when the full 256-bit security level is not required