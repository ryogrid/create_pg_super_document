# pg_sha384_ctx

## Location
src/common/sha2_int.h: 68 - 91

## Overview
The pg_sha384_ctx is a type alias for pg_sha512_ctx, representing the context for SHA-384 hash computation in PostgreSQL, which reuses SHA-512's internal structure.

## Definition
```c
typedef struct pg_sha512_ctx pg_sha384_ctx;
```

## Detailed Description
The pg_sha384_ctx is defined as a type alias that directly maps to the pg_sha512_ctx structure. This design reflects the cryptographic relationship between SHA-384 and SHA-512: SHA-384 is essentially SHA-512 with different initial hash values and a truncated final output (384 bits instead of 512 bits). By reusing the same context structure, PostgreSQL efficiently implements SHA-384 without duplicating the underlying hash computation infrastructure.

The SHA-384 algorithm uses the same 64-bit arithmetic, 1024-bit block processing, and internal state management as SHA-512, differing only in:
1. Initial hash values used during initialization (different from SHA-512 values)
2. Final output truncation (384 bits vs 512 bits)
3. Object identifier in standards compliance

This architectural pattern mirrors the relationship between SHA-224 and SHA-256, demonstrating consistent design principles across PostgreSQL's cryptographic hash implementations.

## Parameters / Member Variables
Since pg_sha384_ctx is a type alias for pg_sha512_ctx, it inherits all the same members:
- `state[8]`: Array of 8 64-bit words containing the intermediate hash state values (initialized with SHA-384-specific values)
- `bitcount[2]`: Array of 2 64-bit counters to track the total number of bits processed, supporting inputs up to 2^128-1 bits
- `buffer[PG_SHA512_BLOCK_LENGTH]`: Internal 128-byte buffer for accumulating input data until a complete 1024-bit block can be processed

## Dependencies
- Functions called/Symbols referenced:
  - pg_sha512_ctx (base structure type)
  - PG_SHA512_BLOCK_LENGTH (inherited through pg_sha512_ctx)

- Called from (representative examples):
  - pg_cryptohash_ctx (general cryptographic hash context structure)
  - pg_sha384_init (initializes context with SHA-384-specific initial values)
  - pg_sha384_update (processes data chunks using SHA-512 algorithm)
  - pg_sha384_final (finalizes hash computation with 384-bit output truncation)

## Notes and Other Information
- This type alias demonstrates the architectural relationship between SHA-384 and SHA-512 algorithms
- The same underlying SHA-512 transformation functions are used for SHA-384 operations
- Only the initialization values and final output length differ between SHA-384 and SHA-512
- This design pattern matches the SHA-224/SHA-256 relationship, showing consistent cryptographic implementation strategy
- SHA-384 provides a middle ground between SHA-256 (256 bits) and SHA-512 (512 bits) for applications requiring intermediate security levels
- The approach reduces code duplication while maintaining clear semantic separation between different hash algorithms
- Uses 64-bit arithmetic throughout, providing enhanced security properties compared to 32-bit SHA-256 family algorithms