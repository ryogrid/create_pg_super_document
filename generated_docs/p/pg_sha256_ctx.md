# pg_sha256_ctx

## Location
[src/common/sha2_int.h:55-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2_int.h#L55-L60)

## Overview
The pg_sha256_ctx structure represents the context for SHA-256 hash computation in PostgreSQL, maintaining the internal state required for incremental hashing operations.

## Definition

```c
typedef struct pg_sha256_ctx
{
	uint32		state[8];
	uint64		bitcount;
	uint8		buffer[PG_SHA256_BLOCK_LENGTH];
} pg_sha256_ctx;
```
## Detailed Description
The pg_sha256_ctx structure is the core context structure for SHA-256 cryptographic hash operations in PostgreSQL. It maintains all necessary state information to perform incremental hash computation, allowing data to be processed in chunks rather than requiring the entire input to be available at once. This structure follows the standard SHA-256 algorithm specification and is used throughout PostgreSQL's cryptographic subsystem for secure hashing operations.

The structure is designed to support the standard init/update/final pattern of hash computation:
1. Initialize the context with initial SHA-256 state values
2. Process data in chunks through update operations
3. Finalize the hash and extract the digest

## Parameters / Member Variables
- : Array of 8 32-bit words containing the intermediate hash state values (A, B, C, D, E, F, G, H registers in SHA-256 algorithm)
- : 64-bit counter tracking the total number of bits processed, required for proper padding in the final step
- : Internal buffer of 64 bytes (512 bits) used to accumulate input data until a complete block can be processed

## Dependencies
- Functions called/Symbols referenced:
  - PG_SHA256_BLOCK_LENGTH (constant defining 64-byte block size)

- Called from (representative examples):
  - pg_cryptohash_ctx (general cryptographic hash context structure)
  - pg_sha256_init (initializes the context)
  - pg_sha256_update (processes data chunks)
  - pg_sha256_final (finalizes hash computation)
  - SHA256_Transform (core transformation function)
  - SHA256_Last (final block processing)

## Notes and Other Information
- The structure size is exactly aligned for SHA-256's 512-bit block processing requirements
- The bitcount field is essential for proper FIPS 180-4 compliant padding
- This context is also used as a base for SHA-224 operations (pg_sha224_ctx), as SHA-224 is essentially SHA-256 with different initial values and truncated output
- The buffer allows for efficient processing of arbitrarily-sized input by accumulating partial blocks
- All fields use fixed-width integer types to ensure consistent behavior across different platforms