# pg_sha512_ctx

## Location
[src/common/sha2_int.h:61-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2_int.h#L61-L66)

## Overview
The pg_sha512_ctx structure represents the context for SHA-512 hash computation in PostgreSQL, maintaining the internal state required for incremental hashing operations with 64-bit arithmetic.

## Definition
```c
typedef struct pg_sha512_ctx
{
    uint64      state[8];
    uint64      bitcount[2];
    uint8       buffer[PG_SHA512_BLOCK_LENGTH];
} pg_sha512_ctx;
```

## Detailed Description
The pg_sha512_ctx structure is the core context structure for SHA-512 cryptographic hash operations in PostgreSQL. It maintains all necessary state information to perform incremental hash computation using 64-bit arithmetic, allowing data to be processed in chunks rather than requiring the entire input to be available at once. This structure follows the SHA-512 algorithm specification from FIPS 180-4 and is used throughout PostgreSQL's cryptographic subsystem for secure hashing operations requiring longer hash lengths.

The structure supports the standard init/update/final pattern and is also used as the foundation for SHA-384 operations, as SHA-384 is essentially SHA-512 with different initial values and truncated output. The larger block size (128 bytes vs 64 bytes for SHA-256) and 64-bit arithmetic provide enhanced security properties.

## Parameters / Member Variables
- `state[8]`: Array of 8 64-bit words containing the intermediate hash state values (A, B, C, D, E, F, G, H registers in SHA-512 algorithm)
- `bitcount[2]`: Array of 2 64-bit counters to track the total number of bits processed, allowing for inputs up to 2^128-1 bits in length
- `buffer[PG_SHA512_BLOCK_LENGTH]`: Internal buffer of 128 bytes (1024 bits) used to accumulate input data until a complete block can be processed

## Dependencies
- Functions called/Symbols referenced:
  - PG_SHA512_BLOCK_LENGTH (constant defining 128-byte block size)

- Called from (representative examples):
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md) (general cryptographic hash context structure)
  - [pg_sha512_init](pg_sha512_init.md) (initializes the context)
  - [pg_sha512_update](pg_sha512_update.md) (processes data chunks)
  - [pg_sha512_final](pg_sha512_final.md) (finalizes hash computation)
  - [SHA512_Transform](../S/SHA512_Transform.md) (core transformation function)
  - [SHA512_Last](../S/SHA512_Last.md) (final block processing)
  - [pg_sha384_update](pg_sha384_update.md) (SHA-384 operations using SHA-512 context)
  - [pg_sha384_final](pg_sha384_final.md) (SHA-384 finalization)

## Notes and Other Information
- The structure uses 64-bit arithmetic throughout, distinguishing it from the 32-bit SHA-256 family
- The double-word bitcount array allows tracking of extremely large inputs (up to 2^128-1 bits)
- The 128-byte buffer size matches SHA-512's 1024-bit block processing requirements
- This context is reused for SHA-384 operations, demonstrating the architectural relationship between these algorithms
- The larger state and buffer sizes provide enhanced security properties compared to SHA-256
- All operations maintain strict compliance with FIPS 180-4 specification for SHA-512