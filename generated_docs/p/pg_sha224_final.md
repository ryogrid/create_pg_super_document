# pg_sha224_final

## Location
src/common/sha2.c: 994 - 1017

## Overview
Finalizes a SHA-224 hash computation and produces the final 224-bit digest output, then securely cleans up the context structure.

## Definition

```c
void
pg_sha224_final(pg_sha224_ctx *context, uint8 *digest)
```
## Detailed Description
This function completes the SHA-224 hash computation by performing the final padding and processing steps, then extracts the 224-bit hash result. The function first calls  to perform the final transformation including padding the message according to the SHA-2 specification. After the final hash state is computed, it handles endian conversion if necessary (on little-endian systems), then copies exactly 28 bytes (224 bits) from the hash state to the output digest buffer.

The function implements the key difference between SHA-224 and SHA-256: while both algorithms process data identically, SHA-224 only outputs the first 224 bits (28 bytes) of the 256-bit internal state, effectively truncating the result. After extracting the digest, the function securely zeroes out the context structure to prevent potential information leakage.

## Parameters / Member Variables
- : Pointer to the SHA-224 context structure containing the current hash state
- : Output buffer to receive the 28-byte SHA-224 hash digest (can be NULL to skip output)

## Dependencies
- Functions called/Symbols referenced:
  - SHA256_Last
  - REVERSE32 (on little-endian systems)
  - memcpy
  - memset
- Constants referenced:
  - PG_SHA224_DIGEST_LENGTH (28 bytes)
- Types referenced:
  - pg_sha224_ctx
- Called from (representative examples):
  - pg_cryptohash_final (in cryptohash.c)

## Notes and Other Information
- If  parameter is NULL, the function skips hash extraction but still performs cleanup
- On little-endian systems, performs byte order conversion using REVERSE32 macro before output
- The function extracts exactly 28 bytes from the 32-byte (256-bit) internal hash state
- Securely zeroes the context structure after use to prevent potential security issues
- SHA-224 digest length is exactly 28 bytes (224 bits), as defined by FIPS 180-4
- The endianness handling ensures consistent hash output across different architectures
- Part of PostgreSQL's cryptographic infrastructure, used for various security and integrity functions
- After calling this function, the context cannot be reused and must be reinitialized for new hash operations