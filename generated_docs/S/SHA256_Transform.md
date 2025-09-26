# SHA256_Transform

## Location
[src/common/sha2.c:315-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L315-L386)

## Overview
The core SHA-256 transformation function that processes a single 512-bit block of data through 64 rounds of cryptographic operations to update the hash state.

## Definition

```c
static void
SHA256_Transform(pg_sha256_ctx *context, const uint8 *data)
```
## Detailed Description
SHA256_Transform is the heart of the SHA-256 algorithm, implementing the 64-round transformation that processes each 512-bit block of input data. The function operates on eight 32-bit working variables (a through h) initialized with the current hash state values. It performs two distinct phases: rounds 0-15 use the ROUND256_0_TO_15 macro which directly processes input data, while rounds 16-63 use the ROUND256 macro which operates on previously computed message schedule values. Each round applies a complex series of logical functions, rotations, and additions designed to provide cryptographic strength. After all 64 rounds, the working variables are added back to the context state to produce the intermediate hash value.

## Parameters / Member Variables
- : Pointer to the pg_sha256_ctx structure containing the current hash state and working buffer
- : Pointer to the 512-bit (64-byte) block of input data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sha256_ctx](../p/pg_sha256_ctx.md) (context structure type)
  - ROUND256_0_TO_15 (macro for rounds 0-15)
  - ROUND256 (macro for rounds 16-63)
- Called from (representative examples):
  - [pg_sha256_update](../p/pg_sha256_update.md) (in src/common/sha2.c)
  - [SHA256_Last](SHA256_Last.md) (in src/common/sha2.c)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Implements the standard SHA-256 compression function as specified in FIPS 180-4
- Uses unrolled loops for performance optimization (8 rounds per loop iteration)
- Working variables are explicitly cleared at the end for security purposes
- The W256 pointer aliases the context buffer for efficient word access
- Critical for the security properties of SHA-256 - any modification would break the hash algorithm
- Processing exactly 512 bits (64 bytes) of data per call is mandatory for correct operation