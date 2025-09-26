# SHA256_Last

## Location
src/common/sha2.c: 529 - 576

## Overview
Performs the final padding and processing steps required to complete a SHA-256 hash computation according to the cryptographic specification.

## Definition

```c
static void
SHA256_Last(pg_sha256_ctx *context)
```
## Detailed Description
SHA256_Last implements the crucial final phase of SHA-256 hashing, applying the mandatory padding scheme defined in the cryptographic specification. The function appends a single '1' bit (0x80 byte) followed by zero padding to ensure the message length is congruent to 448 modulo 512 bits, leaving exactly 64 bits for the message length. The total bit count is then appended as a 64-bit big-endian integer. If the current block doesn't have sufficient space for both padding and length, the function processes an additional block. The endianness conversion ensures cross-platform compatibility. This padding is essential for the security properties of SHA-256, preventing length extension attacks and ensuring deterministic hash values.

## Parameters / Member Variables
- : Pointer to the pg_sha256_ctx structure containing the current hash state and accumulated bit count

## Dependencies
- Functions called/Symbols referenced:
  - pg_sha256_ctx (context structure type)
  - PG_SHA256_BLOCK_LENGTH (constant for 64-byte block size)
  - PG_SHA256_SHORT_BLOCK_LENGTH (constant for 56-byte short block)
  - REVERSE64 (macro for byte order conversion)
  - SHA256_Transform (core transformation function)
  - memset (standard library function for zero padding)
- Called from (representative examples):
  - pg_sha256_final (in src/common/sha2.c)
  - pg_sha224_final (in src/common/sha2.c)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Implements the mandatory padding scheme from FIPS 180-4 specification
- Handles both cases where padding fits in current block or requires an additional block
- The bit count is stored in big-endian format for specification compliance
- Byte order conversion is conditional on the target architecture's endianness
- The 0x80 byte represents a single '1' bit followed by seven '0' bits
- Critical for preventing length extension attacks in cryptographic applications
- Must be called exactly once at the end of each hash computation for correct results