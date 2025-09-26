# md5_calc

## Location
[src/common/md5.c:154-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5.c#L154-L309)

## Overview
Performs the core MD5 algorithm computation on a single 64-byte block of data, implementing the four rounds of MD5 transformation operations.

## Definition
```c
static void md5_calc(const uint8 *b64, pg_md5_ctx *ctx)
```

## Detailed Description
The `md5_calc` function is the heart of the MD5 hashing algorithm implementation in PostgreSQL. It processes a single 512-bit (64-byte) block of input data through the MD5 algorithm's four transformation rounds (16 operations each). The function updates the MD5 context state variables (A, B, C, D) by applying the MD5 mathematical operations defined in RFC 1321.

The implementation handles endianness differences: on little-endian systems it directly uses the input bytes as 32-bit words, while on big-endian systems it performs byte swapping to ensure correct word ordering. After processing all four rounds (64 total operations), it adds the computed values back to the context state to maintain the running hash.

## Parameters / Member Variables
- `b64`: Pointer to a 64-byte block of input data to be processed
- `ctx`: Pointer to the MD5 context structure containing the current hash state (md5_sta, md5_stb, md5_stc, md5_std)

## Dependencies
- Functions called/Symbols referenced:
  - ROUND1 (macro for MD5 round 1 operations)
  - ROUND2 (macro for MD5 round 2 operations)
  - ROUND3 (macro for MD5 round 3 operations)
  - ROUND4 (macro for MD5 round 4 operations)
  - Sa, Sb, Sc, Sd (shift constants for round 1)
  - Se, Sf, Sg, Sh (shift constants for round 2)
  - Si, Sj, Sk, Sl (shift constants for round 3)
  - Sm, Sn, So, Sp (shift constants for round 4)
  - pg_md5_ctx (MD5 context structure type)
- Called from (representative examples):
  - md5_pad
  - pg_md5_update

## Notes and Other Information
- This function is static and only used internally within the MD5 implementation
- The function implements the exact MD5 algorithm as specified in RFC 1321
- Big-endian systems require explicit byte swapping due to MD5's little-endian word processing requirements
- Each round applies 16 operations with specific mathematical transformations and rotation amounts
- The function maintains the cumulative hash state by adding the round results to the existing context values