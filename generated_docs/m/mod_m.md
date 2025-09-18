# mod_m

## Location
src/backend/lib/bloomfilter.c: 288 - 294

## Overview
The `mod_m` function performs fast modulo calculation for power-of-two values using bitwise AND operations, optimized for bloom filter hash value distribution.

## Definition
```c
static inline uint32 mod_m(uint32 val, uint64 m)
```

## Detailed Description
This inline static function calculates "val MOD m" efficiently by taking advantage of the mathematical property that for power-of-two numbers, modulo operations can be replaced with bitwise AND operations. Instead of using the expensive division-based modulo operator, it uses the formula: val & (m - 1), which is equivalent to val % m when m is a power of two.

The function includes two assertions to verify its preconditions: m must not exceed PG_UINT32_MAX + 1 (ensuring it fits in the expected range), and m must be a power of two (verified by checking that (m-1) & m equals 0). This optimization not only improves performance but also avoids modulo bias effects that could affect hash distribution quality.

## Parameters / Member Variables
- `val`: The value to calculate modulo for (uint32)
- `m`: The modulo divisor, must be a power of two (uint64)

## Dependencies
- Functions called/Symbols referenced:
  - PG_UINT32_MAX (PostgreSQL constant)
  - Assert (PostgreSQL assertion macro)
  - UINT64CONST (PostgreSQL macro for 64-bit constants)
- Called from:
  - bloom_filter (at src/backend/lib/bloomfilter.c:58)
  - k_hashes (at src/backend/lib/bloomfilter.c:264, 265, 271, 272) - called 4 times

## Notes and Other Information
- Requires m to be a power of two for correctness - this is enforced by assertion
- Uses bitwise AND instead of division for significant performance improvement
- Avoids modulo bias effects that could skew hash distribution in bloom filters
- The power-of-two constraint aligns with bloom filter bitset sizes determined by my_bloom_power
- Marked as inline for maximum performance in hash-intensive bloom filter operations
- The assertion checks ensure the function is only used with valid power-of-two values up to the supported range