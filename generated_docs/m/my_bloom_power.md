# my_bloom_power

## Location
src/backend/lib/bloomfilter.c: 210 - 228

## Overview
The `my_bloom_power` function determines the power of two that represents the optimal size for a bloom filter bitset, given a target number of bits.

## Definition
```c
static int my_bloom_power(uint64 target_bitset_bits)
```

## Detailed Description
This static function calculates which element in the sequence of powers of two is less than or equal to the target bitset bits. It ensures the returned power is safe for use as the basis for actual bitset size. The function enforces a maximum limit where bitsets never exceed 2^32 bits (512MB), which is sufficient for all current PostgreSQL callers and allows the use of efficient 32-bit hash functions. The size limit also ensures compatibility with PostgreSQL's MaxAllocSize restriction, leaving room for non-bitset fields that appear before the flexible array member.

The function works by repeatedly right-shifting the target bits and incrementing a power counter until either the target becomes 0 or the power reaches 32 (the maximum allowed).

## Parameters / Member Variables
- `target_bitset_bits`: The target number of bits for the bloom filter bitset (uint64)

## Dependencies
- Functions called/Symbols referenced: None (uses only basic arithmetic operations)
- Called from:
  - [bloom_filter](../b/bloom_filter.md) (at src/backend/lib/bloomfilter.c:54)
  - [bloom_create](../b/bloom_create.md) (at src/backend/lib/bloomfilter.c:108)

## Notes and Other Information
- Maximum bitset size is limited to 2^32 bits (512MB) for performance and memory allocation reasons
- The power calculation ensures bitset sizes are always powers of two, which enables efficient modulo operations using bitwise AND
- This constraint supports the bloom filter's hash distribution strategy and avoids modulo bias effects
- The function returns -1 initially and increments up to a maximum of 32