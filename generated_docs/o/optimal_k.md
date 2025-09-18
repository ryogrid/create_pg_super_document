# optimal_k

## Location
src/backend/lib/bloomfilter.c: 229 - 249

## Overview
The `optimal_k` function calculates the optimal number of hash functions for a bloom filter based on the filter size in bits and the projected total number of elements to minimize false positive rates.

## Definition
```c
static int optimal_k(uint64 bitset_bits, int64 total_elems)
```

## Detailed Description
This static function implements the mathematical formula for determining the optimal number of hash functions in a bloom filter. It uses the well-known formula k = ln(2) * m/n, where m is the number of bits in the filter and n is the expected number of elements. The function uses `rint()` to round the calculated value to the nearest integer and then constrains the result between 1 and MAX_HASH_FUNCS (10) to ensure practical limits.

The optimal number of hash functions minimizes the false positive probability of the bloom filter. Too few hash functions result in insufficient bit coverage, while too many can cause excessive bit overlap, both increasing false positive rates.

## Parameters / Member Variables
- `bitset_bits`: The size of the bloom filter in bits (uint64)
- `total_elems`: The projected total number of elements to be added to the filter (int64)

## Dependencies
- Functions called/Symbols referenced:
  - MAX_HASH_FUNCS (defined as 10)
  - rint() (standard math library function)
  - log() (natural logarithm, standard math library)
  - Max() (PostgreSQL utility macro)
  - Min() (PostgreSQL utility macro)
- Called from:
  - [bloom_filter](../b/bloom_filter.md) (at src/backend/lib/bloomfilter.c:55)
  - [bloom_create](../b/bloom_create.md) (at src/backend/lib/bloomfilter.c:115)

## Notes and Other Information
- The formula k = ln(2) * m/n is mathematically derived to minimize false positive probability
- The result is constrained to be between 1 and 10 hash functions for practical implementation
- Using the optimal number of hash functions provides the best balance between computation cost and false positive rate
- The function assumes accurate estimation of total_elems for optimal performance