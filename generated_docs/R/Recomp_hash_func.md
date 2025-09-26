# Recomp_hash_func

## Location
[src/include/common/unicode_norm_hashfunc.h:2712-2974](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_norm_hashfunc.h#L2712-L2974)

## Overview
A perfect hash function used for fast Unicode character recomposition lookups in PostgreSQL's Unicode normalization system.

## Definition
```c
static int
Recomp_hash_func(const void *key)
```

## Detailed Description
`Recomp_hash_func` is a static perfect hash function that serves as a key component in PostgreSQL's Unicode normalization functionality for character recomposition operations. It takes a Unicode character sequence (typically a base character followed by combining characters) as input and returns a hash value that can be used to quickly locate the sequence's recomposition information in the `pg_unicode_recompinfo` table.

The function implements a perfect hash algorithm specifically designed for Unicode recomposition, ensuring collision-free mapping for all valid Unicode character sequences that can be recomposed. This provides O(1) lookup time for Unicode recomposition operations, which is essential for efficient text normalization.

The hash function uses a dual-hashing approach with two hash calculations based on different multipliers (257 and 17). It processes exactly 8 bytes of the input key and combines the results from two separate hash table lookups in a static table containing 1,883 int16 values. The final hash value is computed as the sum of the two intermediate hash values.

## Parameters / Member Variables
- `key`: A pointer to the Unicode character sequence (8 bytes) for which the hash value should be computed

## Dependencies
- Functions called/Symbols referenced:
  - pg_unicode_recompinfo (referenced in the same file for recomposition lookups)
- Called from (representative examples):
  - Used internally within Unicode normalization routines for fast character recomposition lookups

## Notes and Other Information
- This is a generated perfect hash function, likely created by an external hash function generator tool
- The function is marked as `static`, indicating it's only used within the compilation unit where it's defined
- The hash table contains 1,883 entries, optimized for Unicode character sequences requiring recomposition
- Uses a dual-hashing technique with multipliers 257 and 17 for better distribution
- Processes exactly 8 bytes of input data, making it suitable for Unicode character sequences
- The perfect hash property eliminates collision resolution overhead, ensuring consistent O(1) performance
- This function is part of PostgreSQL's internal Unicode handling infrastructure and is not exposed to user applications
- The dual-hash approach helps distribute Unicode character sequences more evenly across the hash space