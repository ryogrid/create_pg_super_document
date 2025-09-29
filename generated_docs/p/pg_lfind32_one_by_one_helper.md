# pg_lfind32_one_by_one_helper

## Location
[src/include/port/pg_lfind.h:90-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_lfind.h#L90-L108)

## Overview
A simple scalar implementation for searching 32-bit integers one element at a time, serving as a fallback when vectorized SIMD operations are not available or appropriate.

## Definition
```c
static inline bool pg_lfind32_one_by_one_helper(uint32 key, const uint32 *base, uint32 nelem)
```

## Detailed Description
The `pg_lfind32_one_by_one_helper` function provides a straightforward, non-vectorized implementation for linear search of 32-bit integers. This helper function is designed to be used as a fallback mechanism when SIMD optimizations are not available or when processing small arrays where the overhead of vectorized operations would not provide performance benefits.

The function iterates through each element in the array sequentially, performing direct scalar comparisons. This approach ensures compatibility across all platforms and serves as a reliable baseline implementation.

## Parameters / Member Variables
- `key`: The 32-bit value to search for in the array
- `base`: Pointer to the array of 32-bit unsigned integers to search through (marked as const)
- `nelem`: Number of elements in the array to search

## Dependencies
- Functions called/Symbols referenced:
  - USE_NO_SIMD (compilation flag/macro reference)
- Called from (representative examples):
  - [pg_lfind32](pg_lfind32.md) (main 32-bit search function - used as fallback)

## Notes and Other Information
- The function is declared as `static inline` for performance optimization
- Serves as a fallback implementation when SIMD operations are not available
- The caller is responsible for ensuring the array contains at least nelem valid elements
- Used by the main `pg_lfind32` function in scenarios where vectorized operations are not optimal
- Simple scalar implementation with no SIMD dependencies
- Part of PostgreSQL's layered optimization strategy for linear search operations
- Returns true immediately upon finding the first match (short-circuit evaluation)

## Simplified Source

```c
static inline bool
pg_lfind32_one_by_one_helper(uint32 key, const uint32 *base, uint32 nelem)
{
    // Simple linear search through array elements
    for (uint32 i = 0; i < nelem; i++) {
        if (key == base[i])
            return true;  // Found match
    }

    return false;  // No match found
}
```