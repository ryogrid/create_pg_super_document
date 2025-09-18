# pg_lfind8_le

## Location
[src/include/port/pg_lfind.h:58-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_lfind.h#L58-L89)

## Overview
Searches for any 8-bit value within an array that is less than or equal to a specified key value, using vectorized operations for performance optimization.

## Definition
```c
static inline bool pg_lfind8_le(uint8 key, uint8 *base, uint32 nelem)
```

## Detailed Description
The `pg_lfind8_le` function performs a linear search through an array of 8-bit unsigned integers to find any element that is less than or equal to the specified key value. Similar to `pg_lfind8`, this function is optimized for performance using SIMD vectorized operations to process multiple elements simultaneously.

The implementation uses a hybrid approach: first processing elements in vectorized chunks using Vector8 operations, then handling any remaining elements individually with scalar comparisons. This ensures both high performance for large arrays and correctness for arrays of any size.

## Parameters / Member Variables
- `key`: The 8-bit threshold value for comparison (elements <= this value will match)
- `base`: Pointer to the array of 8-bit unsigned integers to search through  
- `nelem`: Number of elements in the array

## Dependencies
- Functions called/Symbols referenced:
  - Vector8 (vector data type for SIMD operations)
  - [vector8_load](../v/vector8_load.md) (loads data into vector register)
  - [vector8_has_le](../v/vector8_has_le.md) (checks if vector contains any value less than or equal to key)
- Called from (representative examples):
  - test_lfind8_le_internal (in test modules)

## Notes and Other Information
- The function is declared as `static inline` for performance optimization
- Uses vectorized SIMD operations with `vector8_has_le` for efficient less-than-or-equal comparisons
- Automatically handles arrays that are not perfectly aligned to vector boundaries
- Part of PostgreSQL's optimized linear find utilities for range-based searches
- Useful in scenarios requiring threshold-based filtering or range queries
- The tail_idx calculation ensures proper vector alignment by rounding down to vector boundary
- Returns true as soon as the first matching element is found (short-circuit evaluation)