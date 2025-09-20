# pg_lfind8

## Location
[src/include/port/pg_lfind.h:26-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_lfind.h#L26-L57)

## Overview
Searches for a specific 8-bit value within an array of 8-bit integers using vectorized operations for performance optimization.

## Definition

```c
static inline bool
pg_lfind8(uint8 key, uint8 *base, uint32 nelem)
```
## Detailed Description
The  function performs a linear search for a given 8-bit key value within an array of 8-bit unsigned integers. The function is optimized for performance by using SIMD (Single Instruction, Multiple Data) vectorized operations when possible, processing multiple elements simultaneously. When the array size is not perfectly aligned with the vector size, it falls back to element-by-element comparison for the remaining elements.

The implementation first processes elements in chunks using vectorized operations (Vector8), then handles any remaining elements individually. This hybrid approach maximizes performance while ensuring correctness for arrays of any size.

## Parameters / Member Variables
- : The 8-bit value to search for in the array
- : Pointer to the array of 8-bit unsigned integers to search through
- : Number of elements in the array

## Dependencies
- Functions called/Symbols referenced:
  - Vector8 (vector data type for SIMD operations)
  - [vector8_load](../v/vector8_load.md) (loads data into vector register)
  - [vector8_has](../v/vector8_has.md) (checks if vector contains the target value)
- Called from (representative examples):
  - test_lfind8_internal (in test modules)

## Notes and Other Information
- The function is declared as  for performance optimization
- Uses vectorized SIMD operations to process multiple elements simultaneously
- Automatically handles arrays that are not perfectly aligned to vector boundaries
- Part of PostgreSQL's optimized linear find utilities located in pg_lfind.h
- Primarily used in performance-critical scenarios where fast linear search is needed
- The tail_idx calculation ensures proper vector alignment by rounding down to vector boundary