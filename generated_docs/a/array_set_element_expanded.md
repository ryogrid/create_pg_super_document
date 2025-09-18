# array_set_element_expanded

## Location
src/backend/utils/adt/arrayfuncs.c: 2501 - 2805

## Overview
Implements element assignment for expanded arrays, providing optimized performance by modifying arrays in-place without full reconstruction when possible.

## Definition


## Detailed Description
This function is the specialized implementation of  for expanded arrays. Expanded arrays are PostgreSQL's internal representation that allows efficient in-place modifications without the overhead of complete array reconstruction. The function handles:

1. **Array dimension management**: Can extend single-dimensional arrays by adding elements before or after existing bounds
2. **Memory management**: Safely manages memory contexts and prevents corruption during partial failures
3. **Null handling**: Maintains null bitmaps and properly handles null value assignments
4. **Bounds checking**: Validates subscripts and prevents array size overflow
5. **Storage optimization**: Efficiently reuses existing storage space when possible

The function is designed to be failure-safe, ensuring the expanded array object remains in a consistent state even if operations fail partway through.

## Parameters / Member Variables
- : The expanded array object to modify (as a Datum)
- : Number of array subscripts provided
- : Array of subscript values specifying the target element position
- : The new value to assign to the specified array element
- : Boolean indicating whether the new value is NULL
- : Type length of the array (-1 for variable-length arrays)
- : Length of individual array elements
- : Whether array elements are passed by value
- : Alignment requirement for array elements

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetExpandedArray](../D/DatumGetExpandedArray.md)
  - deconstruct_expanded_array
  - [datumCopy](../d/datumCopy.md)
  - ArrayGetNItems
  - ArrayCheckBounds
  - ArrayGetOffset
  - EOHPGetRWDatum
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [repalloc](../r/repalloc.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
- Called from:
  - [array_set_element](array_set_element.md)

## Notes and Other Information
- Only supports extending single-dimensional arrays; multi-dimensional arrays cannot be extended during assignment
- Uses overflow-safe arithmetic to prevent integer overflow when calculating new array dimensions
- Implements copy-on-write semantics for array elements when necessary
- The function maintains the expanded array's internal consistency by deferring irreversible changes until all memory allocations succeed
- Performance is optimized for repeated element assignments by reusing allocated storage space
- Part of PostgreSQL's array manipulation subsystem located in 