# array_contains_nulls

## Location
[src/backend/utils/adt/arrayfuncs.c:3755-3801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3755-L3801)

## Overview
Efficiently determines whether a PostgreSQL array contains any null elements by examining the array's null bitmap, providing a definitive answer unlike the ARR_HASNULL macro.

## Definition

```c
bool
array_contains_nulls(ArrayType *array)
```
## Detailed Description
The  function performs an accurate scan of a PostgreSQL array to determine if it contains any null elements. Unlike the ARR_HASNULL macro which only indicates that the array *might* contain nulls (based on whether a null bitmap exists), this function actually examines the null bitmap bit-by-bit to provide a definitive answer.

The function implements an optimized scanning strategy: it first processes complete bytes of the null bitmap (checking 8 elements at once), then handles any remaining elements in the final partial byte. A bit value of 0 in the null bitmap indicates a null element, while 1 indicates a non-null element. The function returns true as soon as it finds the first null element, making it efficient for arrays with nulls near the beginning.

If the array has no null bitmap (ARR_HASNULL returns false), the function immediately returns false without further processing, since PostgreSQL only creates null bitmaps when necessary.

## Parameters / Member Variables
- : The PostgreSQL array object to examine for null elements

## Dependencies
- Functions called/Symbols referenced:
  - ARR_HASNULL (macro to check if array has a null bitmap)
  - ArrayGetNItems (calculates total number of elements)
  - ARR_NDIM (macro for array dimensions)
  - ARR_DIMS (macro for dimension sizes)
  - ARR_NULLBITMAP (macro to get pointer to null bitmap)
  - bits8 (type for bitmap manipulation)

- Called from (representative examples):
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md) (logical replication slot processing)
  - [array_position_common](array_position_common.md) (array element position finding)
  - [array_positions](array_positions.md) (finding all positions of an element)
  - [array_fill_internal](array_fill_internal.md) (array filling operations)
  - [width_bucket_array](../w/width_bucket_array.md) (histogram bucket calculations)
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md) (integer type modifier extraction)
  - [get_path_all](../g/get_path_all.md)/get_jsonb_path_all (JSON path operations)
  - [getWeights](../g/getWeights.md) (text search weight extraction)
  - [pg_isolation_test_session_is_blocked](../p/pg_isolation_test_session_is_blocked.md) (isolation testing)

## Notes and Other Information
- Provides an accurate determination of null presence, unlike ARR_HASNULL which only indicates possibility
- Implements byte-wise optimization: processes 8 bits at once when possible for better performance
- Uses bit mask 0xFF (all bits set) to check if a complete byte contains no nulls
- Short-circuits and returns true immediately upon finding the first null element
- The null bitmap uses inverted logic: 0 = null, 1 = not null
- Essential for operations that need to know definitively whether nulls are present, such as certain optimization decisions
- Performance scales with the position of the first null element rather than total array size
- Used extensively in array utility functions and JSON processing where null handling is critical