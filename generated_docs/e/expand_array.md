# expand_array

## Location
[src/backend/utils/adt/array_expanded.c:50-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L50-L184)

## Overview
Converts a PostgreSQL array Datum into an expanded array representation, which provides an optimized in-memory format for array operations.

## Definition


## Detailed Description
The  function converts a standard PostgreSQL array Datum into an expanded array representation. Expanded arrays are an optimized in-memory format that allows for more efficient array operations by avoiding repeated serialization/deserialization cycles. 

The function creates a new memory context as a child of the provided parent context to hold the expanded array data. It handles both flat array representations and already-expanded arrays as input. When the source is already an expanded array with pass-by-value elements and a Datum-array representation, it can optimize by copying the metadata and Datum/isnull arrays directly.

For other cases, it creates a flat representation by detoasting and copying the source array, storing metadata about dimensions, bounds, and element types for later use.

## Parameters / Member Variables
- : The input array Datum to be expanded
- : Memory context that will be the parent of the expanded array's private context
- : Optional cache for element type metadata to avoid repeated catalog lookups across calls; can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - EOH_init_header
  - VARATT_IS_EXTERNAL_EXPANDED
  - DatumGetEOHP
  - [copy_byval_expanded_array](../c/copy_byval_expanded_array.md)
  - DatumGetArrayTypePCopy
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - EOHPGetRWDatum
  - ARR_NDIM, ARR_DIMS, ARR_LBOUND, ARR_ELEMTYPE, ARR_DATA_PTR, ARR_SIZE
- Called from (representative examples):
  - [DatumGetExpandedArray](../D/DatumGetExpandedArray.md)
  - DatumGetExpandedArrayX
  - [construct_empty_expanded_array](../c/construct_empty_expanded_array.md)
  - AARR_LBOUND

## Notes and Other Information
- The function creates a private memory context with ALLOCSET_START_SMALL_SIZES to allow for efficient memory management as the array grows
- Includes optimization for already-expanded arrays with pass-by-value elements
- The metacache parameter enables cross-call caching of element type information to improve performance
- Returns a read/write pointer to the expanded array using EOHPGetRWDatum
- The expanded array initially contains only the flat representation; deconstructed representation is created on demand