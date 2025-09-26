# expand_array

## Location
[src/backend/utils/adt/array_expanded.c:50-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L50-L184)

## Overview
Converts a PostgreSQL array Datum into an expanded array representation, which provides an optimized in-memory format for array operations.

## Definition

```c
struct that later if necessary.  For the pass-by-ref case, we
		 * could perhaps save some cycles with custom code that generates the
		 * deconstructed representation in parallel with copying the values,
		 * but it would be a lot of extra code for fairly marginal gain.  So,
		 * fall through into the flat-source code path.
		 */
	}

	/*
	 * Detoast and copy source array into private context, as a flat array.
	 *
	 * Note that this coding risks leaking some memory in the private context
	 * if we have to fetch data from a TOAST table;
```
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
  - [EOH_init_header](../E/EOH_init_header.md)
  - VARATT_IS_EXTERNAL_EXPANDED
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - [copy_byval_expanded_array](../c/copy_byval_expanded_array.md)
  - DatumGetArrayTypePCopy
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [EOHPGetRWDatum](../E/EOHPGetRWDatum.md)
  - ARR_NDIM, ARR_DIMS, ARR_LBOUND, ARR_ELEMTYPE, ARR_DATA_PTR, ARR_SIZE
- Called from (representative examples):
  - [DatumGetExpandedArray](../D/DatumGetExpandedArray.md)
  - [DatumGetExpandedArrayX](../D/DatumGetExpandedArrayX.md)
  - [construct_empty_expanded_array](../c/construct_empty_expanded_array.md)
  - AARR_LBOUND

## Notes and Other Information
- The function creates a private memory context with ALLOCSET_START_SMALL_SIZES to allow for efficient memory management as the array grows
- Includes optimization for already-expanded arrays with pass-by-value elements
- The metacache parameter enables cross-call caching of element type information to improve performance
- Returns a read/write pointer to the expanded array using EOHPGetRWDatum
- The expanded array initially contains only the flat representation; deconstructed representation is created on demand