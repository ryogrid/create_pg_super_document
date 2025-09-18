# array_position_common

## Location
src/backend/utils/adt/array_userfuncs.c: 1244 - 1398

## Overview
Common implementation function that searches for an element within a one-dimensional PostgreSQL array and returns its position.

## Definition


## Detailed Description
 is the core implementation for both  and  PostgreSQL functions. It searches for a specified element within a one-dimensional array and returns the 1-based index of the first occurrence found at or after a specified starting position. The function handles both null and non-null search elements, uses cached type information for efficiency across multiple calls, and employs PostgreSQL's array iteration infrastructure.

The function performs several validation checks: it rejects multi-dimensional arrays (since reporting element location would be ambiguous), handles empty arrays by returning NULL, and validates that starting positions are not null when provided. For null element searches, it can quickly return NULL if the array contains no nulls.

The search implementation uses PostgreSQL's ArrayIterator for efficient traversal and maintains type cache information (ArrayMetaState) in the function's local context to avoid repeated type lookups. It uses the element type's equality operator to compare values during the search.

## Parameters / Member Variables
- : Function call information structure containing:
  - : The input array to search within
  - : The element value to search for (can be NULL)
  -  (optional): Starting position for the search (1-based)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM, ARR_ELEMTYPE, ARR_LBOUND
  - array_contains_nulls
  - MemoryContextAlloc
  - get_typlenbyvalalign
  - lookup_type_cache
  - fmgr_info_cxt
  - array_create_iterator, array_iterate, array_free_iterator
  - FunctionCall2Coll
- Called from (representative examples):
  - array_position
  - array_position_start

## Notes and Other Information
- Static function shared between array_position and array_position_start wrapper functions
- Maintains ArrayMetaState cache in fn_extra to optimize repeated calls with same element type
- Only supports one-dimensional arrays; throws ERRCODE_FEATURE_NOT_SUPPORTED for multi-dimensional arrays
- Returns NULL for empty arrays, not found elements, or NULL input arrays
- Handles NULL element searches efficiently by checking array_contains_nulls first
- Uses collation-aware comparison through FunctionCall2Coll for proper element matching
- Memory management includes proper cleanup of toasted input arrays
- Located in src/backend/utils/adt/array_userfuncs.c:1244-1398