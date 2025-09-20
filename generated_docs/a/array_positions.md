# array_positions

## Location
[src/backend/utils/adt/array_userfuncs.c:1399-1536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1399-L1536)

## Overview
A PostgreSQL user function that returns an array of all positions where a specified element occurs in a one-dimensional array.

## Definition

```c
Datum
array_positions(PG_FUNCTION_ARGS)
```
## Detailed Description
 searches through a one-dimensional PostgreSQL array and returns an array containing the 1-based positions of all occurrences of a specified element. Unlike  which returns only the first match, this function finds and returns all matching positions. It uses "IS NOT DISTINCT FROM" semantics for comparisons, meaning it can properly handle NULL values in both the search element and array elements.

The function builds the result array incrementally using PostgreSQL's ArrayBuildState mechanism. When no matches are found, it returns an empty array (not NULL). For NULL input arrays, it returns NULL. The function maintains the same restrictions as other array position functions: it only works with one-dimensional arrays and will reject multi-dimensional arrays.

Like , it uses cached type information (ArrayMetaState) for efficiency across multiple calls and employs PostgreSQL's array iteration infrastructure for traversal.

## Parameters / Member Variables
- : Function call information structure containing:
  - : The input array to search within
  - : The element value to search for (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM, ARR_ELEMTYPE, ARR_LBOUND
  - [array_contains_nulls](array_contains_nulls.md)
  - [initArrayResult](../i/initArrayResult.md), accumArrayResult, makeArrayResult
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [array_create_iterator](array_create_iterator.md), array_iterate, array_free_iterator
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - SQL function  (through function catalog)

## Notes and Other Information
- Returns an empty array when no matches are found, unlike array_position which returns NULL
- Uses "IS NOT DISTINCT FROM" semantics for comparisons, properly handling NULL values
- Only supports one-dimensional arrays; throws ERRCODE_FEATURE_NOT_SUPPORTED for multi-dimensional arrays
- Maintains ArrayMetaState cache in fn_extra to optimize repeated calls with same element type
- Uses ArrayBuildState for efficient incremental array construction
- Memory management includes proper cleanup of toasted input arrays
- Returns integer array (INT4OID) containing all matching positions
- Can efficiently skip null searches when array contains no nulls
- Located in src/backend/utils/adt/array_userfuncs.c:1399-1536