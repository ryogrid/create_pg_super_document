# DatumGetExpandedArrayX

## Location
[src/backend/utils/adt/array_expanded.c:372-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L372-L400)

## Overview
DatumGetExpandedArrayX converts a PostgreSQL Datum to an expanded array header, with the ability to cache element type information for improved performance in repeated operations.

## Definition
```c
ExpandedArrayHeader *DatumGetExpandedArrayX(Datum d, ArrayMetaState *metacache)
```

## Detailed Description
This function is an optimized version of DatumGetExpandedArray that accepts an optional ArrayMetaState cache parameter. The function first checks if the input Datum is already a writable expanded array. If so, it returns the existing header directly and optionally updates the caller's metadata cache with the array's element type information (element_type, typlen, typbyval, typalign). If the Datum is not already an expanded array, it calls expand_array() to convert it, passing along the metadata cache to potentially accelerate the expansion process.

The function is designed for scenarios where the caller maintains a cache of array element type information and can benefit from both providing and receiving such cached data to avoid repeated type lookups.

## Parameters / Member Variables
- `d`: The input Datum that should be converted to an expanded array
- `metacache`: Optional pointer to ArrayMetaState structure for caching element type information. Can be NULL if caching is not needed.

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED_RW (checks if Datum is writable expanded array)
  - DatumGetEOHP (extracts expanded object header pointer)
  - expand_array (converts regular array to expanded form)
  - DatumGetPointer (extracts pointer from Datum)
- Called from (representative examples):
  - PG_GETARG_EXPANDED_ARRAYX (macro for function argument processing)
  - AARR_LBOUND (array lower bound access macro)

## Notes and Other Information
- The function assumes the input Datum represents a valid PostgreSQL array
- When metacache is provided and the array is already expanded, the cache is updated with current type information
- The EA_MAGIC assertion ensures the expanded array header has the correct magic number for validation
- Memory allocation for new expanded arrays occurs in CurrentMemoryContext
- This function is part of PostgreSQL's expanded object infrastructure for efficient in-memory array manipulation