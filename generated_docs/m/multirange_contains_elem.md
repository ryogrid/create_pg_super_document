# multirange_contains_elem

## Location
[src/backend/utils/adt/multirangetypes.c:1645-1657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1645-L1657)

## Overview
Tests whether a multirange contains a specific element value.

## Definition
```c
Datum multirange_contains_elem(PG_FUNCTION_ARGS)
```

## Detailed Description
This function checks if a given element value is contained within any of the ranges in a multirange. It serves as the SQL-callable wrapper for the internal containment testing logic. The function delegates the actual containment check to `multirange_contains_elem_internal` after setting up the necessary type information.

The function works by:
1. Extracting the multirange and element value from the function arguments
2. Getting the type cache for the multirange's base range type
3. Calling the internal containment function to perform the actual check
4. Returning the boolean result

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `mr`: The input multirange to search within
  - `val`: The element value to search for

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeType
  - PG_GETARG_MULTIRANGE_P
  - [multirange_get_typcache](multirange_get_typcache.md)
  - MultirangeTypeGetOid
  - [multirange_contains_elem_internal](multirange_contains_elem_internal.md)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is a SQL-callable function that implements the `@>` operator for multirange @> element containment
- The actual containment logic is implemented in `multirange_contains_elem_internal`
- An element is contained if it falls within any of the ranges in the multirange
- Part of the multirange SQL functions for containment operations
- Located in src/backend/utils/adt/multirangetypes.c:1645-1657