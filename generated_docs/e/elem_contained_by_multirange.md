# elem_contained_by_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 1658 - 1673

## Overview
Tests whether an element value is contained by (within) a multirange - the reverse argument order version of multirange_contains_elem.

## Definition
```c
Datum elem_contained_by_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
This function checks if a given element value is contained within any of the ranges in a multirange. It is functionally identical to `multirange_contains_elem` but with the arguments in reverse order - the element comes first, then the multirange. This allows for different SQL operator syntax (element <@ multirange vs multirange @> element).

The function works by:
1. Extracting the element value and multirange from the function arguments (in reverse order)
2. Getting the type cache for the multirange's base range type
3. Calling the same internal containment function as `multirange_contains_elem`
4. Returning the boolean result

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `val`: The element value to test for containment
  - `mr`: The multirange to search within

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeType
  - PG_GETARG_MULTIRANGE_P
  - [multirange_get_typcache](../m/multirange_get_typcache.md)
  - MultirangeTypeGetOid
  - [multirange_contains_elem_internal](../m/multirange_contains_elem_internal.md)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is a SQL-callable function that implements the `<@` operator for element <@ multirange containment
- Functionally identical to `multirange_contains_elem` but with reversed argument order
- Uses the same internal containment logic as `multirange_contains_elem_internal`
- Provides syntactic flexibility: `element <@ multirange` vs `multirange @> element`
- Part of the multirange SQL functions for containment operations
- Located in src/backend/utils/adt/multirangetypes.c:1658-1673