# range_contains_elem

## Location
src/backend/utils/adt/rangetypes.c: 544 - 556

## Overview
Determines whether a given range contains a specific element value, returning a boolean result indicating if the element falls within the range boundaries.

## Definition
```c
Datum range_contains_elem(PG_FUNCTION_ARGS)
```

## Detailed Description
This function tests whether a PostgreSQL range type contains a specific element value. It serves as a SQL-callable wrapper around `range_contains_elem_internal()`, which performs the actual containment logic. The function first extracts the range and element arguments, obtains the appropriate type cache information for the range type, then delegates to the internal function that performs boundary comparisons to determine containment. The containment test considers both the range boundaries and their inclusivity/exclusivity flags.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro containing:
  - `r`: RangeType pointer - the range to test for containment
  - `val`: Datum - the element value to check for containment within the range

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - macro to extract range argument
  - `PG_GETARG_DATUM` - macro to extract element value argument
  - `range_get_typcache` - function to get type cache entry for range operations
  - `RangeTypeGetOid` - function to get the OID of the range type
  - `range_contains_elem_internal` - internal function that performs actual containment logic
  - `PG_RETURN_BOOL` - macro to return boolean result

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function exposed to SQL with the `@>` operator for range @> element
- Also accessible through explicit function call syntax in SQL
- Returns true if the element is contained within the range, false otherwise
- The containment logic handles empty ranges (returns false), infinite bounds, and inclusivity/exclusivity
- The function is located in `src/backend/utils/adt/rangetypes.c:544-556`
- Works with all range types (int4range, numrange, tsrange, etc.)
- Part of PostgreSQL's comprehensive range type system supporting GiST indexing
- The actual containment algorithm is in `range_contains_elem_internal` at line 2627-2674