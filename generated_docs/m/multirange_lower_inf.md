# multirange_lower_inf

## Location
[src/backend/utils/adt/multirangetypes.c:1603-1621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1603-L1621)

## Overview
Returns whether the lower bound of the first range in a multirange is infinite (unbounded).

## Definition
```c
Datum multirange_lower_inf(PG_FUNCTION_ARGS)
```

## Detailed Description
This function determines if the lower bound of the leftmost (first) range in a multirange is infinite. It extracts the bounds of the first range in the multirange and returns the infinite flag of the lower bound. If the multirange is empty, it returns false since there are no bounds to examine.

The function works by:
1. Checking if the multirange is empty - returns false if so
2. Getting the type cache for the multirange's base range type
3. Extracting the bounds of the first range (at index 0)
4. Returning the infinite flag of the lower bound

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `mr`: The input multirange to examine

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeType
  - PG_GETARG_MULTIRANGE_P
  - RangeBound
  - MultirangeIsEmpty
  - [multirange_get_typcache](multirange_get_typcache.md)
  - MultirangeTypeGetOid
  - [multirange_get_bounds](multirange_get_bounds.md)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function specifically examines the **first** range in the multirange, not all ranges
- Returns false for empty multiranges as they have no bounds
- An infinite lower bound means the range extends indefinitely in the negative direction
- Part of the multirange SQL functions exposed to users for introspecting multirange properties
- Located in src/backend/utils/adt/multirangetypes.c:1603-1621