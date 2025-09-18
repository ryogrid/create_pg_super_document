# multirange_upper_inf

## Location
src/backend/utils/adt/multirangetypes.c: 1622 - 1644

## Overview
Returns whether the upper bound of the last range in a multirange is infinite (unbounded).

## Definition
```c
Datum multirange_upper_inf(PG_FUNCTION_ARGS)
```

## Detailed Description
This function determines if the upper bound of the rightmost (last) range in a multirange is infinite. It extracts the bounds of the last range in the multirange and returns the infinite flag of the upper bound. If the multirange is empty, it returns false since there are no bounds to examine.

The function works by:
1. Checking if the multirange is empty - returns false if so
2. Getting the type cache for the multirange's base range type
3. Extracting the bounds of the last range (at index `rangeCount - 1`)
4. Returning the infinite flag of the upper bound

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `mr`: The input multirange to examine

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeType
  - PG_GETARG_MULTIRANGE_P
  - RangeBound
  - MultirangeIsEmpty
  - multirange_get_typcache
  - MultirangeTypeGetOid
  - multirange_get_bounds
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function specifically examines the **last** range in the multirange, not all ranges
- Returns false for empty multiranges as they have no bounds
- An infinite upper bound means the range extends indefinitely in the positive direction
- Part of the multirange SQL functions exposed to users for introspecting multirange properties
- Located in src/backend/utils/adt/multirangetypes.c:1622-1644