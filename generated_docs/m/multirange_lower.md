# multirange_lower

## Location
src/backend/utils/adt/multirangetypes.c: 1507 - 1529

## Overview
Extracts the lower bound value from a multirange, returning the smallest value contained in the multirange or NULL if the multirange is empty or has an infinite lower bound.

## Definition
```c
Datum multirange_lower(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the lower bound value of a multirange type. It first checks if the multirange is empty, returning NULL if so. For non-empty multiranges, it obtains the bounds of the first range within the multirange (which represents the overall lower bound since multiranges are stored in sorted order) and returns the lower bound value if it's finite, or NULL if the lower bound is infinite.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention containing:
  - Arg 0: Input multirange (MultirangeType)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeIsEmpty
  - multirange_get_typcache
  - MultirangeTypeGetOid
  - multirange_get_bounds
  - PG_RETURN_DATUM
  - PG_RETURN_NULL
  - MultirangeType
  - RangeBound
- Called from (representative examples):
  - No direct references found (likely used via SQL function calls)

## Notes and Other Information
- Returns NULL for empty multiranges
- Returns NULL for multiranges with infinite lower bounds (unbounded below)
- The function uses index 0 when calling multirange_get_bounds, which retrieves the bounds of the first (lowest) range in the multirange
- Since multiranges maintain ranges in sorted, non-overlapping order, the lower bound of the first range represents the overall lower bound of the entire multirange
- The return type is Datum, allowing for any PostgreSQL data type that can serve as a range element