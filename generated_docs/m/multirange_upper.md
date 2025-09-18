# multirange_upper

## Location
src/backend/utils/adt/multirangetypes.c: 1530 - 1555

## Overview
Extracts the upper bound value from a multirange, returning the largest value contained in the multirange or NULL if the multirange is empty or has an infinite upper bound.

## Definition
```c
Datum multirange_upper(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the upper bound value of a multirange type. It first checks if the multirange is empty, returning NULL if so. For non-empty multiranges, it obtains the bounds of the last range within the multirange (using mr->rangeCount - 1 as the index) which represents the overall upper bound since multiranges maintain ranges in sorted order. It returns the upper bound value if it's finite, or NULL if the upper bound is infinite.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention containing:
  - Arg 0: Input multirange (MultirangeType)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeIsEmpty
  - [multirange_get_typcache](multirange_get_typcache.md)
  - MultirangeTypeGetOid
  - [multirange_get_bounds](multirange_get_bounds.md)
  - PG_RETURN_DATUM
  - PG_RETURN_NULL
  - MultirangeType
  - RangeBound
- Called from (representative examples):
  - No direct references found (likely used via SQL function calls)

## Notes and Other Information
- Returns NULL for empty multiranges
- Returns NULL for multiranges with infinite upper bounds (unbounded above)
- The function uses mr->rangeCount - 1 when calling multirange_get_bounds, which retrieves the bounds of the last (highest) range in the multirange
- Since multiranges maintain ranges in sorted, non-overlapping order, the upper bound of the last range represents the overall upper bound of the entire multirange
- Complementary to multirange_lower function, providing access to the opposite boundary
- The return type is Datum, allowing for any PostgreSQL data type that can serve as a range element