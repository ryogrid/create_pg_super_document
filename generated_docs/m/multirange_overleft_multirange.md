# multirange_overleft_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2133-2157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2133-L2157)

## Overview
Checks if the first multirange does not extend to the right of the second multirange (PostgreSQL "&<" operator for multirange-multirange comparison).

## Definition


## Detailed Description
This function implements the "overleft" or "does not extend to right of" operator (&<) between two multirange types. It determines whether the first multirange does not extend to the right of the second multirange by comparing their rightmost upper bounds. 

The function extracts the bounds from the last range of each multirange (which represents the rightmost range) and compares their upper bounds. It returns true if the first multirange's upper bound is less than or equal to the second multirange's upper bound, meaning the first multirange does not extend beyond the rightmost point of the second multirange.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access:
  - Argument 0: `MultirangeType *mr1` - The first multirange to compare
  - Argument 1: `MultirangeType *mr2` - The second multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange arguments (used twice)
  - `MultirangeIsEmpty` - Check if multiranges are empty
  - [multirange_get_typcache](multirange_get_typcache.md) - Get type cache information
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - [multirange_get_bounds](multirange_get_bounds.md) - Extract bounds from multiranges (used twice)
  - [range_cmp_bounds](../r/range_cmp_bounds.md) - Compare range bounds
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called using the &< operator in SQL between two multiranges
- Returns false if either multirange is empty
- Uses the last range in each multirange (at index `rangeCount - 1`) to get the rightmost bounds for comparison
- The function directly implements the comparison logic without delegating to an internal function
- Both multiranges must be of the same base range type for the comparison to be valid
- Part of PostgreSQL's range and multirange type system for complex range operations
- Located in `src/backend/utils/adt/multirangetypes.c:2133-2157`