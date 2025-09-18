# multirange_upper_inc

## Location
src/backend/utils/adt/multirangetypes.c: 1584 - 1602

## Overview
Returns whether the upper bound of the last range in a multirange is inclusive (includes the boundary value).

## Definition


## Detailed Description
This function determines if the upper bound of the rightmost (last) range in a multirange is inclusive. It extracts the bounds of the last range in the multirange and returns the inclusive flag of the upper bound. If the multirange is empty, it returns false since there are no bounds to examine.

The function works by:
1. Checking if the multirange is empty - returns false if so
2. Getting the type cache for the multirange's base range type
3. Extracting the bounds of the last range (at index )
4. Returning the inclusive flag of the upper bound

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - : The input multirange to examine

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
- This function specifically examines the **last** range in the multirange, not all ranges
- Returns false for empty multiranges as they have no bounds
- Part of the multirange SQL functions exposed to users for introspecting multirange properties
- Located in src/backend/utils/adt/multirangetypes.c:1584-1602