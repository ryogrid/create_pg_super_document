# multirange_upper_inc

## Location
[src/backend/utils/adt/multirangetypes.c:1584-1602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1584-L1602)

## Overview
Returns whether the upper bound of the last range in a multirange is inclusive (includes the boundary value).

## Definition

```c
Datum
multirange_upper_inc(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum multirange_upper_inc(PG_FUNCTION_ARGS) {
    MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
    TypeCacheEntry *typcache;
    RangeBound lower, upper;

    // Return false for empty multiranges
    if (MultirangeIsEmpty(mr))
        PG_RETURN_BOOL(false);

    // Get type cache and extract bounds of last range
    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    multirange_get_bounds(typcache->rngtype, mr, mr->rangeCount - 1, &lower, &upper);

    // Return inclusivity of upper bound
    PG_RETURN_BOOL(upper.inclusive);
}
```