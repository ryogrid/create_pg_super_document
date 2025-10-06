# multirange_overleft_range

## Location
[src/backend/utils/adt/multirangetypes.c:2108-2132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2108-L2132)

## Overview
Checks if a multirange does not extend to the right of a range (PostgreSQL "&<" operator for multirange-range comparison).

## Definition

```c
Datum
multirange_overleft_range(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the "overleft" or "does not extend to right of" operator (&<) between a multirange type and a range type. It determines whether the given multirange does not extend to the right of the given range by comparing their upper bounds. Unlike `range_overleft_multirange`, this function takes a multirange as the first argument and a range as the second argument.

The function extracts the bounds of both the multirange and range, then compares their upper bounds. It returns true if the multirange's upper bound is less than or equal to the range's upper bound, meaning the multirange does not extend beyond the rightmost point of the range.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access:
  - Argument 0: `MultirangeType *mr` - The multirange to compare
  - Argument 1: `RangeType *r` - The range to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange argument
  - `PG_GETARG_RANGE_P` - Extract range argument
  - `MultirangeIsEmpty` - Check if multirange is empty
  - `RangeIsEmpty` - Check if range is empty
  - [multirange_get_typcache](multirange_get_typcache.md) - Get type cache information
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - [multirange_get_bounds](multirange_get_bounds.md) - Extract bounds from multirange
  - [range_deserialize](../r/range_deserialize.md) - Extract bounds from range
  - [range_cmp_bounds](../r/range_cmp_bounds.md) - Compare range bounds
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called using the &< operator in SQL
- Returns false if either the multirange or range is empty
- The function directly implements the comparison logic without delegating to an internal function (unlike `range_overleft_multirange`)
- Uses the last range in the multirange (at index `rangeCount - 1`) to get the rightmost bounds
- Part of PostgreSQL's range and multirange type system for complex range operations
- Located in `src/backend/utils/adt/multirangetypes.c:2108-2132`

## Simplified Source

```c
Datum multirange_overleft_range(PG_FUNCTION_ARGS) {
    // Extract multirange and range arguments
    MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
    RangeType *r = PG_GETARG_RANGE_P(1);

    // Return false if either is empty
    if (MultirangeIsEmpty(mr) || RangeIsEmpty(r))
        PG_RETURN_BOOL(false);

    // Get type cache and bounds
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    RangeBound mr_lower, mr_upper, r_lower, r_upper;
    bool empty;

    // Get bounds from rightmost range in multirange and from the range
    multirange_get_bounds(typcache->rngtype, mr, mr->rangeCount - 1, &mr_lower, &mr_upper);
    range_deserialize(typcache->rngtype, r, &r_lower, &r_upper, &empty);

    // Compare upper bounds: multirange overleft if its upper <= range's upper
    PG_RETURN_BOOL(range_cmp_bounds(typcache->rngtype, &mr_upper, &r_upper) <= 0);
}
```