# range_overleft_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2096-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2096-L2107)

## Overview
Checks if a range does not extend to the right of a multirange (PostgreSQL "&<" operator for range-multirange comparison).

## Definition

```c
Datum
range_overleft_multirange(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the "overleft" or "does not extend to right of" operator (&<) between a range type and a multirange type. It determines whether the given range does not extend to the right of the given multirange by comparing their bounds. The function serves as a PostgreSQL function wrapper that extracts arguments, retrieves type information, and delegates the actual comparison logic to `range_overleft_multirange_internal`.

The overleft operator returns true if the range's upper bound is less than or equal to the multirange's upper bound, meaning the range does not extend beyond the rightmost point of the multirange.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access:
  - Argument 0: `RangeType *r` - The range to compare
  - Argument 1: `MultirangeType *mr` - The multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - Extract range argument
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange argument  
  - [multirange_get_typcache](../m/multirange_get_typcache.md) - Get type cache information
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - [range_overleft_multirange_internal](range_overleft_multirange_internal.md) - Perform actual comparison logic
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called using the &< operator in SQL
- Returns false if either the range or multirange is empty
- The actual comparison logic is implemented in `range_overleft_multirange_internal`
- Part of PostgreSQL's range and multirange type system introduced to support complex range operations
- Located in `src/backend/utils/adt/multirangetypes.c:2096-2107`

## Simplified Source

```c
Datum range_overleft_multirange(PG_FUNCTION_ARGS) {
    // Extract range and multirange arguments
    RangeType *r = PG_GETARG_RANGE_P(0);
    MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);

    // Get type cache for multirange
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    // Delegate to internal comparison function
    PG_RETURN_BOOL(range_overleft_multirange_internal(typcache->rngtype, r, mr));
}
```