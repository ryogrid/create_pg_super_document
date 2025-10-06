# multirange_overright_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2215-2237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2215-L2237)

## Overview
Tests whether one multirange is positioned to the right of or overlapping with another multirange by comparing their leftmost bounds.

## Definition
```c
Datum multirange_overright_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
The `multirange_overright_multirange` function implements the "overright" or "not left of" operator (&>) for checking the positional relationship between two multiranges. It returns true if the first multirange is positioned to the right of or overlapping with the second multirange, determined by comparing their leftmost bounds.

Similar to `multirange_overright_range`, this function extracts the first range from each multirange and compares their lower bounds. The operation is essential for spatial queries and range-based operations involving multirange types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: `MultirangeType *mr1` - The first multirange to test
  - Argument 1: `MultirangeType *mr2` - The second multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange from function arguments
  - `MultirangeIsEmpty` - Check if multirange is empty
  - [multirange_get_typcache](multirange_get_typcache.md) - Get type cache for range type
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - [multirange_get_bounds](multirange_get_bounds.md) - Extract bounds from multirange
  - [range_cmp_bounds](../r/range_cmp_bounds.md) - Compare range bounds
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- Returns false immediately if either multirange is empty
- Uses the first range in each multirange for comparison (index 0)
- The comparison result >= 0 indicates the first multirange is "overright" of the second
- This function supports the &> operator in SQL queries between multirange types
- Located in src/backend/utils/adt/multirangetypes.c:2215-2237

## Simplified Source

```c
Datum multirange_overright_multirange(PG_FUNCTION_ARGS) {
    // Extract both multirange arguments
    MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);

    // Return false if either is empty
    if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
        PG_RETURN_BOOL(false);

    // Get type cache and bounds for both multiranges
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));
    RangeBound lower1, upper1, lower2, upper2;

    // Get bounds from leftmost range of each multirange
    multirange_get_bounds(typcache->rngtype, mr1, 0, &lower1, &upper1);
    multirange_get_bounds(typcache->rngtype, mr2, 0, &lower2, &upper2);

    // Compare lower bounds: mr1 overright mr2 if mr1's lower >= mr2's lower
    PG_RETURN_BOOL(range_cmp_bounds(typcache->rngtype, &lower1, &lower2) >= 0);
}
```