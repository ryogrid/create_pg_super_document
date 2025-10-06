# range_minus_internal

## Location
[src/backend/utils/adt/rangetypes.c:993-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L993-L1051)

## Overview
The range_minus_internal function performs the core logic for range set difference operations, implementing complex boundary condition checks to compute A - B for two ranges.

## Definition
RangeType *range_minus_internal(TypeCacheEntry *typcache, RangeType *r1, RangeType *r2)

## Detailed Description
This internal function implements the mathematical set difference operation between two ranges. It handles all possible geometric relationships between the ranges (disjoint, overlapping, containing, etc.) and returns the appropriate result. The function performs extensive boundary comparisons to determine the correct output range. If the result would not be contiguous (i.e., would require multiple separate ranges), it throws an error since PostgreSQL ranges must be contiguous intervals.

## Parameters / Member Variables
- typcache: Type cache entry containing comparison functions for the range element type
- r1: First range (minuend) - the range from which elements are subtracted
- r2: Second range (subtrahend) - the range of elements to subtract

## Dependencies
- Functions called/Symbols referenced:
  - RangeBound
  - [range_deserialize](range_deserialize.md)
  - [range_cmp_bounds](range_cmp_bounds.md)
  - [make_empty_range](../m/make_empty_range.md)
  - [make_range](../m/make_range.md)
- Called from (representative examples):
  - [range_minus](range_minus.md)

## Notes and Other Information
- Returns r1 unchanged if either range is empty
- Throws DATA_EXCEPTION error if result would not be contiguous
- Returns r1 unchanged if ranges are disjoint
- Returns empty range if r1 is completely contained within r2
- Handles partial overlap cases by adjusting boundary inclusivity
- Uses comprehensive boundary comparison logic with cmp_l1l2, cmp_l1u2, cmp_u1l2, cmp_u1u2 variables
- Critical component of PostgreSQL range type arithmetic operations

## Simplified Source

```c
RangeType *range_minus_internal(TypeCacheEntry *typcache, RangeType *r1, RangeType *r2) {
    RangeBound lower1, lower2, upper1, upper2;
    bool empty1, empty2;

    // Extract boundaries from both ranges
    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    // If either range is empty, return r1 unchanged
    if (empty1 || empty2)
        return r1;

    // Compare all boundary combinations
    int cmp_l1l2 = range_cmp_bounds(typcache, &lower1, &lower2);
    int cmp_l1u2 = range_cmp_bounds(typcache, &lower1, &upper2);
    int cmp_u1l2 = range_cmp_bounds(typcache, &upper1, &lower2);
    int cmp_u1u2 = range_cmp_bounds(typcache, &upper1, &upper2);

    // Error if result would be non-contiguous (r2 in middle of r1)
    if (cmp_l1l2 < 0 && cmp_u1u2 > 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("result of range difference would not be contiguous")));

    // Ranges are disjoint - return r1 unchanged
    if (cmp_l1u2 > 0 || cmp_u1l2 < 0)
        return r1;

    // r1 completely contained in r2 - return empty range
    if (cmp_l1l2 >= 0 && cmp_u1u2 <= 0)
        return make_empty_range(typcache);

    // r2 overlaps right side of r1 - return left portion
    if (cmp_l1l2 <= 0 && cmp_u1l2 >= 0 && cmp_u1u2 <= 0) {
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false;
        return make_range(typcache, &lower1, &lower2, false, NULL);
    }

    // r2 overlaps left side of r1 - return right portion
    if (cmp_l1l2 >= 0 && cmp_u1u2 >= 0 && cmp_l1u2 <= 0) {
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true;
        return make_range(typcache, &upper2, &upper1, false, NULL);
    }

    elog(ERROR, "unexpected case in range_minus");
    return NULL;
}
```