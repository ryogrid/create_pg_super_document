# range_minus_internal

## Location
src/backend/utils/adt/rangetypes.c: 993 - 1051

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