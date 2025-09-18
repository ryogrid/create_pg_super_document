# range_cmp_bounds

## Location
[src/backend/utils/adt/rangetypes.c:2016-2089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2016-L2089)

## Overview
Compares two range boundary points and returns a comparison result indicating their relative ordering, handling both finite and infinite bounds with inclusive/exclusive semantics.

## Definition


## Detailed Description
The `range_cmp_bounds` function performs comprehensive comparison between two range boundary points, returning -1, 0, or 1 to indicate whether the first bound is less than, equal to, or greater than the second bound. The function handles complex boundary semantics including infinite bounds (representing minus/plus infinity), inclusive vs exclusive bounds, and proper ordering for both lower and upper boundaries. For finite bounds with equal values, the inclusiveness and boundary type (upper vs lower) determine the final comparison result, ensuring correct range operations and ordering.

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison function information for the range element type
- `b1`: First range boundary to compare  
- `b2`: Second range boundary to compare

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - [range_eq_internal](range_eq_internal.md)
  - [range_overlaps_internal](range_overlaps_internal.md)  
  - [range_before_internal](range_before_internal.md)
  - [range_after_internal](range_after_internal.md)
  - [range_cmp](range_cmp.md)
  - [range_compare](range_compare.md)
  - [range_contains_internal](range_contains_internal.md)
  - [range_gist_penalty](range_gist_penalty.md)
  - [multirange_cmp](../m/multirange_cmp.md)

## Notes and Other Information
- Infinite bounds are handled specially: lower infinite bounds represent minus infinity, upper infinite bounds represent plus infinity
- For equal finite values, exclusive bounds are considered "just greater than" (lower) or "just less than" (upper) the held value
- Two boundaries compare equal only when both are inclusive with the same finite value, regardless of being upper or lower bounds
- The function is fundamental to all range comparison operations and is heavily used throughout the range type system
- Comparison uses the range type's configured comparison function from the type cache