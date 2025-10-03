# range_bounds_contains

## Location
[src/backend/utils/adt/multirangetypes.c:878-897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L878-L897)

## Overview
Determines whether the first range completely contains the second range by comparing their boundary values directly.

## Definition

```c
static bool
range_bounds_contains(TypeCacheEntry *typcache,
					  RangeBound *lower1, RangeBound *upper1,
					  RangeBound *lower2, RangeBound *upper2)
```
## Detailed Description
This function implements containment detection logic for range intervals using direct boundary comparisons. It checks if the first range (defined by lower1, upper1) completely contains the second range (defined by lower2, upper2). A range contains another if its lower bound is less than or equal to the contained range's lower bound, and its upper bound is greater than or equal to the contained range's upper bound. This provides an efficient way to test containment without constructing full range objects.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing comparison functions and metadata for the range element type
- `*lower1`: Lower boundary of the containing range
- `*upper1`: Upper boundary of the containing range
- `*lower2`: Lower boundary of the potentially contained range
- `*upper2`: Upper boundary of the potentially contained range
## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bounds](range_cmp_bounds.md) (used for boundary comparisons)
  - RangeBound (boundary structure type)
- Called from (representative examples):
  - [multirange_range_contains_bsearch_comparison](../m/multirange_range_contains_bsearch_comparison.md)
  - [range_contains_multirange_internal](range_contains_multirange_internal.md)
  - [multirange_contains_multirange_internal](../m/multirange_contains_multirange_internal.md)

## Notes and Other Information
- This is a static function, internal to the multirange implementation
- More efficient than range_contains_internal() when working with boundary values directly
- Uses range_cmp_bounds for proper boundary comparison that handles inclusive/exclusive bounds
- The containment logic properly handles all boundary inclusion/exclusion combinations
- Widely used in multirange containment operations and binary search comparisons
- Located in 