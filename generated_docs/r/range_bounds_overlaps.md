# range_bounds_overlaps

## Location
[src/backend/utils/adt/multirangetypes.c:858-877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L858-L877)

## Overview
Determines whether two range intervals overlap by comparing their boundary values directly, without requiring full range objects.

## Definition

```c
static bool
range_bounds_overlaps(TypeCacheEntry *typcache,
					  RangeBound *lower1, RangeBound *upper1,
					  RangeBound *lower2, RangeBound *upper2)
```
## Detailed Description
This function implements overlap detection logic for range intervals using boundary comparisons. It checks if two ranges defined by their lower and upper bounds have any overlapping portion. The function uses a two-way comparison approach: it checks if the lower bound of the first range falls within the second range, or if the lower bound of the second range falls within the first range. This is more efficient than constructing full range objects when only boundary information is needed for overlap detection.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing comparison functions and metadata for the range element type
- `*lower1`: Lower boundary of the first range
- `*upper1`: Upper boundary of the first range
- `*lower2`: Lower boundary of the second range
- `*upper2`: Upper boundary of the second range
## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bounds](range_cmp_bounds.md) (used for boundary comparisons)
  - RangeBound (boundary structure type)
- Called from (representative examples):
  - [multirange_overlaps_multirange_internal](../m/multirange_overlaps_multirange_internal.md)

## Notes and Other Information
- This is a static function, internal to the multirange implementation
- More efficient than range_overlaps_internal() when working with boundary values directly
- Uses range_cmp_bounds for proper boundary comparison that handles inclusive/exclusive bounds
- The overlap detection logic handles all boundary inclusion/exclusion cases correctly
- Located in 