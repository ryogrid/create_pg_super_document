# range_bounds_contains

## Location
src/backend/utils/adt/multirangetypes.c: 878 - 897

## Overview
Determines whether the first range completely contains the second range by comparing their boundary values directly.

## Definition


## Detailed Description
This function implements containment detection logic for range intervals using direct boundary comparisons. It checks if the first range (defined by lower1, upper1) completely contains the second range (defined by lower2, upper2). A range contains another if its lower bound is less than or equal to the contained range's lower bound, and its upper bound is greater than or equal to the contained range's upper bound. This provides an efficient way to test containment without constructing full range objects.

## Parameters / Member Variables
- : Type cache entry containing comparison functions and metadata for the range element type
- : Lower boundary of the containing range
- : Upper boundary of the containing range
- : Lower boundary of the potentially contained range
- : Upper boundary of the potentially contained range

## Dependencies
- Functions called/Symbols referenced:
  - range_cmp_bounds (used for boundary comparisons)
  - RangeBound (boundary structure type)
- Called from (representative examples):
  - multirange_range_contains_bsearch_comparison
  - range_contains_multirange_internal
  - multirange_contains_multirange_internal

## Notes and Other Information
- This is a static function, internal to the multirange implementation
- More efficient than range_contains_internal() when working with boundary values directly
- Uses range_cmp_bounds for proper boundary comparison that handles inclusive/exclusive bounds
- The containment logic properly handles all boundary inclusion/exclusion combinations
- Widely used in multirange containment operations and binary search comparisons
- Located in 