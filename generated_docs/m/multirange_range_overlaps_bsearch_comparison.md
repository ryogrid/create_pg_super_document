# multirange_range_overlaps_bsearch_comparison

## Location
src/backend/utils/adt/multirangetypes.c: 1976 - 1992

## Overview
Static comparison function used for binary search to determine if any range within a multirange overlaps with a given key range.

## Definition
```c
static int multirange_range_overlaps_bsearch_comparison(TypeCacheEntry *typcache,
                                                      RangeBound *lower, RangeBound *upper,
                                                      void *key, bool *match)
```

## Detailed Description
This function serves as a comparison callback for binary search operations when checking if a target range overlaps with any range in a multirange. It compares the bounds of a multirange's constituent range (lower, upper) against the bounds of a key range passed through the `key` parameter.

The function implements the three-way comparison logic required for binary search:
- Returns -1 if the key range is entirely before the current range
- Returns 1 if the key range is entirely after the current range  
- Returns 0 and sets `*match = true` if the ranges overlap

This enables efficient O(log n) searching through the sorted ranges in a multirange to find overlaps.

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison functions for the range element type
- `lower`: Lower bound of the current multirange's constituent range being compared
- `upper`: Upper bound of the current multirange's constituent range being compared
- `key`: Void pointer to an array of two RangeBound structures representing the key range's bounds
- `match`: Output parameter set to true if the ranges overlap

## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bounds](../r/range_cmp_bounds.md) - Compare range bounds using the type's comparison function
  - `RangeBound` - Structure representing range boundary values
- Called from (representative examples):
  - [range_overlaps_multirange_internal](../r/range_overlaps_multirange_internal.md) - Uses this function with `multirange_bsearch_match`

## Notes and Other Information
- This is a static (internal) function not exposed outside the compilation unit
- The key parameter is cast to `(RangeBound *) key` to access the lower bound and `(RangeBound *) key + 1` for the upper bound
- The function assumes the key contains exactly two consecutive RangeBound structures
- Located in `src/backend/utils/adt/multirangetypes.c` at lines 1976-1992
- Essential for the efficient implementation of range-to-multirange overlap checking