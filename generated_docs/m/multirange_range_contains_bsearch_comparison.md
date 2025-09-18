# multirange_range_contains_bsearch_comparison

## Location
src/backend/utils/adt/multirangetypes.c: 1774 - 1800

## Overview
A static comparison function used in binary search operations to determine if any range within a multirange contains a given key range.

## Definition
```c
static int multirange_range_contains_bsearch_comparison(TypeCacheEntry *typcache,
                                                       RangeBound *lower, RangeBound *upper,
                                                       void *key, bool *match)
```

## Detailed Description
This function serves as a specialized comparison function for binary search operations when checking if a multirange contains a specific range. It compares the bounds of ranges within a multirange against a key range to determine containment. The function implements a three-way comparison (-1, 0, 1) typical of binary search comparisons, with special handling for containment checking. When an overlapping range is found, it sets the `match` parameter to indicate whether the overlapping range fully contains the key range.

## Parameters / Member Variables
- `typcache`: TypeCacheEntry pointer containing type-specific information for range operations
- `lower`: RangeBound pointer representing the lower bound of the current range being examined
- `upper`: RangeBound pointer representing the upper bound of the current range being examined  
- `key`: void pointer to the key range being searched for (cast to RangeBound array)
- `match`: bool pointer set to true if the current range contains the key range, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - `[range_cmp_bounds](../r/range_cmp_bounds.md)` - Compare range bounds for ordering
  - `[range_bounds_contains](../r/range_bounds_contains.md)` - Check if one range fully contains another
  - `RangeBound` - Structure representing range boundaries
- Called from (representative examples):
  - `[multirange_contains_range_internal](multirange_contains_range_internal.md)` - Uses this function in binary search for containment checking

## Notes and Other Information
- This is a static function, only accessible within the multirangetypes.c file
- Designed specifically for use with binary search algorithms (bsearch family functions)
- The function exploits the property that multiranges contain only non-overlapping ranges to optimize the search
- Returns -1 if key is to the left, 1 if key is to the right, and 0 if there is overlap
- When returning 0 (overlap found), the search stops and the `match` parameter indicates actual containment