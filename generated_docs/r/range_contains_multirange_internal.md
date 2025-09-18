# range_contains_multirange_internal

## Location
src/backend/utils/adt/multirangetypes.c: 1829 - 1863

## Overview
An internal function that tests whether a single range completely contains an entire multirange by checking if the range contains the multirange's union.

## Definition
```c
bool range_contains_multirange_internal(TypeCacheEntry *rangetyp,
                                       const RangeType *r,
                                       const MultirangeType *mr)
```

## Detailed Description
This function determines if a single range completely contains a multirange. It uses an efficient algorithm based on the principle that a range contains a multirange if and only if it contains the union of all ranges in the multirange. Rather than computing the actual union, it simply checks if the containing range encompasses the overall bounds of the multirange (from the lower bound of the first range to the upper bound of the last range). The function handles edge cases where empty multiranges are contained by any range, while empty ranges contain no non-empty multiranges.

## Parameters / Member Variables
- `rangetyp`: TypeCacheEntry pointer containing type-specific information for range operations
- `r`: const RangeType pointer to the range that may contain the multirange
- `mr`: const MultirangeType pointer to the multirange being tested for containment

## Dependencies
- Functions called/Symbols referenced:
  - `MultirangeIsEmpty` - Check if a multirange is empty
  - `RangeIsEmpty` - Check if a range is empty
  - [range_deserialize](range_deserialize.md) - Extract bounds from range structure
  - [multirange_get_bounds](../m/multirange_get_bounds.md) - Get bounds from specific ranges within multirange
  - [range_bounds_contains](range_bounds_contains.md) - Check if one set of bounds contains another
  - `RangeBound` - Structure for representing range boundaries
- Called from (representative examples):
  - [range_contains_multirange](range_contains_multirange.md) - Public SQL function wrapper
  - [multirange_contained_by_range](../m/multirange_contained_by_range.md) - Inverse containment operation
  - [range_gist_consistent_int_multirange](range_gist_consistent_int_multirange.md) - GiST index consistency checking (multiple locations)

## Notes and Other Information
- Uses an optimization: instead of computing the full union, it only checks the overall bounds
- Leverages the fact that multiranges store ranges in sorted, non-overlapping order
- The algorithm works because if a range contains the overall bounds, it must contain all individual ranges within those bounds
- Empty multiranges are special-cased to always be contained (mathematical convention)
- Part of PostgreSQL's comprehensive range type system supporting complex containment operations