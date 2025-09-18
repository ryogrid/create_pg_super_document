# multirange_get_union_range

## Location
src/backend/utils/adt/multirangetypes.c: 802 - 825

## Overview
This function constructs a single range that represents the union (span) of all ranges within a multirange, extending from the lowest lower bound to the highest upper bound.

## Definition


## Detailed Description
The function creates a range that encompasses all ranges in the multirange by taking the lower bound from the first range and the upper bound from the last range. Since multiranges maintain their constituent ranges in sorted, non-overlapping order, this approach efficiently produces the minimal spanning range.

For empty multiranges, the function returns an empty range. For non-empty multiranges, it extracts bounds from the first (index 0) and last (index rangeCount-1) ranges, then constructs a new range with those bounds. The resulting range represents the total span covered by the multirange, potentially including gaps between the constituent ranges.

## Parameters / Member Variables
- : TypeCacheEntry containing type information for range construction
- : Pointer to the source MultirangeType structure

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeIsEmpty
  - [make_empty_range](make_empty_range.md)
  - [multirange_get_bounds](multirange_get_bounds.md)
  - [make_range](make_range.md)
  - RangeBound
- Called from (representative examples):
  - [multirange_gist_compress](multirange_gist_compress.md)
  - PG_RETURN_MULTIRANGE_P

## Notes and Other Information
- The function leverages the sorted nature of ranges within multiranges for efficiency
- Uses a temporary RangeBound variable to handle the unused bound from each extraction
- The resulting union range may span gaps between constituent ranges
- Commonly used in GiST index operations where a bounding range is needed
- Returns an empty range for empty multiranges rather than NULL
- The union range maintains the same type as the constituent ranges