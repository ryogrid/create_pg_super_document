# multirange_canonicalize

## Location
src/backend/utils/adt/multirangetypes.c: 477 - 547

## Overview
Converts a list of arbitrary ranges into a sorted and merged list, processing an array of ranges to eliminate overlaps and adjacent ranges by merging them into a canonical form.

## Definition


## Detailed Description
This function takes an array of ranges and transforms it into a canonical form by:
1. Sorting the ranges using the range comparison function
2. Merging overlapping or adjacent ranges
3. Removing empty ranges from consideration
4. Returning the final count of ranges after canonicalization

The function modifies the input array in-place, potentially reducing the number of valid ranges. It ensures that the resulting multirange has no overlapping or touching ranges, which is essential for the proper representation of multirange types in PostgreSQL.

## Parameters / Member Variables
- : TypeCacheEntry pointer containing type information for the range type being processed
- : The number of ranges in the input array
- : Array of RangeType pointers that will be modified in-place to contain the canonicalized ranges

## Dependencies
- Functions called/Symbols referenced:
  - qsort_arg (for sorting ranges)
  - range_compare (comparison function for sorting)
  - RangeIsEmpty (to check if a range is empty)
  - range_adjacent_internal (to check if ranges are adjacent)
  - range_union_internal (to merge ranges)
  - range_before_internal (to check range ordering)
- Called from (representative examples):
  - make_multirange

## Notes and Other Information
- The function assumes no input ranges are null, but empty ranges are acceptable and will be filtered out
- The return value may be less than the input count but never more, as ranges can only be merged, not split
- The sorting step is crucial for the merging logic to work correctly, ensuring that adjacent/overlapping ranges are processed in the correct order
- The function handles three cases during merging: adjacent ranges (merge), separated ranges (keep separate), and overlapping ranges (merge)