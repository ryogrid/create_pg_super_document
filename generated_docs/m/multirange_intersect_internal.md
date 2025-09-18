# multirange_intersect_internal

## Location
src/backend/utils/adt/multirangetypes.c: 1260 - 1339

## Overview
The core internal function that implements the intersection algorithm for two multiranges, computing overlapping portions between all ranges in both multiranges.

## Definition
MultirangeType *multirange_intersect_internal(Oid mltrngtypoid, TypeCacheEntry *rangetyp, int32 range_count1, RangeType **ranges1, int32 range_count2, RangeType **ranges2)

## Detailed Description
This function performs the actual intersection computation between two multiranges. It implements a parallel iteration algorithm that walks through both sorted arrays of ranges, finding all overlapping portions. The algorithm is similar to a merge operation but focuses on intersections rather than unions.

The function handles edge cases such as empty multiranges and optimizes memory allocation by estimating the worst-case scenario for the result size. It uses a two-pointer approach to efficiently traverse both range arrays, discarding non-overlapping ranges and collecting intersecting portions.

## Parameters / Member Variables
- `mltrngtypoid`: OID of the multirange type for the result
- `rangetyp`: Type cache entry for the underlying range type
- `range_count1`: Number of ranges in the first multirange
- `ranges1`: Array of ranges from the first multirange
- `range_count2`: Number of ranges in the second multirange
- `ranges2`: Array of ranges from the second multirange

## Dependencies
- Functions called/Symbols referenced:
  - make_multirange
  - range_before_internal
  - range_overlaps_internal  
  - range_intersect_internal
  - range_overleft_internal
  - palloc0
- Called from:
  - multirange_intersect
  - multirange_intersect_agg_transfn

## Notes and Other Information
- Uses a parallel iteration algorithm similar to merge sort for efficiency
- Allocates memory for worst-case scenario: range_count1 + range_count2 ranges
- Returns empty multirange if either input has zero ranges
- The algorithm maintains sorted order in the result
- Handles complex overlapping patterns efficiently with O(n+m) time complexity
- Located in src/backend/utils/adt/multirangetypes.c:1260-1339