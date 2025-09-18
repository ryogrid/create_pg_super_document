# multirange_intersect_internal

## Location
[src/backend/utils/adt/multirangetypes.c:1260-1339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1260-L1339)

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
  - [make_multirange](make_multirange.md)
  - [range_before_internal](../r/range_before_internal.md)
  - [range_overlaps_internal](../r/range_overlaps_internal.md)  
  - [range_intersect_internal](../r/range_intersect_internal.md)
  - [range_overleft_internal](../r/range_overleft_internal.md)
  - [palloc0](../p/palloc0.md)
- Called from:
  - [multirange_intersect](multirange_intersect.md)
  - [multirange_intersect_agg_transfn](multirange_intersect_agg_transfn.md)

## Notes and Other Information
- Uses a parallel iteration algorithm similar to merge sort for efficiency
- Allocates memory for worst-case scenario: range_count1 + range_count2 ranges
- Returns empty multirange if either input has zero ranges
- The algorithm maintains sorted order in the result
- Handles complex overlapping patterns efficiently with O(n+m) time complexity
- Located in src/backend/utils/adt/multirangetypes.c:1260-1339