# multirange_overlaps_multirange_internal

## Location
src/backend/utils/adt/multirangetypes.c: 2015 - 2072

## Overview
Internal function that efficiently determines if two multiranges have any overlapping ranges using an optimized two-pointer algorithm.

## Definition
```c
bool multirange_overlaps_multirange_internal(TypeCacheEntry *rangetyp,
                                            const MultirangeType *mr1,
                                            const MultirangeType *mr2)
```

## Detailed Description
This function implements an efficient algorithm to check if two multiranges overlap by comparing their constituent ranges. Instead of using a naive O(n²) approach that compares every range in mr1 with every range in mr2, it uses a sophisticated two-pointer technique that takes advantage of the fact that ranges within each multirange are sorted and non-overlapping.

The algorithm maintains two indices (i1, i2) and advances through both multiranges simultaneously. For each range r2 in mr2, it discards ranges r1 that are entirely to the left of r2 (r1 << r2), then checks if the current r1 overlaps with r2. If an overlap is found, it returns true immediately. If no overlap is found for r2, it moves to the next range in mr2.

This approach ensures that each range is examined at most once, achieving O(n + m) time complexity where n and m are the number of ranges in each multirange.

## Parameters / Member Variables
- `rangetyp`: Type cache entry containing comparison functions and metadata for the range element type
- `mr1`: First multirange to check for overlaps
- `mr2`: Second multirange to check for overlaps

## Dependencies
- Functions called/Symbols referenced:
  - `MultirangeIsEmpty` - Check if a multirange contains no ranges
  - `multirange_get_bounds` - Extract lower and upper bounds from a specific range within a multirange
  - `range_cmp_bounds` - Compare two range bounds using the type's comparison function
  - `range_bounds_overlaps` - Check if two pairs of bounds represent overlapping ranges
  - `RangeBound` - Structure representing range boundary points
- Called from (representative examples):
  - `multirange_overlaps_multirange` - The SQL operator wrapper function
  - Multirange overlap operations in queries and expressions

## Notes and Other Information
- Returns false immediately if either multirange is empty, following PostgreSQL's range semantics
- The comment explains the optimization strategy compared to range_overlaps_multirange where single ranges are involved
- Uses a "discard while" loop to skip ranges in mr1 that are entirely before the current range in mr2
- The algorithm is similar to merge operations in merge sort, leveraging the sorted nature of ranges within multiranges
- Located in `src/backend/utils/adt/multirangetypes.c` at lines 2015-2072
- Critical for the performance of multirange overlap operations in PostgreSQL