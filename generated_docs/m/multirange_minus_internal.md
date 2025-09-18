# multirange_minus_internal

## Location
src/backend/utils/adt/multirangetypes.c: 1144 - 1229

## Overview
Implements the core logic for subtracting one multirange from another, handling the complex range splitting and overlap resolution required for multirange difference operations.

## Definition
```c
MultirangeType *multirange_minus_internal(Oid mltrngtypoid, TypeCacheEntry *rangetyp,
                                         int32 range_count1, RangeType **ranges1,
                                         int32 range_count2, RangeType **ranges2)
```

## Detailed Description
This function performs the actual computation for multirange subtraction (A - B). It iterates through ranges in the first multirange (minuend) and progressively subtracts overlapping ranges from the second multirange (subtrahend). The algorithm maintains parallel progress through both sorted range arrays, similar to multirange_overlaps_multirange_internal. For each range in the first multirange, it processes all overlapping ranges from the second multirange, potentially splitting ranges when partial overlaps occur. The function handles three main cases: ranges that split the target range in the middle, ranges that partially overlap, and ranges that are completely disjoint.

## Parameters / Member Variables
- `mltrngtypoid`: OID of the multirange type being operated on
- `rangetyp`: TypeCacheEntry for the underlying range type
- `range_count1`: Number of ranges in the first multirange (minuend)
- `ranges1`: Array of ranges from the first multirange
- `range_count2`: Number of ranges in the second multirange (subtrahend)
- `ranges2`: Array of ranges from the second multirange

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [range_before_internal](../r/range_before_internal.md)
  - [range_split_internal](../r/range_split_internal.md)
  - [range_overlaps_internal](../r/range_overlaps_internal.md)
  - [range_minus_internal](../r/range_minus_internal.md)
  - RangeIsEmpty
  - [make_multirange](make_multirange.md)
- Called from (representative examples):
  - [multirange_minus](multirange_minus.md)
  - PG_RETURN_MULTIRANGE_P (via macro expansion)

## Notes and Other Information
- Implements sophisticated range arithmetic with splitting and merging logic
- Allocates worst-case memory assuming every range interaction results in a split
- Uses parallel iteration through both sorted range arrays for efficiency
- Handles partial overlaps by splitting ranges and managing remainder pieces
- Empty ranges are automatically filtered out by make_multirange
- The algorithm maintains sorted order throughout the computation
- Located in src/backend/utils/adt/multirangetypes.c:1144-1229