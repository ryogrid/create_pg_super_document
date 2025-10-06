# multirange_minus_internal

## Location
[src/backend/utils/adt/multirangetypes.c:1144-1229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1144-L1229)

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

## Simplified Source

```c
MultirangeType *
multirange_minus_internal(Oid mltrngtypoid, TypeCacheEntry *rangetyp,
                         int32 range_count1, RangeType **ranges1,
                         int32 range_count2, RangeType **ranges2)
{
    // Allocate worst-case result array (each range could be split)
    RangeType **result_ranges = palloc0((range_count1 + range_count2) * sizeof(RangeType *));
    int32 result_count = 0;

    // Process each range from first multirange (minuend)
    RangeType *current_subtrahend = ranges2[0];
    int32 subtrahend_index = 0;

    for (int32 i = 0; i < range_count1; i++) {
        RangeType *current_range = ranges1[i];

        // Skip subtrahend ranges that come before current range
        while (current_subtrahend != NULL &&
               range_before_internal(rangetyp, current_subtrahend, current_range)) {
            current_subtrahend = (++subtrahend_index >= range_count2) ?
                                NULL : ranges2[subtrahend_index];
        }

        // Subtract overlapping ranges from current range
        while (current_subtrahend != NULL) {
            if (range_split_internal(rangetyp, current_range, current_subtrahend,
                                   &result_ranges[result_count], &current_range)) {
                // Range split in middle - keep first part, continue with remainder
                result_count++;
                current_subtrahend = (++subtrahend_index >= range_count2) ?
                                   NULL : ranges2[subtrahend_index];
            }
            else if (range_overlaps_internal(rangetyp, current_range, current_subtrahend)) {
                // Partial overlap - subtract and update current range
                current_range = range_minus_internal(rangetyp, current_range, current_subtrahend);

                // Decide whether to advance to next subtrahend or keep current one
                if (RangeIsEmpty(current_range) ||
                    range_before_internal(rangetyp, current_range, current_subtrahend))
                    break;  // Current range exhausted or passed
                else
                    current_subtrahend = (++subtrahend_index >= range_count2) ?
                                       NULL : ranges2[subtrahend_index];
            }
            else {
                // No overlap - current and future subtrahends are past current range
                break;
            }
        }

        // Add whatever remains of current range to result
        result_ranges[result_count++] = current_range;
    }

    // Create final multirange (handles empty range removal and normalization)
    return make_multirange(mltrngtypoid, rangetyp, result_count, result_ranges);
}
```

This function iterates through the first multirange, subtracting overlapping ranges from the second multirange, handling splits and partial overlaps to compute the difference.