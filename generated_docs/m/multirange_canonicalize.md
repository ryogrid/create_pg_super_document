# multirange_canonicalize

## Location
[src/backend/utils/adt/multirangetypes.c:477-547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L477-L547)

## Overview
Converts a list of arbitrary ranges into a sorted and merged list, processing an array of ranges to eliminate overlaps and adjacent ranges by merging them into a canonical form.

## Definition

```c
union_internal(rangetyp, lastRange, currentRange, false);
```
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
  - [range_compare](../r/range_compare.md) (comparison function for sorting)
  - RangeIsEmpty (to check if a range is empty)
  - [range_adjacent_internal](../r/range_adjacent_internal.md) (to check if ranges are adjacent)
  - [range_union_internal](../r/range_union_internal.md) (to merge ranges)
  - [range_before_internal](../r/range_before_internal.md) (to check range ordering)
- Called from (representative examples):
  - [make_multirange](make_multirange.md)

## Notes and Other Information
- The function assumes no input ranges are null, but empty ranges are acceptable and will be filtered out
- The return value may be less than the input count but never more, as ranges can only be merged, not split
- The sorting step is crucial for the merging logic to work correctly, ensuring that adjacent/overlapping ranges are processed in the correct order
- The function handles three cases during merging: adjacent ranges (merge), separated ranges (keep separate), and overlapping ranges (merge)

## Simplified Source

```c
static int32
multirange_canonicalize(TypeCacheEntry *rangetyp, int32 input_range_count,
                        RangeType **ranges)
{
    RangeType  *lastRange = NULL;
    RangeType  *currentRange;
    int32       i;
    int32       output_range_count = 0;

    // Sort ranges for proper merging order
    qsort_arg(ranges, input_range_count, sizeof(RangeType *), range_compare, rangetyp);

    // Merge overlapping and adjacent ranges
    for (i = 0; i < input_range_count; i++)
    {
        currentRange = ranges[i];

        // Skip empty ranges
        if (RangeIsEmpty(currentRange))
            continue;

        // First range - just add it
        if (lastRange == NULL)
        {
            ranges[output_range_count++] = lastRange = currentRange;
            continue;
        }

        // Check if ranges are adjacent (touching)
        if (range_adjacent_internal(rangetyp, lastRange, currentRange))
        {
            // Merge touching ranges
            ranges[output_range_count - 1] = lastRange =
                range_union_internal(rangetyp, lastRange, currentRange, false);
        }
        else if (range_before_internal(rangetyp, lastRange, currentRange))
        {
            // Gap between ranges - add as separate
            lastRange = ranges[output_range_count] = currentRange;
            output_range_count++;
        }
        else
        {
            // Overlapping ranges - merge them
            ranges[output_range_count - 1] = lastRange =
                range_union_internal(rangetyp, lastRange, currentRange, true);
        }
    }

    return output_range_count;
}
```