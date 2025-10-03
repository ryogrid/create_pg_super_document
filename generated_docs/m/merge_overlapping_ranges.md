# merge_overlapping_ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:1231-1304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1231-L1304)

## Overview
Merges overlapping ranges in a sorted array of ExpandedRange structures by detecting overlaps and combining them into single expanded ranges.

## Definition
static int merge_overlapping_ranges(FmgrInfo *cmp, Oid colloid, ExpandedRange *eranges, int neranges)

## Detailed Description
This function processes a sorted array of ExpandedRange structures to merge overlapping ranges. It assumes the input is pre-sorted by minval and then maxval. The algorithm works by:

1. **Overlap Detection**: For each consecutive pair of ranges, compares the maxval of the current range with the minval of the next range. If maxval ≥ minval, the ranges overlap.

2. **Range Merging**: When overlap is detected, the function:
   - Keeps the minval of the first range (since ranges are sorted)
   - Compares the maxval of both ranges and keeps the larger one
   - Marks the merged range as not collapsed
   - Removes the second range by shifting subsequent ranges

3. **Iterative Processing**: After each merge, the algorithm re-examines the same position since the newly merged range might overlap with additional subsequent ranges.

The function operates in-place to minimize memory usage and returns the new count of ranges after merging.

## Parameters / Member Variables
- : FmgrInfo structure containing the comparison function for the data type
- : Collation OID to use for comparison operations
- : Array of sorted ExpandedRange structures to process (modified in-place)
- : Number of elements in the eranges array

## Dependencies
- Functions called/Symbols referenced:
  - [ExpandedRange](../E/ExpandedRange.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - memmove

- Called from (representative examples):
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- Requires input to be pre-sorted by minval, then maxval for correct operation
- Returns the number of ranges after merging (always ≤ input count)
- Merged ranges are automatically marked as not collapsed since they represent true intervals
- Uses memmove for safe array element shifting when removing merged ranges
- The algorithm has O(n) time complexity in the best case, but can be O(n²) if many ranges overlap
- Critical for the BRIN union operation to prevent range explosion during index maintenance
- The function is static and used internally within the BRIN minmax_multi implementation

## Simplified Source

```c
static int
merge_overlapping_ranges(FmgrInfo *cmp, Oid colloid,
                         ExpandedRange *eranges, int neranges)
{
    int idx = 0;

    while (idx < (neranges - 1)) {
        // Check if current range overlaps with next range
        // Ranges overlap if current_max >= next_min
        Datum r = FunctionCall2Coll(cmp, colloid,
                                    eranges[idx].maxval,
                                    eranges[idx + 1].minval);

        if (DatumGetBool(r)) {
            // No overlap - ranges are ordered, so no more overlaps possible
            idx++;
            continue;
        }

        // Ranges overlap - merge them
        // Keep the larger of the two maximum values
        r = FunctionCall2Coll(cmp, colloid,
                              eranges[idx].maxval,
                              eranges[idx + 1].maxval);

        if (DatumGetBool(r))
            eranges[idx].maxval = eranges[idx + 1].maxval;

        // Mark merged range as not collapsed
        eranges[idx].collapsed = false;

        // Remove the merged range by shifting remaining ranges
        memmove(&eranges[idx + 1], &eranges[idx + 2],
                (neranges - (idx + 2)) * sizeof(ExpandedRange));

        neranges--;
        // Don't increment idx - check if newly merged range overlaps with next
    }

    return neranges;
}
```