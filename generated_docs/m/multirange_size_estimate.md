# multirange_size_estimate

## Location
[src/backend/utils/adt/multirangetypes.c:569-595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L569-L595)

## Overview
Estimates the total size in bytes required to serialize a multirange structure containing a specified number of ranges.

## Definition

```c
struct, items and flags.
	 */
	size = att_align_nominal(sizeof(MultirangeType) +
							 Max(range_count - 1, 0) * sizeof(uint32) +
							 range_count * sizeof(uint8), elemalign);
```
## Detailed Description
This function calculates the memory footprint needed to store a multirange in its serialized form. The calculation includes space for the MultirangeType structure itself, item offsets, flags, and the actual range data. The function takes into account proper alignment requirements based on the element type's alignment characteristics.

The size calculation consists of:
1. Base MultirangeType structure size
2. Array of offset values (uint32) for range_count - 1 items 
3. Array of flags (uint8) for each range
4. Actual range bound data for each range, excluding range headers

All components are properly aligned according to the element type's alignment requirements using att_align_nominal.

## Parameters / Member Variables
- : TypeCacheEntry containing type information for the range type, including element type alignment information
- : Number of ranges that will be stored in the multirange
- : Array of RangeType pointers containing the actual range data to be measured

## Dependencies
- Functions called/Symbols referenced:
  - att_align_nominal (for proper memory alignment calculations)
  - VARSIZE (to get the total size of each range structure)
  - MultirangeType (the structure whose size is being calculated)
  - Max (macro for maximum value calculation)
- Called from (representative examples):
  - [make_multirange](make_multirange.md)

## Notes and Other Information
- This is a static function used internally for memory allocation planning
- The function accounts for alignment requirements which can add padding bytes between components
- The calculation excludes one offset value (range_count - 1) because the first range's offset is implicit
- [Range](../R/Range.md) header overhead (sizeof(RangeType) + sizeof(char)) is excluded from each range's contribution since only the bound data is stored
- The estimate is used by make_multirange to allocate the appropriate amount of memory before serialization
- Proper size estimation is critical for avoiding buffer overruns during multirange construction

## Simplified Source

```c
static Size
multirange_size_estimate(TypeCacheEntry *rangetyp, int32 range_count,
                         RangeType **ranges)
{
    char    elemalign = rangetyp->rngelemtype->typalign;
    Size    size;
    int32   i;

    // Calculate space for multirange structure + items + flags
    size = att_align_nominal(sizeof(MultirangeType) +
                            Max(range_count - 1, 0) * sizeof(uint32) +
                            range_count * sizeof(uint8), elemalign);

    // Add space for each range's boundary data
    for (i = 0; i < range_count; i++)
        size += att_align_nominal(VARSIZE(ranges[i]) -
                                 sizeof(RangeType) -
                                 sizeof(char), elemalign);

    return size;
}
```