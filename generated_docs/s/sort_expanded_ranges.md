# sort_expanded_ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:1179-1230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1179-L1230)

## Overview
Sorts and deduplicates an array of ExpandedRange structures using quicksort algorithm and range comparison functions.

## Definition
static int sort_expanded_ranges(FmgrInfo *cmp, Oid colloid, ExpandedRange *eranges, int neranges)

## Detailed Description
This function performs a two-phase operation on an array of ExpandedRange structures:

1. **Sorting Phase**: Uses  with a custom comparison function  to sort all ranges by their minimum values (and maximum values as secondary criteria).

2. **Deduplication Phase**: Performs an in-place deduplication by comparing consecutive ranges after sorting. Identical ranges are removed, with the remaining unique ranges compacted toward the beginning of the array.

The function currently uses quicksort for all elements, though there's potential for optimization using merge sort since some input data (existing ranges and potentially some values) may already be sorted.

## Parameters / Member Variables
- : FmgrInfo structure containing the comparison function for the data type
- : Collation OID to use for comparison operations
- : Array of ExpandedRange structures to sort and deduplicate (modified in-place)
- : Number of elements in the eranges array

## Dependencies
- Functions called/Symbols referenced:
  - [ExpandedRange](../E/ExpandedRange.md)
  - [compare_context](../c/compare_context.md)
  - [compare_expanded_ranges](../c/compare_expanded_ranges.md)
  - qsort_arg
  - memcpy

- Called from (representative examples):
  - [build_expanded_ranges](../b/build_expanded_ranges.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- Returns the number of unique ranges after deduplication (always ≤ input count)
- Modifies the input array in-place for memory efficiency
- The deduplication can significantly reduce the number of ranges, potentially avoiding expensive distance calculations
- Contains a TODO comment suggesting merge sort optimization could leverage partially sorted input data
- Uses assertion checks to ensure valid input and output conditions
- The function is static and used internally within the BRIN minmax_multi implementation