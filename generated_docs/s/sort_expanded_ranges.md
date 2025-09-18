# sort_expanded_ranges

## Location
src/backend/access/brin/brin_minmax_multi.c: 1179 - 1230

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
  - ExpandedRange
  - compare_context
  - compare_expanded_ranges
  - qsort_arg
  - memcpy

- Called from (representative examples):
  - build_expanded_ranges
  - brin_minmax_multi_union

## Notes and Other Information
- Returns the number of unique ranges after deduplication (always ≤ input count)
- Modifies the input array in-place for memory efficiency
- The deduplication can significantly reduce the number of ranges, potentially avoiding expensive distance calculations
- Contains a TODO comment suggesting merge sort optimization could leverage partially sorted input data
- Uses assertion checks to ensure valid input and output conditions
- The function is static and used internally within the BRIN minmax_multi implementation