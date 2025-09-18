# build_distinct_groups

## Location
src/backend/statistics/mcv.c: 424 - 464

## Overview
Builds an array of SortItems representing distinct groups with their occurrence counts from a sorted array of items for MCV (Most Common Values) statistics.

## Definition


## Detailed Description
This function processes a sorted array of SortItem objects to identify distinct groups and count their frequencies. It creates a new array where each element represents a unique combination of values with its occurrence count. The resulting array is sorted by frequency in descending order to support MCV list generation. The function assumes the input array is already sorted in ascending order and uses multi-column comparison to identify distinct groups.

## Parameters / Member Variables
- `numrows`: Number of rows/items in the input array
- `items`: Sorted array of SortItem objects to process
- `mss`: MultiSortSupport structure for multi-column comparison operations
- `ndistinct`: Output parameter that receives the number of distinct groups found

## Dependencies
- Functions called/Symbols referenced:
  - [count_distinct_groups](../c/count_distinct_groups.md)
  - [multi_sort_compare](../m/multi_sort_compare.md)
  - [compare_sort_item_count](../c/compare_sort_item_count.md)
  - qsort_interruptible
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - SizeOfMCVList
  - [statext_mcv_build](../s/statext_mcv_build.md)

## Notes and Other Information
- The input array must be sorted in ascending order for correct operation
- The function allocates memory for the result array using palloc
- Uses assertions to verify sorting assumptions and validate output
- The final result is sorted by count in descending order to prioritize most frequent values
- Part of PostgreSQL's extended statistics system for MCV list generation