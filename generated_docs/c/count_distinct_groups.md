# count_distinct_groups

## Location
src/backend/statistics/mcv.c: 379 - 402

## Overview
Counts the number of distinct value combinations in a sorted array of SortItems by comparing adjacent elements using multi-column comparison logic.

## Definition
```c
static int count_distinct_groups(int numrows, SortItem *items, MultiSortSupport mss)
```

## Detailed Description
This function efficiently determines how many unique value combinations exist in a pre-sorted array of SortItems. It leverages the fact that the array is already sorted according to the MultiSortSupport to perform a single linear pass through the data.

The algorithm works by:
1. Starting with a count of 1 (assuming at least one row exists)
2. Iterating through the array from the second element
3. Comparing each element with its predecessor using multi_sort_compare
4. Incrementing the distinct count whenever a difference is found

The function includes assertion checks to verify that the input array is properly sorted, which is crucial for the algorithm's correctness.

## Parameters / Member Variables
- `numrows`: The number of SortItem elements in the array
- `items`: Array of SortItem structures, assumed to be sorted according to mss
- `mss`: MultiSortSupport defining the comparison criteria for multi-column sorting

## Dependencies
- Functions called/Symbols referenced:
  - multi_sort_compare
  - SortItem, MultiSortSupport
- Called from (representative examples):
  - SizeOfMCVList
  - build_distinct_groups

## Notes and Other Information
- Assumes the input array is pre-sorted; includes assertions to verify this
- Returns the count of distinct groups, not the count of total rows
- Uses multi_sort_compare for consistent comparison logic across the codebase
- Efficient O(n) algorithm that takes advantage of sorted input
- Essential for determining how many unique combinations exist before building MCV lists
- The comparison function returns 0 for equal items, enabling detection of group boundaries