# compare_sort_item_count

## Location
src/backend/statistics/mcv.c: 403 - 423

## Overview
A comparator function that sorts SortItem structures by their count (frequency) values in descending order, used for organizing groups by their occurrence frequency.

## Definition
```c
static int compare_sort_item_count(const void *a, const void *b, void *arg)
```

## Detailed Description
This function implements a three-way comparison for SortItem structures based on their count field. It follows the standard qsort_r comparator contract, returning:
- 0 when both items have equal counts
- -1 when the first item has a higher count (appears more frequently)
- 1 when the first item has a lower count (appears less frequently)

The descending order arrangement ensures that the most frequently occurring value combinations appear first in the sorted array, which is the desired order for MCV (Most Common Values) list construction.

## Parameters / Member Variables
- `a`: Pointer to the first SortItem to compare
- `b`: Pointer to the second SortItem to compare  
- `arg`: Unused argument (required by qsort_r interface)

## Dependencies
- Functions called/Symbols referenced:
  - SortItem
- Called from (representative examples):
  - build_distinct_groups

## Notes and Other Information
- Designed for use with qsort_r() or similar sorting functions that accept a comparator
- The arg parameter is unused but required for compatibility with the qsort_r interface
- Produces descending order (highest counts first), which is optimal for MCV list construction
- Simple integer comparison logic with no complex data type handling required
- Essential for frequency-based sorting in statistical analysis operations