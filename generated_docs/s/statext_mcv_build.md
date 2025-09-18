# statext_mcv_build

## Location
src/backend/statistics/mcv.c: 180 - 346

## Overview
Builds a Most Common Values (MCV) list from sampled rows for multi-column extended statistics, implementing a four-step algorithm to identify and store the most frequently occurring value combinations.

## Definition
```c
MCVList *statext_mcv_build(StatsBuildData *data, double totalrows, int stattarget)
```

## Detailed Description
This function constructs an MCV list for multi-column statistics using a sophisticated algorithm that differs from single-column MCV construction. The process involves four main steps:

1. **Sort the data**: Uses default collation and '<' operator for the data types
2. **Count distinct groups**: Determines how many distinct value combinations exist
3. **Build MCV list**: Uses a statistical threshold to decide which combinations to keep
4. **Cleanup**: Removes rows represented by the MCV from the sample

The key difference from single-column MCV lists is that this function considers how actual frequencies differ from base frequencies (assuming column independence). It uses `get_mincount_for_mcv_list()` to establish a statistical threshold for inclusion, keeping all groups that appear more frequently than this minimum count.

For each MCV item, the function calculates both the observed frequency and the base frequency (what the frequency would be if columns were independent), enabling detection of both common and unexpectedly rare combinations.

## Parameters / Member Variables
- `data`: Statistical build data containing sampled rows and column information
- `totalrows`: Total number of rows in the table (for statistical calculations)
- `stattarget`: Target number of statistics items to keep (upper bound)

## Dependencies
- Functions called/Symbols referenced:
  - build_mss
  - build_sorted_items
  - build_distinct_groups
  - get_mincount_for_mcv_list
  - build_column_frequencies
  - bsearch_arg
  - multi_sort_compare
  - MCVList, MCVItem, SortItem, StatsBuildData
  - STATS_MCV_MAGIC, STATS_MCV_TYPE_BASIC
- Called from (representative examples):
  - BuildRelationExtStatistics

## Notes and Other Information
- Returns NULL if no items are available or if sorting fails
- May return NULL for uniform distributions with many groups where no values exceed the minimum threshold
- Stores both frequency and base_frequency for each MCV item to enable independence analysis
- Groups are sorted by frequency in descending order
- Uses binary search to efficiently find column frequencies when calculating base frequencies
- Allocates memory for MCVList structure and individual MCVItem components
- The algorithm specifically handles multi-column scenarios where traditional single-column approaches are insufficient