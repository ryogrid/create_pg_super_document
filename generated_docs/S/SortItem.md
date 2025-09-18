# SortItem

## Location
src/include/statistics/extended_stats_internal.h: 53 - 58

## Overview
SortItem is a structure that represents a tuple of values with their corresponding frequency count, used as the fundamental unit for sorting and statistical operations in PostgreSQL's extended statistics framework.

## Definition


## Detailed Description
SortItem serves as a container for multi-dimensional data tuples in PostgreSQL's extended statistics processing. Each SortItem represents a unique combination of values across multiple columns along with the frequency of that combination's occurrence. This structure is central to building various types of extended statistics, including MCV (Most Common Values) lists, dependency statistics, and multi-variate distinct value calculations. The structure efficiently groups identical tuples together and tracks their occurrence count, which is essential for frequency-based statistical analysis.

## Parameters / Member Variables
- : Pointer to an array of Datum values representing the tuple across multiple dimensions/columns
- : Pointer to an array of boolean flags indicating which values in the corresponding positions are NULL
- : The frequency count indicating how many times this particular combination of values appears in the dataset

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references from this struct)
- Called from (representative examples):
  - dependency_degree (src/backend/statistics/dependencies.c:226)
  - multi_sort_compare (src/backend/statistics/extended_stats.c:868, 869)
  - build_sorted_items (src/backend/statistics/extended_stats.c:996, 1003, 1009, 1010, 1107)
  - statext_mcv_build (src/backend/statistics/mcv.c:188, 189, 261, 265, 314, 324, 325)
  - compare_sort_item_count (src/backend/statistics/mcv.c:405, 406, 423)
  - build_distinct_groups (src/backend/statistics/mcv.c:424, 431, 456)
  - sort_item_compare (src/backend/statistics/mcv.c:468, 469, 489)
  - build_column_frequencies (src/backend/statistics/mcv.c:490, 495, 502, 503, 506, 507, 514, 515, 527)
  - ndistinct_for_combination (src/backend/statistics/mvdistinct.c:435, 447, 491)

## Notes and Other Information
- Core data structure for extended statistics processing in PostgreSQL
- Used extensively in MCV list construction, dependency analysis, and distinct value estimation
- The values array corresponds to the order of attributes in the statistics definition
- Count field enables frequency-based analysis and identification of most common value combinations
- Supports multi-dimensional NULL handling through the isnull array
- Essential for sorting operations across multiple dimensions in statistical computations
- Located in src/include/statistics/extended_stats_internal.h as part of the extended statistics framework