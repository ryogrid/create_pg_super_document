# build_sorted_items

## Location
src/backend/statistics/extended_stats.c: 986 - 1117

## Overview
Builds a sorted array of SortItem structures from sample rows for extended statistics computation, handling memory allocation and data transformation efficiently.

## Definition


## Detailed Description
This function creates a sorted array of SortItem structures from statistical sample data. It performs several critical operations: allocates memory in a single chunk for efficiency, extracts and processes attribute values from sample rows, handles variable-length data by detoasting when necessary, filters out overly wide values that exceed WIDTH_THRESHOLD, and finally sorts the resulting items using multi-column sort support. The function is designed to support extended statistics calculations like dependency analysis and multi-column value (MCV) statistics.

## Parameters / Member Variables
- : StatsBuildData structure containing sample rows and metadata for statistics computation
- : Output parameter that receives the actual number of items in the resulting sorted array
- : MultiSortSupport structure providing multi-column sorting capabilities
- : Number of attributes to process from the sample data
- : Array of attribute numbers specifying which attributes to include

## Dependencies
- Functions called/Symbols referenced:
  - get_typlen
  - toast_raw_datum_size
  - PG_DETOAST_DATUM
  - qsort_interruptible
  - multi_sort_compare
  - WIDTH_THRESHOLD (constant)
  - StatsBuildData (type)
  - MultiSortSupport (type)
  - SortItem (type)
- Called from (representative examples):
  - dependency_degree
  - statext_mcv_build

## Notes and Other Information
- Memory is allocated as a single contiguous chunk for efficient cleanup - caller only needs to pfree() the return value
- Filters out rows containing values that are too wide (exceed WIDTH_THRESHOLD) to avoid memory issues
- Handles variable-length attributes by detoasting them when processing
- Returns NULL if all sample rows are filtered out due to overly wide values
- Includes comprehensive memory layout management with pointer arithmetic to organize SortItem array, Datum values, and null flags
- Uses qsort_interruptible to allow query cancellation during sorting of large datasets
- Located in src/backend/statistics/extended_stats.c:986-1117