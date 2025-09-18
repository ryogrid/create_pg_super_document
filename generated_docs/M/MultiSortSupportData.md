# MultiSortSupportData

## Location
src/include/statistics/extended_stats_internal.h: 44 - 49

## Overview
MultiSortSupportData is a structure that provides multi-dimensional sorting support for PostgreSQL's extended statistics, containing sort support data for multiple dimensions simultaneously.

## Definition


## Detailed Description
MultiSortSupportData is designed to handle sorting operations across multiple dimensions in PostgreSQL's extended statistics framework. This structure is essential for building and processing multi-column statistics, particularly MCV (Most Common Values) lists that span multiple attributes. The structure uses a flexible array member to accommodate varying numbers of dimensions, making it adaptable to different statistical scenarios. Each dimension gets its own SortSupportData entry to handle type-specific sorting logic.

## Parameters / Member Variables
- : The number of dimensions (columns) that this multi-sort structure handles
- : A flexible array of SortSupportData structures, one for each dimension, containing the sort support functions and metadata for that dimension's data type

## Dependencies
- Functions called/Symbols referenced:
  - SortSupportData (embedded structure for each dimension)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length array)
- Called from (representative examples):
  - multi_sort_init (src/backend/statistics/extended_stats.c:838)
  - statext_mcv_build (src/backend/statistics/mcv.c:269)
  - MultiSortSupport (typedef alias at src/include/statistics/extended_stats_internal.h:51)

## Notes and Other Information
- Part of PostgreSQL's extended statistics internal implementation
- Uses flexible array member pattern for variable number of dimensions
- Essential for multi-column statistical operations like MCV list construction
- Each SortSupportData entry provides type-specific comparison functions for efficient sorting
- The structure enables efficient sorting of tuples across multiple attributes simultaneously
- Located in src/include/statistics/extended_stats_internal.h alongside other extended statistics structures