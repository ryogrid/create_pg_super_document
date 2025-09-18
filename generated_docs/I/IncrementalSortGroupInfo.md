# IncrementalSortGroupInfo

## Location
src/include/nodes/execnodes.h: 2351 - 2359

## Overview
IncrementalSortGroupInfo is an instrumentation structure that collects performance and resource usage statistics for groups processed during incremental sort operations in PostgreSQL.

## Definition


## Detailed Description
IncrementalSortGroupInfo serves as a comprehensive instrumentation structure for tracking the performance characteristics of incremental sort operations. Incremental sorting optimizes multi-key sorts when the input data is already partially sorted on a prefix of the sort keys. This structure collects detailed metrics about resource usage (both memory and disk), tracks the number of groups processed, and records which sorting methods were employed. The data is essential for query optimization, performance analysis, and explaining query execution plans to users.

## Parameters / Member Variables
- : Total number of groups processed during the incremental sort operation
- : Maximum disk space usage in bytes across all groups
- : Cumulative disk space usage in bytes for all groups
- : Maximum memory usage in bytes across all groups
- : Cumulative memory usage in bytes for all groups
- : Bitmask indicating which TuplesortMethod algorithms were used during sorting

## Dependencies
- Functions called/Symbols referenced:
  - bits32
- Called from (representative examples):
  - show_incremental_sort_group_info
  - show_incremental_sort_info
  - instrumentSortedGroup
  - ExecInitIncrementalSort
  - IncrementalSortInfo

## Notes and Other Information
IncrementalSortGroupInfo is specifically designed for PostgreSQL's incremental sort optimization, which divides the sorting work into groups based on the presorted prefix keys. The instrumentation data helps identify performance bottlenecks, memory pressure, and optimal sorting strategies. The sortMethods bitmask tracks which algorithms (quicksort, heapsort, external sort, etc.) were used, providing insights into the sorting behavior under different data characteristics and memory constraints. This information is particularly valuable for query plan explanation and performance tuning.