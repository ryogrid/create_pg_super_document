# instrumentSortedGroup

## Location
src/backend/executor/nodeIncrementalSort.c: 127 - 163

## Overview
A static function that captures and accumulates instrumentation statistics from a completed tuplesort operation within an incremental sort node for later EXPLAIN ANALYZE output.

## Definition


## Detailed Description
This function is responsible for collecting performance statistics from a completed sort batch in the incremental sort executor. Since incremental sort processes potentially many sort batches (both full sorts and prefix sorts), this function captures tuplesort statistics each time a sort state is finalized. The collected data includes memory/disk space usage, sort methods used, and group counts, which are later aggregated and displayed in EXPLAIN ANALYZE output.

The function retrieves instrumentation data from the tuplesort state and updates the group information with cumulative statistics including total and maximum space usage (memory or disk), and tracks which sort methods have been employed.

## Parameters / Member Variables
- : Pointer to IncrementalSortGroupInfo structure that accumulates statistics for a group of sort operations (either fullsort or prefixsort groups)
- : Pointer to the Tuplesortstate from which to extract instrumentation statistics

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_get_stats (retrieves instrumentation data from sort state)
  - IncrementalSortGroupInfo (structure type for accumulating group statistics)
  - Tuplesortstate (tuplesort state structure)
  - TuplesortInstrumentation (structure for sort instrumentation data)
  - SORT_SPACE_TYPE_DISK (enum value for disk-based sorting)
  - SORT_SPACE_TYPE_MEMORY (enum value for memory-based sorting)
- Called from (representative examples):
  - INSTRUMENT_SORT_GROUP macro (used to choose between local and shared instrumentation storage)

## Notes and Other Information
- This function is only called when instrumentation is enabled (when the plan state has an instrument structure)
- The function handles both parallel and non-parallel execution contexts through the INSTRUMENT_SORT_GROUP macro wrapper
- Statistics are accumulated across multiple sort batches to provide comprehensive performance information
- The function tracks both total and maximum resource usage to help identify performance bottlenecks in incremental sorting operations