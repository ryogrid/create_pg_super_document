# tuplesort_get_stats

## Location
[src/backend/utils/sort/tuplesort.c:2537-2580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2537-L2580)

## Overview
Extracts summary statistics from a completed tuplesort operation, providing information about the sorting method used and space consumption for performance analysis and debugging.

## Definition
```c
void tuplesort_get_stats(Tuplesortstate *state, TuplesortInstrumentation *stats)
```

## Detailed Description
This function retrieves comprehensive statistics about a tuplesort operation after it has been performed. It provides essential information for query performance analysis, EXPLAIN output, and debugging purposes.

The function determines the sorting method based on the maximum space status reached during the sort operation:
- **SORT_TYPE_QUICKSORT**: Used for in-memory sorts without bounded heap
- **SORT_TYPE_TOP_N_HEAPSORT**: Used for in-memory bounded heap sorts (LIMIT queries)
- **SORT_TYPE_EXTERNAL_SORT**: Used when sorting required external tapes
- **SORT_TYPE_EXTERNAL_MERGE**: Used during final merge phase of external sorts
- **SORT_TYPE_STILL_IN_PROGRESS**: Returned if sorting is not yet complete

Space information includes both the type (memory vs. disk) and the amount used in kilobytes. The function calls `tuplesort_updatemax` to ensure the maximum space usage is current before reporting statistics.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing the sort operation to analyze
- `stats`: Pointer to TuplesortInstrumentation structure to populate with statistics including sortMethod, spaceType, and spaceUsed

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_updatemax](tuplesort_updatemax.md)
- Constants referenced:
  - SORT_SPACE_TYPE_DISK
  - SORT_SPACE_TYPE_MEMORY
  - SORT_TYPE_TOP_N_HEAPSORT
  - SORT_TYPE_QUICKSORT
  - SORT_TYPE_EXTERNAL_SORT
  - SORT_TYPE_EXTERNAL_MERGE
  - SORT_TYPE_STILL_IN_PROGRESS
  - TSS_SORTEDINMEM
  - TSS_SORTEDONTAPE
  - TSS_FINALMERGE
- Called from (representative examples):
  - [show_sort_info](../s/show_sort_info.md) (in explain.c for EXPLAIN output)
  - [ExecSort](../E/ExecSort.md) (in nodeSort.c for execution statistics)
  - [instrumentSortedGroup](../i/instrumentSortedGroup.md) (in nodeIncrementalSort.c)

## Notes and Other Information
- Should only be called after tuplesort_performsort() has completed
- Space usage is reported in kilobytes (rounded up from bytes)
- Memory usage tracking has limitations once tuples are being returned to the caller due to untracked pfree operations
- The function provides the basis for sort statistics shown in EXPLAIN ANALYZE output
- Distinguishes between different sort algorithms to help users understand query performance characteristics