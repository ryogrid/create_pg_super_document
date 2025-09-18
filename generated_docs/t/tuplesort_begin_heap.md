# tuplesort_begin_heap

## Location
[src/backend/utils/sort/tuplesortvariants.c:168-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L168-L242)

## Overview
Initializes a Tuplesortstate for sorting heap tuples with support for multiple sort keys, memory management, and various optimization strategies including abbreviation and parallel sorting.

## Definition


## Detailed Description
This function creates and configures a new tuplesort state specifically for heap tuple sorting. It sets up the sorting infrastructure including comparison functions, I/O functions, and sort support data for each sort key. The function supports advanced features like abbreviation optimization for improved performance, parallel sorting coordination, and configurable memory usage limits. It establishes the foundation for efficient tuple sorting operations by preparing all necessary comparison and data handling mechanisms.

## Parameters / Member Variables
- : Tuple descriptor defining the structure of tuples to be sorted
- : Number of sort keys (must be > 0)
- : Array of attribute numbers for sort keys
- : Array of comparison operator OIDs for each sort key
- : Array of collation OIDs for each sort key
- : Array of boolean flags indicating null ordering preference for each key
- : Amount of memory (in KB) available for sorting operations
- : Coordination structure for parallel sorting operations
- : Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [removeabbrev_heap](../r/removeabbrev_heap.md)
  - [comparetup_heap](../c/comparetup_heap.md)
  - [comparetup_heap_tiebreak](../c/comparetup_heap_tiebreak.md)
  - [writetup_heap](../w/writetup_heap.md)
  - [readtup_heap](../r/readtup_heap.md)
  - PrepareSortSupportFromOrderingOp
- Called from (representative examples):
  - [initialize_phase](../i/initialize_phase.md) (nodeAgg.c:524)
  - [initialize_aggregate](../i/initialize_aggregate.md) (nodeAgg.c:612)
  - [ExecSort](../E/ExecSort.md) (nodeSort.c:114)
  - ExecIncrementalSort (nodeIncrementalSort.c:610)

## Notes and Other Information
- The function supports the "onlyKey" optimization when there's a single sort key without abbreviation
- Abbreviation optimization is enabled for the first sort key when applicable, improving performance for pass-by-reference types
- Memory context switching ensures proper allocation of sort-related data structures
- Includes tracing support for debugging sort operations when TRACE_SORT is enabled
- The function assumes the TupleDesc doesn't need to be copied and stores it directly as an argument