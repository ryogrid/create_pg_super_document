# dumptuples

## Location
[src/backend/utils/sort/tuplestore.c:1206-1232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1206-L1232)

## Overview
Static function that removes tuples from memory and writes them as an initial run to tape during external sorting operations in PostgreSQL's tuplesort implementation.

## Definition
```c
static void dumptuples(Tuplesortstate *state, bool alltuples)
```

## Detailed Description
This function is a critical component of PostgreSQL's external sorting mechanism. It handles the transition from in-memory sorting to disk-based sorting when memory limitations are reached. The function sorts all tuples currently held in memory using quicksort, then writes them as a sorted run to tape storage.

The function performs several important operations: it checks whether dumping is necessary based on memory constraints, sorts the in-memory tuples, writes them to the destination tape, resets memory contexts to avoid fragmentation, and updates memory accounting. The function is designed to handle both regular memory pressure situations and final cleanup when all input has been processed.

The implementation includes safeguards against excessive run creation and provides extensive tracing capabilities for debugging external sort operations.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure containing the sort state and configuration
- `alltuples`: Boolean flag indicating whether to dump all tuples regardless of memory constraints (used at end of input)

## Dependencies
- Functions called/Symbols referenced:
  - LACKMEM (memory pressure check macro)
  - selectnewtape
  - tuplesort_sort_memtuples
  - WRITETUP
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - FREEMEM
  - [markrunend](../m/markrunend.md)
  - pg_rusage_show (for tracing)
- Types referenced:
  - Tuplesortstate
  - SortTuple
  - TSS_BUILDRUNS
- Called from (representative examples):
  - tuplesort_puttuple_common
  - tuplesort_performsort
  - tuplestore_puttuple_common

## Notes and Other Information
- Only operates when sort state is TSS_BUILDRUNS
- Includes protection against creating more than INT_MAX runs
- Performs quicksort on in-memory tuples before writing to tape
- Resets tuple memory context to prevent fragmentation from varying tuple sizes
- Includes extensive tracing support for debugging external sort performance
- Critical for enabling sorts of datasets larger than available memory
- Part of PostgreSQL's sophisticated external sorting infrastructure
- Handles both regular memory pressure dumps and final end-of-input dumps