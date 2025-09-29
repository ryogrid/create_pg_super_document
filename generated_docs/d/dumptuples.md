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
  - [selectnewtape](../s/selectnewtape.md)
  - [tuplesort_sort_memtuples](../t/tuplesort_sort_memtuples.md)
  - WRITETUP
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - FREEMEM
  - [markrunend](../m/markrunend.md)
  - [pg_rusage_show](../p/pg_rusage_show.md) (for tracing)
- Types referenced:
  - [Tuplesortstate](../T/Tuplesortstate.md)
  - SortTuple
  - TSS_BUILDRUNS
- Called from (representative examples):
  - [tuplesort_puttuple_common](../t/tuplesort_puttuple_common.md)
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [tuplestore_puttuple_common](../t/tuplestore_puttuple_common.md)

## Notes and Other Information
- Only operates when sort state is TSS_BUILDRUNS
- Includes protection against creating more than INT_MAX runs
- Performs quicksort on in-memory tuples before writing to tape
- Resets tuple memory context to prevent fragmentation from varying tuple sizes
- Includes extensive tracing support for debugging external sort performance
- Critical for enabling sorts of datasets larger than available memory
- Part of PostgreSQL's sophisticated external sorting infrastructure
- Handles both regular memory pressure dumps and final end-of-input dumps

## Simplified Source

```c
static void
dumptuples(Tuplesortstate *state, bool alltuples)
{
    int memtupwrite;
    int i;

    // Skip if we still fit in memory and this isn't the final call
    if (state->memtupcount < state->memtupsize && !LACKMEM(state) && !alltuples)
        return;

    // Avoid creating completely empty runs (except for workers)
    if (state->memtupcount == 0 && state->currentRun > 0)
        return;

    Assert(state->status == TSS_BUILDRUNS);

    // Check run count limit
    if (state->currentRun == INT_MAX)
        ereport(ERROR, "cannot have more than %d runs for an external sort");

    // Select new tape if not the first run
    if (state->currentRun > 0)
        selectnewtape(state);

    state->currentRun++;

    // Sort all tuples in memory using quicksort
    tuplesort_sort_memtuples(state);

    // Write all sorted tuples to tape
    memtupwrite = state->memtupcount;
    for (i = 0; i < memtupwrite; i++) {
        SortTuple *stup = &state->memtuples[i];
        WRITETUP(state, state->destTape, stup);
    }

    // Reset tuple count and memory accounting
    state->memtupcount = 0;

    // Reset tuple memory context to avoid fragmentation
    MemoryContextReset(state->base.tuplecontext);

    // Update memory accounting
    FREEMEM(state, state->tupleMem);
    state->tupleMem = 0;

    // Mark end of this run on tape
    markrunend(state->destTape);
}
```