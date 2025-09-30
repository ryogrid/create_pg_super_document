# tuplesort_begin_common

## Location
[src/backend/utils/sort/tuplesort.c:645-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L645-L756)

## Overview
The core initialization function for PostgreSQL's tuple sorting system that sets up the common infrastructure for all tuplesort operations, including memory management, parallel coordination, and basic state initialization.

## Definition
```c
Tuplesortstate *tuplesort_begin_common(int workMem, SortCoordinate coordinate, int sortopt)
```

## Detailed Description
This function serves as the foundation for all tuplesort operations in PostgreSQL. It creates and initializes a `Tuplesortstate` structure with the necessary memory contexts, configuration settings, and parallel processing coordination. The function establishes two memory contexts: a main context that survives across multiple sort batches and a sort context that gets reset between operations.

The function handles both serial and parallel sorting scenarios by examining the `coordinate` parameter. For parallel sorts, it sets up appropriate worker identification and shared state management. It enforces a minimum work memory of 64KB to protect against parallel workers with insufficient memory allocation.

After setting up the basic state, it calls `tuplesort_begin_batch` to initialize batch-specific components. The function also includes trace support for debugging sort operations when `TRACE_SORT` is enabled.

## Parameters / Member Variables
- `workMem`: Maximum kilobytes of RAM to use before spilling to disk (minimum 64KB enforced)
- `coordinate`: Parallel sort coordination information (NULL for serial sorts)
- `sortopt`: Bitmask of sort options (TUPLESORT_* flags from tuplesort.h)

## Dependencies
- Functions called/Symbols referenced:
  - `AllocSetContextCreate` - Creates memory contexts for sort operations
  - [tuplesort_begin_batch](tuplesort_begin_batch.md) - Initializes batch-specific state
  - [worker_get_identifier](../w/worker_get_identifier.md) - Gets worker ID for parallel sorts
  - `[pg_rusage_init](../p/pg_rusage_init.md)` - Initializes resource usage tracking (if TRACE_SORT enabled)
  - `TUPLESORT_RANDOMACCESS` - [Sort](../S/Sort.md) option constant
  - `INITIAL_MEMTUPSIZE` - Initial memory tuple array size
- Called from (representative examples):
  - [tuplesort_begin_heap](tuplesort_begin_heap.md) - For heap tuple sorting
  - [tuplesort_begin_cluster](tuplesort_begin_cluster.md) - For cluster operations
  - [tuplesort_begin_index_btree](tuplesort_begin_index_btree.md) - For B-tree index creation
  - [tuplesort_begin_index_hash](tuplesort_begin_index_hash.md) - For hash index creation
  - [tuplesort_begin_datum](tuplesort_begin_datum.md) - For single datum sorting

## Notes and Other Information
- Serves as the common foundation for all specialized tuplesort variants
- Creates persistent memory contexts that survive across multiple sort batches
- Enforces minimum work memory of 64KB to protect parallel workers
- Validates that random access is not requested for parallel sorts
- Sets up infrastructure for both serial and parallel sorting scenarios
- The returned `Tuplesortstate` must be freed with `tuplesort_end`
- Memory context management ensures proper cleanup and isolation between sort operations
- Part of PostgreSQL's sophisticated memory management and parallel processing architecture

## Simplified Source

```c
Tuplesortstate *tuplesort_begin_common(int workMem, SortCoordinate coordinate, int sortopt) {
    Tuplesortstate *state;
    MemoryContext maincontext, sortcontext, oldcontext;

    // Validate parallel sort constraints
    if (coordinate && (sortopt & TUPLESORT_RANDOMACCESS))
        elog(ERROR, "random access disallowed under parallel sort");

    // Create memory contexts for sort operation
    maincontext = AllocSetContextCreate(CurrentMemoryContext,
                                        "TupleSort main",
                                        ALLOCSET_DEFAULT_SIZES);
    sortcontext = AllocSetContextCreate(maincontext,
                                        "TupleSort sort",
                                        ALLOCSET_DEFAULT_SIZES);

    // Allocate and initialize tuplesort state
    oldcontext = MemoryContextSwitchTo(maincontext);
    state = (Tuplesortstate *) palloc0(sizeof(Tuplesortstate));

    // Configure basic sort parameters
    state->base.sortopt = sortopt;
    state->base.tuples = true;
    state->abbrevNext = 10;

    // Set memory limit (minimum 64KB for parallel workers)
    state->allowedMem = Max(workMem, 64) * (int64) 1024;
    state->base.sortcontext = sortcontext;
    state->base.maincontext = maincontext;

    // Initialize tuple array
    state->memtupsize = INITIAL_MEMTUPSIZE;
    state->memtuples = NULL;

    // Setup batch-specific state
    tuplesort_begin_batch(state);

    // Configure parallel coordination
    if (!coordinate) {
        // Serial sort
        state->shared = NULL;
        state->worker = -1;
        state->nParticipants = -1;
    } else if (coordinate->isWorker) {
        // Parallel worker
        state->shared = coordinate->sharedsort;
        state->worker = worker_get_identifier(state);
        state->nParticipants = -1;
    } else {
        // Parallel leader
        state->shared = coordinate->sharedsort;
        state->worker = -1;
        state->nParticipants = coordinate->nParticipants;
    }

    MemoryContextSwitchTo(oldcontext);
    return state;
}
```