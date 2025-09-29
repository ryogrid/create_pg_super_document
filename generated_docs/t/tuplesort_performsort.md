# tuplesort_performsort

## Location
[src/backend/utils/sort/tuplesort.c:1385-1495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1385-L1495)

## Overview
Finalizes the sort operation by executing the appropriate sorting strategy based on the current state, handling memory-based sorts, bounded heapsorts, and tape-based external sorts including parallel processing scenarios.

## Definition
```c
void tuplesort_performsort(Tuplesortstate *state)
```

## Detailed Description
The `tuplesort_performsort` function serves as the main dispatcher for completing sort operations after all tuples have been provided. It implements different sorting strategies based on the current tuplesort state and execution context (serial, parallel worker, or parallel leader).

The function handles four distinct sorting scenarios:

1. **TSS_INITIAL State**:
   - **Serial Mode**: Performs in-memory quicksort via `tuplesort_sort_memtuples` and transitions to `TSS_SORTEDINMEM`
   - **Worker Mode**: Dumps tuples to tape without merging, transitions to `TSS_SORTEDONTAPE` 
   - **Leader Mode**: Takes over worker tapes and performs merge operations

2. **TSS_BOUNDED State**: Transforms the accumulated heap into a properly sorted array using `sort_bounded_heap`, optimized for `ORDER BY ... LIMIT` queries

3. **TSS_BUILDRUNS State**: Completes tape-based external sorting by flushing remaining memory tuples and performing merge operations until a single run remains

The function also initializes scan state variables (`current`, `eof_reached`, `markpos_*`) to prepare for subsequent tuple retrieval operations. Debug tracing provides performance monitoring when `TRACE_SORT` is enabled.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure containing the sort configuration, current state, and accumulated tuples

## Dependencies
- Functions called/Symbols referenced:
  - `SERIAL`: Macro to check if this is a serial sort operation
  - `WORKER`: Macro to check if this is a parallel worker
  - `[tuplesort_sort_memtuples](tuplesort_sort_memtuples.md)`: Performs in-memory quicksort
  - `[inittapes](../i/inittapes.md)`: Initializes tape infrastructure for external sorting
  - `[dumptuples](../d/dumptuples.md)`: Writes tuples from memory to tape
  - `[worker_nomergeruns](../w/worker_nomergeruns.md)`: Handles worker completion without merging
  - `[leader_takeover_tapes](../l/leader_takeover_tapes.md)`: Leader process takes control of worker tapes
  - `[mergeruns](../m/mergeruns.md)`: Performs multi-way merge of sorted runs
  - `[sort_bounded_heap](../s/sort_bounded_heap.md)`: Converts heap to sorted array for bounded sorts
  - `[pg_rusage_show](../p/pg_rusage_show.md)`: Displays resource usage for debugging

- Called from (representative examples):
  - `[_brin_parallel_merge](../b/_brin_parallel_merge.md)` (src/backend/access/brin/brin.c:2624)
  - `[gistbuild](../g/gistbuild.md)` (src/backend/access/gist/gistbuild.c:281) 
  - `[_bt_leafbuild](../b/_bt_leafbuild.md)` (src/backend/access/nbtree/nbtsort.c:551, 556)
  - `[ExecSort](../E/ExecSort.md)` (src/backend/executor/nodeSort.c:160)
  - `[ExecIncrementalSort](../E/ExecIncrementalSort.md)` (src/backend/executor/nodeIncrementalSort.c:696, 777, 817, 931)
  - Various aggregate functions in `orderedsetaggs.c`

## Notes and Other Information
- The function operates within the sort memory context to ensure proper memory management
- Supports both serial and parallel execution modes with different processing paths
- Sets up scan state variables after sorting to enable subsequent `tuplesort_gettuple` operations
- In parallel scenarios, workers dump to tape while leaders coordinate merging operations
- The bounded heap path is specifically optimized for `LIMIT` queries where only the top N results are needed
- External sorting (tape-based) automatically triggers when memory limits are exceeded
- Debug output includes worker identification and resource usage statistics
- Critical transition point between tuple accumulation phase and result retrieval phase
- The function ensures proper state transitions: `TSS_SORTEDINMEM`, `TSS_SORTEDONTAPE`, or `TSS_FINALMERGE`
- Parallel processing coordination allows for efficient utilization of multiple CPU cores in large sort operations

## Simplified Source

```c
void tuplesort_performsort(Tuplesortstate *state) {
    MemoryContext oldcontext = MemoryContextSwitchTo(state->base.sortcontext);

    // Handle different sort states
    switch (state->status) {
        case TSS_INITIAL:
            // All tuples fit in memory or need parallel processing
            if (SERIAL(state)) {
                // Simple in-memory quicksort
                tuplesort_sort_memtuples(state);
                state->status = TSS_SORTEDINMEM;
            } else if (WORKER(state)) {
                // Parallel worker: dump to tape without merging
                inittapes(state, false);
                dumptuples(state, true);
                worker_nomergeruns(state);
                state->status = TSS_SORTEDONTAPE;
            } else {
                // Parallel leader: take over worker tapes and merge
                leader_takeover_tapes(state);
                mergeruns(state);
            }
            // Reset scan position for reading
            state->current = 0;
            state->eof_reached = false;
            break;

        case TSS_BOUNDED:
            // Limited result set using heap - convert to sorted array
            sort_bounded_heap(state);
            state->current = 0;
            state->eof_reached = false;
            break;

        case TSS_BUILDRUNS:
            // External sort: flush remaining tuples and merge runs
            dumptuples(state, true);
            mergeruns(state);
            state->eof_reached = false;
            break;

        default:
            elog(ERROR, "invalid tuplesort state");
    }

    MemoryContextSwitchTo(oldcontext);
}
```