# tuplesort_puttuple_common

## Location
[src/backend/utils/sort/tuplesort.c:1189-1340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1189-L1340)

## Overview
Core function that handles inserting tuples into the sort state, managing memory, abbreviated keys, and state transitions between different sorting phases based on memory constraints and tuple counts.

## Definition
```c
void tuplesort_puttuple_common(Tuplesortstate *state, SortTuple *tuple, bool useAbbrev, Size tuplen)
```

## Detailed Description
The `tuplesort_puttuple_common` function serves as the central tuple insertion mechanism for PostgreSQLs tuplesort subsystem. It handles the complex logic of managing tuples across different sorting states while optimizing memory usage and performance.

The function performs several key operations:

1. **Memory Management**: Tracks memory usage via `USEMEM` and updates `tupleMem` counters
2. **Abbreviated Key Handling**: Manages abbreviated key conversion and abort detection through `consider_abort_common`
3. **State-Dependent Processing**: Handles tuple insertion differently based on current sort state:
   - `TSS_INITIAL`: Stores tuples in memory array, grows array as needed, transitions to bounded heap or tape-based sorting
   - `TSS_BOUNDED`: Implements bounded heapsort by comparing with heap top and discarding lesser tuples
   - `TSS_BUILDRUNS`: Stores tuples and dumps to tape when memory limit exceeded

4. **Smart Transitions**: Makes intelligent decisions about when to switch sorting strategies based on memory constraints and tuple counts

The function includes sophisticated heuristics for determining when to switch from quicksort to heapsort (when tuple count exceeds twice the bound) and when to transition from memory-based to tape-based sorting.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure managing the sort operation
- `tuple`: Pointer to the SortTuple being inserted
- `useAbbrev`: Boolean indicating whether abbreviated keys should be used
- `tuplen`: Size of the tuple in bytes for memory accounting

## Dependencies
- Functions called/Symbols referenced:
  - `LEADER`: Macro to check if this is a leader process
  - `USEMEM`: Macro for memory usage tracking
  - `[consider_abort_common](../c/consider_abort_common.md)`: Determines if abbreviated key optimization should be aborted
  - `REMOVEABBREV`: Macro to remove abbreviated keys from existing tuples
  - `[grow_memtuples](../g/grow_memtuples.md)`: Expands the memtuples array when needed
  - `LACKMEM`: Macro to check if memory is insufficient
  - `[make_bounded_heap](../m/make_bounded_heap.md)`: Converts to bounded heap sorting
  - `[inittapes](../i/inittapes.md)`: Initializes tape-based sorting
  - `[dumptuples](../d/dumptuples.md)`: Writes tuples to tape
  - `COMPARETUP`: Macro for tuple comparison
  - `[free_sort_tuple](../f/free_sort_tuple.md)`: Frees tuple memory
  - `[tuplesort_heap_replace_top](tuplesort_heap_replace_top.md)`: Replaces heap root in bounded sort

- Called from (representative examples):
  - `[tuplesort_puttupleslot](tuplesort_puttupleslot.md)` (src/backend/utils/sort/tuplesortvariants.c:696)
  - `[tuplesort_putheaptuple](tuplesort_putheaptuple.md)` (src/backend/utils/sort/tuplesortvariants.c:739)
  - `[tuplesort_putindextuplevalues](tuplesort_putindextuplevalues.md)` (src/backend/utils/sort/tuplesortvariants.c:778)
  - `[tuplesort_putbrintuple](tuplesort_putbrintuple.md)` (src/backend/utils/sort/tuplesortvariants.c:812)
  - `[tuplesort_putdatum](tuplesort_putdatum.md)` (src/backend/utils/sort/tuplesortvariants.c:862)

## Notes and Other Information
- The function operates within the sort context memory context for proper memory management
- Includes debug tracing support via `TRACE_SORT` compilation flag
- Handles abbreviated key abort scenarios by reverting all stored tuples to non-abbreviated form
- Uses a heuristic of "twice the bound" to determine optimal switch point to bounded heapsort
- Critical for performance as it implements the core tuple insertion and sorting strategy selection logic
- The bounded heap optimization significantly improves performance for `ORDER BY ... LIMIT` queries
- Memory context switching ensures proper memory lifecycle management
- Includes `CHECK_FOR_INTERRUPTS()` calls for query cancellation support in bounded mode

## Simplified Source

```c
void tuplesort_puttuple_common(Tuplesortstate *state, SortTuple *tuple, bool useAbbrev, Size tuplen) {
    MemoryContext oldcontext = MemoryContextSwitchTo(state->base.sortcontext);

    // Track memory usage for this tuple
    USEMEM(state, tuplen);
    state->tupleMem += tuplen;

    // Handle abbreviated key conversion
    if (useAbbrev && !consider_abort_common(state)) {
        // Convert to abbreviated key representation
        tuple->datum1 = state->base.sortKeys->abbrev_converter(tuple->datum1, state->base.sortKeys);
    } else if (useAbbrev) {
        // Abort abbreviated keys - revert existing tuples
        REMOVEABBREV(state, state->memtuples, state->memtupcount);
    }

    switch (state->status) {
        case TSS_INITIAL:
            // Add tuple to memory array
            if (state->memtupcount >= state->memtupsize - 1) {
                grow_memtuples(state);
            }
            state->memtuples[state->memtupcount++] = *tuple;

            // Check if we should switch to bounded heap
            if (state->bounded &&
                (state->memtupcount > state->bound * 2 ||
                 (state->memtupcount > state->bound && LACKMEM(state)))) {
                make_bounded_heap(state);
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            // Switch to tape-based sorting if memory is full
            if (state->memtupcount >= state->memtupsize || LACKMEM(state)) {
                inittapes(state, true);
                dumptuples(state, false);
            }
            break;

        case TSS_BOUNDED:
            // For bounded heap, only keep best tuples
            if (COMPARETUP(state, tuple, &state->memtuples[0]) <= 0) {
                // New tuple is worse than heap top - discard it
                free_sort_tuple(state, tuple);
            } else {
                // Replace heap top with new tuple
                free_sort_tuple(state, &state->memtuples[0]);
                tuplesort_heap_replace_top(state, tuple);
            }
            break;

        case TSS_BUILDRUNS:
            // Add tuple and dump if memory limit exceeded
            state->memtuples[state->memtupcount++] = *tuple;
            dumptuples(state, false);
            break;

        default:
            elog(ERROR, "invalid tuplesort state");
    }

    MemoryContextSwitchTo(oldcontext);
}
```