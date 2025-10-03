# tuplesort_gettupleslot

## Location
[src/backend/utils/sort/tuplesortvariants.c:890-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L890-L927)

## Overview
Fetches the next tuple from a sorted tuplesort operation in either forward or backward direction and stores it in a provided TupleTableSlot.

## Definition
```c
bool tuplesort_gettupleslot(Tuplesortstate *state, bool forward, bool copy, 
                           TupleTableSlot *slot, Datum *abbrev)
```

## Detailed Description
This function retrieves the next tuple from a completed sorting operation and places it into a TupleTableSlot for further processing. It supports both forward and backward iteration through the sorted results. The function can optionally copy the tuple into the caller's memory context for safety, or provide a direct pointer for efficiency. When abbreviation was used during sorting, the abbreviated value can be returned to the caller for cheap inequality comparisons without requiring full tuple comparison.

## Parameters / Member Variables
- `state`: Tuplesortstate pointer representing the sorting operation to fetch from
- `forward`: Boolean indicating direction of iteration (true for forward, false for backward)
- `copy`: Boolean controlling whether to copy the tuple into caller's memory context
- `slot`: TupleTableSlot to store the retrieved tuple
- `abbrev`: Optional pointer to receive the abbreviated key value for optimization

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [tuplesort_gettuple_common](tuplesort_gettuple_common.md)
  - [heap_copy_minimal_tuple](../h/heap_copy_minimal_tuple.md)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
- Called from (representative examples):
  - [fetch_input_tuple](../f/fetch_input_tuple.md)
  - [process_ordered_aggregate_multi](../p/process_ordered_aggregate_multi.md)
  - [switchToPresortedPrefixMode](../s/switchToPresortedPrefixMode.md)
  - [ExecIncrementalSort](../E/ExecIncrementalSort.md)
  - [ExecSort](../E/ExecSort.md)
  - [hypothetical_rank_common](../h/hypothetical_rank_common.md)

## Notes and Other Information
- Returns true if a tuple was successfully retrieved, false if no more tuples are available
- The copy parameter determines tuple lifetime: copy=true creates a safe copy, copy=false provides efficient but potentially volatile access
- Abbreviated keys are provided when available to enable fast inequality checks without full tuple comparison
- Used extensively in executor nodes that need to process sorted tuple streams
- Part of the high-level tuplesort interface for tuple-based sorting operations

## Simplified Source

```c
bool tuplesort_gettupleslot(Tuplesortstate *state, bool forward, bool copy,
                           TupleTableSlot *slot, Datum *abbrev)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    SortTuple stup;

    // Switch to sort context for memory operations
    MemoryContext oldcontext = MemoryContextSwitchTo(base->sortcontext);

    // Try to get the next tuple from the sort
    if (!tuplesort_gettuple_common(state, forward, &stup))
        stup.tuple = NULL;  // No more tuples available

    // Restore original memory context
    MemoryContextSwitchTo(oldcontext);

    if (stup.tuple)
    {
        // Provide abbreviated key if available and requested
        if (base->sortKeys->abbrev_converter && abbrev)
            *abbrev = stup.datum1;

        // Copy tuple if requested (safer but slower)
        if (copy)
            stup.tuple = heap_copy_minimal_tuple((MinimalTuple) stup.tuple);

        // Store tuple in the provided slot
        ExecStoreMinimalTuple((MinimalTuple) stup.tuple, slot, copy);
        return true;  // Successfully retrieved tuple
    }
    else
    {
        // No tuple available - clear slot and return false
        ExecClearTuple(slot);
        return false;  // End of sorted data
    }
}
```