# tuplesort_puttupleslot

## Location
[src/backend/utils/sort/tuplesortvariants.c:669-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L669-L708)

## Overview
Accepts a tuple from a TupleTableSlot and adds it to the tuplesort for sorting, converting the slot data to internal sort representation with optimized memory management.

## Definition
```c
void tuplesort_puttupleslot(Tuplesortstate *state, TupleTableSlot *slot)
```

## Detailed Description
This function takes a tuple stored in a TupleTableSlot and prepares it for sorting by converting it to the internal SortTuple representation. It creates a MinimalTuple copy of the slot data to reduce memory overhead, extracts the first sort key value for optimization purposes, and calculates the memory usage for proper memory management. The function handles memory context switching to ensure data is allocated in the appropriate sort context and supports both standard and bump memory allocation strategies based on sort options.

## Parameters / Member Variables
- `state`: The tuplesort state object managing the sort operation
- `slot`: TupleTableSlot containing the tuple data to be added to the sort

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [ExecCopySlotMinimalTuple](../E/ExecCopySlotMinimalTuple.md)
  - [heap_getattr](../h/heap_getattr.md)
  - TupleSortUseBumpTupleCxt
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - [tuplesort_puttuple_common](tuplesort_puttuple_common.md)
- Called from (representative examples):
  - [ExecEvalAggOrderedTransTuple](../E/ExecEvalAggOrderedTransTuple.md)
  - [fetch_input_tuple](../f/fetch_input_tuple.md)
  - [ExecIncrementalSort](../E/ExecIncrementalSort.md)
  - [ExecSort](../E/ExecSort.md)
  - [ordered_set_transition_multi](../o/ordered_set_transition_multi.md)

## Notes and Other Information
- Always creates a copy of the input data - caller does not need to preserve the original
- Converts TupleTableSlot to MinimalTuple format for memory efficiency
- Extracts the first sort column value (datum1) for performance optimization
- Handles memory chunk size calculation differently for bump allocation contexts
- Supports abbreviation optimization when available and the first key is not null
- Uses appropriate memory context switching to manage sort-related allocations
- Part of the standard interface for adding tuples to sorts from executor nodes

## Simplified Source

```c
void
tuplesort_puttupleslot(Tuplesortstate *state, TupleTableSlot *slot)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);
    TupleDesc tupDesc = (TupleDesc) base->arg;
    SortTuple stup;
    MinimalTuple tuple;
    HeapTupleData htup;
    Size tuplen;

    // Copy slot data to minimal tuple format
    tuple = ExecCopySlotMinimalTuple(slot);
    stup.tuple = (void *) tuple;

    // Extract first sort key for optimization
    htup.t_len = tuple->t_len + MINIMAL_TUPLE_OFFSET;
    htup.t_data = (HeapTupleHeader) ((char *) tuple - MINIMAL_TUPLE_OFFSET);
    stup.datum1 = heap_getattr(&htup, base->sortKeys[0].ssup_attno, tupDesc, &stup.isnull1);

    // Calculate tuple size based on memory context type
    if (TupleSortUseBumpTupleCxt(base->sortopt))
        tuplen = MAXALIGN(tuple->t_len);
    else
        tuplen = GetMemoryChunkSpace(tuple);

    // Add to sort with abbreviation if available
    tuplesort_puttuple_common(state, &stup,
                              base->sortKeys->abbrev_converter && !stup.isnull1,
                              tuplen);

    MemoryContextSwitchTo(oldcontext);
}
```