# tuplesort_putheaptuple

## Location
[src/backend/utils/sort/tuplesortvariants.c:709-751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L709-L751)

## Overview
Accepts a HeapTuple and adds it to the tuplesort for sorting, specifically designed for heap tuple sorting operations like table clustering.

## Definition
```c
void tuplesort_putheaptuple(Tuplesortstate *state, HeapTuple tup)
```

## Detailed Description
This function takes a HeapTuple and prepares it for sorting by creating a complete copy of the tuple data and setting up the appropriate sort keys. It is specifically designed for operations that work with heap tuples, such as table clustering operations. The function extracts the first sort key value when available (controlled by haveDatum1 flag) using the index attribute information stored in the sort arguments. It handles memory management by calculating tuple size appropriately for different allocation contexts and supports abbreviation optimization for performance when applicable.

## Parameters / Member Variables
- `state`: The tuplesort state object managing the sort operation
- `tup`: HeapTuple to be added to the sort

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_getattr](../h/heap_getattr.md)
  - TupleSortUseBumpTupleCxt
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - [tuplesort_puttuple_common](tuplesort_puttuple_common.md)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)

## Notes and Other Information
- Always creates a complete copy of the input HeapTuple data
- Uses TuplesortClusterArg to access index information for key extraction
- Conditionally extracts the first sort key value based on haveDatum1 flag
- Calculates memory usage differently for bump allocation contexts vs. standard contexts
- Supports abbreviation optimization when conditions are met (haveDatum1, abbrev_converter available, and key is not null)
- Primarily used in table clustering operations where heap tuples need to be sorted by index key values
- Memory context management ensures proper allocation in sort-specific contexts

## Simplified Source

```c
void
tuplesort_putheaptuple(Tuplesortstate *state, HeapTuple tup)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);
    TuplesortClusterArg *arg = (TuplesortClusterArg *) base->arg;

    // Copy the tuple into sort storage
    HeapTuple tup_copy = heap_copytuple(tup);
    SortTuple stup;
    stup.tuple = (void *) tup_copy;

    // Extract first sort key value if available for optimization
    if (base->haveDatum1)
    {
        stup.datum1 = heap_getattr(tup_copy,
                                  arg->indexInfo->ii_IndexAttrNumbers[0],
                                  arg->tupDesc,
                                  &stup.isnull1);
    }

    // Calculate memory usage based on allocation context
    Size tuplen;
    if (TupleSortUseBumpTupleCxt(base->sortopt))
        tuplen = MAXALIGN(HEAPTUPLESIZE + tup_copy->t_len);
    else
        tuplen = GetMemoryChunkSpace(tup_copy);

    // Add tuple to sort with abbreviation support if available
    tuplesort_puttuple_common(state, &stup,
                             base->haveDatum1 &&
                             base->sortKeys->abbrev_converter &&
                             !stup.isnull1, tuplen);

    MemoryContextSwitchTo(oldcontext);
}
```