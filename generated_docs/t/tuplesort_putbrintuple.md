# tuplesort_putbrintuple

## Location
[src/backend/utils/sort/tuplesortvariants.c:788-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L788-L825)

## Overview
Collects one BRIN tuple while collecting input data for sorting operations, handling the allocation and initialization of BRIN-specific sort tuples.

## Definition

```c
void
tuplesort_putbrintuple(Tuplesortstate *state, BrinTuple *tuple, Size size)
```
## Detailed Description
This function is a specialized variant of tuple insertion for BRIN (Block Range Index) tuples during the sorting process. It allocates memory for a BrinSortTuple structure, copies the provided BRIN tuple data, and sets up the sorting key (block number) for comparison purposes. The function handles memory management by switching to the appropriate memory context and calculating the correct tuple length based on whether bump contexts are being used.

## Parameters / Member Variables
- `*state`: Tuplesortstate pointer representing the current sorting operation state
- `*tuple`: BrinTuple pointer to the BRIN tuple to be inserted into the sort
- `size`: Size of the BRIN tuple data in bytes
## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - BRINSORTTUPLE_SIZE
  - [palloc](../p/palloc.md)
  - memcpy
  - TupleSortUseBumpTupleCxt
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - [tuplesort_puttuple_common](tuplesort_puttuple_common.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [form_and_spill_tuple](../f/form_and_spill_tuple.md)

## Notes and Other Information
- Uses the BRIN tuple's block number (bt_blkno) as the primary sorting datum
- Handles memory allocation differently for bump contexts vs regular contexts
- Part of the specialized tuple sorting infrastructure for BRIN indexes
- Switches memory contexts to ensure proper allocation in the tuple context

## Simplified Source

```c
void tuplesort_putbrintuple(Tuplesortstate *state, BrinTuple *tuple, Size size)
{
    SortTuple stup;
    BrinSortTuple *bstup;
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);

    // Allocate and copy BRIN tuple
    bstup = palloc(BRINSORTTUPLE_SIZE(size));
    bstup->tuplen = size;
    memcpy(&bstup->tuple, tuple, size);

    // Set up sort tuple with block number as primary sort key
    stup.tuple = bstup;
    stup.datum1 = tuple->bt_blkno;
    stup.isnull1 = false;

    // Calculate tuple size for memory management
    Size tuplen;
    if (TupleSortUseBumpTupleCxt(base->sortopt))
        tuplen = MAXALIGN(BRINSORTTUPLE_SIZE(size));
    else
        tuplen = GetMemoryChunkSpace(bstup);

    // Add to sort with abbreviation support
    tuplesort_puttuple_common(state, &stup,
                             base->sortKeys && base->sortKeys->abbrev_converter && !stup.isnull1,
                             tuplen);

    MemoryContextSwitchTo(oldcontext);
}
```