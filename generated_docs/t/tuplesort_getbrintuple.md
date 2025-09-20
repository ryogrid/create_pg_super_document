# tuplesort_getbrintuple

## Location
[src/backend/utils/sort/tuplesortvariants.c:970-1017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L970-L1017)

## Overview
Fetches the next BRIN tuple from a tuplesort state in either forward or backward direction, returning the tuple for BRIN index operations.

## Definition

```c
BrinTuple *
tuplesort_getbrintuple(Tuplesortstate *state, Size *len, bool forward)
```
## Detailed Description
This function is part of PostgreSQL's tuplesort framework, specifically designed to retrieve BRIN (Block Range Index) tuples from a sorted tuple collection. It serves as the interface between the generic tuplesort machinery and BRIN-specific tuple handling. The function manages memory context switching to ensure proper allocation and handles the conversion from the internal SortTuple format to the external BrinTuple format that callers expect.

The function operates by calling the common tuple retrieval mechanism and then unwrapping the BRIN-specific tuple data. It ensures that the returned tuple belongs to the tuplesort's memory context and provides the tuple length information through an output parameter.

## Parameters / Member Variables
- : Tuplesortstate pointer representing the active tuplesort operation containing BRIN tuples
- : Output parameter that receives the length of the returned BRIN tuple
- : Boolean flag indicating the direction of retrieval (true for forward, false for backward)

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [tuplesort_gettuple_common](tuplesort_gettuple_common.md)
  - [BrinSortTuple](../B/BrinSortTuple.md) (struct type)
  - SortTuple (struct type)
- Called from (representative examples):
  - [_brin_parallel_merge](../b/_brin_parallel_merge.md)

## Notes and Other Information
- The returned tuple belongs to the tuplesort memory context and must not be freed by the caller
- The caller cannot rely on the tuple remaining valid after further manipulation of the tuplesort state
- Returns NULL when no more tuples are available in the specified direction
- The function temporarily switches to the sort context for memory operations and restores the previous context before returning
- This is part of the tuplesortvariants.c file which contains specialized tuple sort implementations for different data types