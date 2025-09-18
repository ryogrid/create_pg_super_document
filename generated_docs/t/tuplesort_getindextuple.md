# tuplesort_getindextuple

## Location
[src/backend/utils/sort/tuplesortvariants.c:949-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L949-L969)

## Overview
Fetches the next index tuple from a sorted tuplesort operation in either forward or backward direction, returning a direct pointer to the IndexTuple.

## Definition
```c
IndexTuple tuplesort_getindextuple(Tuplesortstate *state, bool forward)
```

## Detailed Description
This function retrieves the next IndexTuple from a completed sorting operation, specifically designed for index construction and maintenance operations. Like tuplesort_getheaptuple, it provides direct access to the tuple data without copying, returning a pointer that belongs to the tuplesort's memory context. The function is optimized for index building scenarios where sorted index tuples need to be processed sequentially with minimal overhead.

## Parameters / Member Variables
- `state`: Tuplesortstate pointer representing the sorting operation to fetch from
- `forward`: Boolean indicating direction of iteration (true for forward, false for backward)

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [tuplesort_gettuple_common](tuplesort_gettuple_common.md)
- Called from (representative examples):
  - [gist_indexsortbuild](../g/gist_indexsortbuild.md)
  - [_h_indexbuild](../h/_h_indexbuild.md)
  - _bt_load (multiple locations in B-tree index building)

## Notes and Other Information
- Returns NULL if no more tuples are available
- The returned IndexTuple pointer is only valid until the next tuplesort operation
- Extensively used in index construction algorithms across different index types (GiST, Hash, B-tree)
- Provides efficient sequential access for sorted index tuple processing
- Similar to tuplesort_getheaptuple but specifically typed for IndexTuple usage
- Part of the specialized tuplesort interface for index building operations
- No copying overhead makes it suitable for high-performance index construction