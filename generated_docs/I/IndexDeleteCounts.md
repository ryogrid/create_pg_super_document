# IndexDeleteCounts

## Location
[src/backend/access/heap/heapam.c:209-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L209-L214)

## Overview
IndexDeleteCounts is a struct used by heap_index_delete_tuples to track groups of TIDs (tuple identifiers) during bottom-up index deletion operations, helping determine which heap blocks to visit for optimal deletion efficiency.

## Definition


## Detailed Description
The IndexDeleteCounts structure is used internally by the heap access method's index deletion functionality to optimize bottom-up index deletion operations. It groups TIDs by heap blocks and tracks metadata about each group to help determine the most efficient order for visiting heap blocks during deletion. This structure is specifically designed to support the bottom-up deletion strategy where the system tries to visit heap blocks in an order that maximizes the likelihood of finding deletable tuples while minimizing I/O operations.

The struct helps the deletion process make informed decisions about which blocks are worth visiting by tracking both the total number of TIDs in a group and how many of those are considered "promising" (likely to be deletable based on various heuristics).

## Parameters / Member Variables
- : The number of TIDs in this group that are considered "promising" candidates for deletion based on preliminary analysis
- : The total number of TIDs (tuple identifiers) grouped together for this heap block
- : The offset/index to the first deltid (deletion TID) in this group within the larger deltids array

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure definition with no direct function calls)
- Called from (representative examples):
  - [bottomup_nblocksfavorable](../b/bottomup_nblocksfavorable.md) (src/backend/access/heap/heapam.c:8537, 8557)
  - [bottomup_sort_and_shrink_cmp](../b/bottomup_sort_and_shrink_cmp.md) (src/backend/access/heap/heapam.c:8582, 8583)
  - [bottomup_sort_and_shrink](../b/bottomup_sort_and_shrink.md) (src/backend/access/heap/heapam.c:8655, 8666, 8726, 8737, 8748)

## Notes and Other Information
- This struct is specifically used in the context of bottom-up index deletion, which is an optimization strategy for bulk index tuple deletion operations
- The structure is defined in src/backend/access/heap/heapam.c:209-214
- It's used in conjunction with the heap_index_delete_tuples function to implement efficient batch deletion of index entries
- The "promising" TID concept helps the system make better decisions about which heap blocks to prioritize during deletion operations, reducing unnecessary I/O
- This is part of PostgreSQL's table access method (tableam) interface implementation for heap tables