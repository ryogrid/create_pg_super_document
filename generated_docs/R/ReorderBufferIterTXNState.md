# ReorderBufferIterTXNState

## Location
[src/backend/replication/logical/reorderbuffer.c:167-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L167-L173)

## Overview
ReorderBufferIterTXNState is a structure that manages the iteration state for processing changes across multiple transactions in PostgreSQL's logical replication reorder buffer.

## Definition

```c
typedef struct ReorderBufferIterTXNState
{
	binaryheap *heap;
	Size		nr_txns;
	dlist_head	old_change;
	ReorderBufferIterTXNEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ReorderBufferIterTXNState;
```
## Detailed Description
This structure serves as the control mechanism for iterating through changes from multiple transactions in a coordinated manner during logical replication. It maintains a binary heap to efficiently order changes by their commit LSN (Log Sequence Number), ensuring that changes are processed in the correct chronological order across transaction boundaries. The structure supports iteration over multiple concurrent transactions while preserving the global ordering required for consistent logical replication.

## Parameters / Member Variables
- `*heap`: Binary heap data structure used to maintain ordering of transaction changes by commit LSN
- `nr_txns`: Number of transactions currently being tracked in the iteration state
- `old_change`: Doubly-linked list head for managing previously processed changes that may need to be revisited
- `entries[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing ReorderBufferIterTXNEntry structures, one for each transaction being iterated
## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap](../b/binaryheap.md)
  - [dlist_head](../d/dlist_head.md)
  - [ReorderBufferIterTXNEntry](ReorderBufferIterTXNEntry.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md)
  - [ReorderBufferIterTXNNext](ReorderBufferIterTXNNext.md)
  - [ReorderBufferIterTXNFinish](ReorderBufferIterTXNFinish.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)

## Notes and Other Information
- This structure is central to the logical replication's ability to maintain transaction ordering across multiple concurrent transactions
- The binary heap ensures efficient O(log n) operations for maintaining the correct change ordering
- The flexible array member allows for dynamic sizing based on the number of concurrent transactions
- Used internally by the reorder buffer system to coordinate multi-transaction iteration during logical decoding