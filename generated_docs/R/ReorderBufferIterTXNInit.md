# ReorderBufferIterTXNInit

## Location
[src/backend/replication/logical/reorderbuffer.c:1280-1407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1280-L1407)

## Overview
Allocates and initializes an iterator for traversing changes from a transaction and its subtransactions in LSN order using a k-way merge algorithm.

## Definition
```c
static void ReorderBufferIterTXNInit(ReorderBuffer *rb, ReorderBufferTXN *txn, ReorderBufferIterTXNState *volatile *iter_state)
```

## Detailed Description
This function creates and initializes an iterator state for efficiently processing changes from a transaction hierarchy in Log Sequence Number (LSN) order. It implements a k-way merge algorithm using a binary heap data structure to merge changes from the top-level transaction and all its subtransactions.

The function performs several key operations:
1. Counts transactions with changes to determine heap size
2. Allocates memory for the iterator state and entry array
3. Handles serialized transactions by restoring changes from disk
4. Creates a binary heap using ReorderBufferIterCompare for ordering
5. Populates the heap with the first change from each transaction
6. Assembles the heap for efficient traversal

The iterator state is returned through the iter_state parameter rather than as a return value to ensure proper cleanup in exception handling scenarios. The function includes assertions to verify that changes within each transaction are properly ordered by LSN.

## Parameters / Member Variables
- `rb`: The reorder buffer instance managing the transactions
- `txn`: The top-level transaction to iterate over (including its subtransactions)
- `iter_state`: Pointer to a volatile pointer that will receive the allocated iterator state

## Dependencies
- Functions called/Symbols referenced:
  - [AssertChangeLsnOrder](../A/AssertChangeLsnOrder.md) (validates LSN ordering within transactions)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (allocates zeroed memory)
  - dlist_foreach, dlist_container, dlist_head_element (doubly-linked list operations)
  - [binaryheap_allocate](../b/binaryheap_allocate.md), binaryheap_add_unordered, binaryheap_build (binary heap operations)
  - [ReorderBufferIterCompare](ReorderBufferIterCompare.md) (heap comparison function)
  - [ReorderBufferSerializeTXN](ReorderBufferSerializeTXN.md), ReorderBufferRestoreChanges (serialization handling)
  - rbtxn_is_serialized (checks if transaction is serialized)
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (during transaction processing)
  - IsInsertOrUpdate (during change processing)

## Notes and Other Information
- This is a static function used internally for iterator initialization
- The function handles both in-memory and serialized (disk-based) transactions
- Uses a binary heap for efficient k-way merge of transaction changes
- Memory allocation includes space for ReorderBufferIterTXNEntry array based on transaction count
- Initializes file descriptors to -1 and segment numbers to 0 for all entries
- The heap is populated in unordered fashion first, then assembled for efficiency
- Part of PostgreSQLs logical replication infrastructure for ordered change processing
- Exception-safe design ensures iterator state is available for cleanup even if initialization fails