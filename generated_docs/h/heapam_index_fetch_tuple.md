# heapam_index_fetch_tuple

## Location
[src/backend/access/heap/heapam_handler.c:113-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L113-L180)

## Overview
Fetches a tuple from a heap relation using an item pointer (TID) obtained from an index, handling HOT (Heap-Only Tuples) chain traversal and visibility checking according to the provided snapshot.

## Definition

```c
static bool
heapam_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
```
## Detailed Description
This function is the core implementation of tuple fetching for heap tables within PostgreSQL's table access method framework. It retrieves a tuple from the heap using a tuple identifier (TID) obtained from an index scan. The function handles complex scenarios including HOT chain traversal, buffer management, page pruning, and MVCC visibility checking. It uses heap_hot_search_buffer() to find the appropriate tuple version in a HOT chain that satisfies the given snapshot's visibility requirements. The function manages buffer switching when accessing different pages and performs page pruning optimization when encountering a new page.

## Parameters / Member Variables
- : Pointer to IndexFetchTableData structure containing scan state
- : ItemPointer (TID) identifying the tuple location from the index
- : Snapshot for MVCC visibility checking
- : TupleTableSlot to store the retrieved tuple
- : Output parameter indicating if there are more tuples in the HOT chain
- : Output parameter indicating if all tuples in the HOT chain are dead

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseAndReadBuffer (buffer management)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md) (extract block number from TID)
  - [heap_page_prune_opt](heap_page_prune_opt.md) (page pruning optimization)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking for concurrency control)
  - [heap_hot_search_buffer](heap_hot_search_buffer.md) (HOT chain search and visibility checking)
  - IsMVCCSnapshot (snapshot type checking)
  - [ExecStoreBufferHeapTuple](../E/ExecStoreBufferHeapTuple.md) (store tuple in slot)
  - TTS_IS_BUFFERTUPLE (slot type verification)
- Called from (representative examples):
  - Part of TableAmRoutine structure as a callback function
  - Referenced by SampleHeapTupleVisible

## Notes and Other Information
- Central function for index-guided tuple retrieval in heap access method
- Handles HOT (Heap-Only Tuples) chain traversal for updated tuples
- Performs page pruning optimization when switching to a new buffer page
- Uses proper buffer locking to ensure consistency during visibility checks
- The call_again parameter enables iteration through HOT chains in non-MVCC snapshots
- Returns true if a visible tuple was found, false otherwise
- Buffer switching logic is optimized to avoid unnecessary operations when already on correct page