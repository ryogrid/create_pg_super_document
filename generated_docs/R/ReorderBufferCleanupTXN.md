# ReorderBufferCleanupTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:1531-1650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1531-L1650)

## Overview
Performs comprehensive cleanup of a transaction and all its associated resources, including subtransactions, changes, snapshots, and memory, typically after the transaction has committed or aborted.

## Definition

```c
static void
ReorderBufferCleanupTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
```
## Detailed Description
This function systematically cleans up all resources associated with a transaction in the reorder buffer. The cleanup process follows a specific order to ensure proper resource management:

1. **Subtransaction cleanup**: Recursively cleans up all subtransactions associated with the main transaction. Subtransactions are always flattened to the toplevel transaction, so recursion is limited to one level.

2. **Change cleanup**: Iterates through all changes in the transaction, validates they belong to the correct transaction, calculates memory freed, and returns changes to the buffer's free pool. Memory counter updates are batched for efficiency.

3. **Tuple CID cleanup**: Cleans up tuple command IDs (tuplecids) stored for decoding catalog snapshot access. These are special internal changes used for handling catalog modifications.

4. **Snapshot cleanup**: 
   - Releases the base snapshot by decrementing its reference count
   - Cleans up the snapshot for the last streamed run if the transaction was streamed

5. **List removal**: Removes the transaction from various lists including the LSN-ordered list of toplevel transactions and the list of catalog-modifying transactions.

6. **Hash table removal**: Removes the transaction entry from the hash table lookup structure.

7. **Serialized data cleanup**: If the transaction was serialized to disk, cleans up the associated spilled files.

8. **Memory deallocation**: Finally returns the transaction structure itself to the memory pool.

## Parameters / Member Variables
- : Pointer to the main ReorderBuffer structure managing the reordering operations
- : Pointer to the ReorderBufferTXN structure representing the transaction to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify
  - dlist_container
  - rbtxn_is_known_subxact
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md) (recursive call)
  - [ReorderBufferChangeSize](ReorderBufferChangeSize.md)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)
  - ReorderBufferChangeMemoryUpdate
  - [SnapBuildSnapDecRefcount](../S/SnapBuildSnapDecRefcount.md)
  - [dlist_delete](../d/dlist_delete.md)
  - rbtxn_is_streamed
  - [ReorderBufferFreeSnap](ReorderBufferFreeSnap.md)
  - rbtxn_has_catalog_changes
  - [dclist_delete_from](../d/dclist_delete_from.md)
  - [hash_search](../h/hash_search.md)
  - rbtxn_is_serialized
  - [ReorderBufferRestoreCleanup](ReorderBufferRestoreCleanup.md)
  - [ReorderBufferReturnTXN](ReorderBufferReturnTXN.md)
- Called from (representative examples):
  - [ReorderBufferStreamCommit](ReorderBufferStreamCommit.md)
  - [ReorderBufferReplay](ReorderBufferReplay.md)
  - [ReorderBufferFinishPrepared](ReorderBufferFinishPrepared.md)
  - [ReorderBufferAbort](ReorderBufferAbort.md)
  - [ReorderBufferAbortOld](ReorderBufferAbortOld.md)
  - [ReorderBufferForget](ReorderBufferForget.md)

## Notes and Other Information
- This is a static function, accessible only within reorderbuffer.c
- The function is recursive but only to one level deep due to subtransaction flattening
- Memory counter updates are batched to avoid frequent heap maintenance overhead
- The function handles both regular and streamed transactions differently for snapshot cleanup
- Cleanup order is critical: resources must be freed before removing hash table entries
- The function includes multiple assertions to verify data integrity during cleanup
- Part of PostgreSQL's logical replication infrastructure for managing transaction ordering