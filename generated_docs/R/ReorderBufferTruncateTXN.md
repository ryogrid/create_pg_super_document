# ReorderBufferTruncateTXN

## Location
src/backend/replication/logical/reorderbuffer.c: 1651 - 1777

## Overview
Discards changes from a transaction while preserving essential metadata (transactions, tuplecids, invalidations, and snapshots), typically used after streaming or decoding transactions at PREPARE time.

## Definition


## Detailed Description
This function performs selective cleanup of transaction data, removing changes while preserving the transaction structure and metadata needed for later processing. The operation differs from full cleanup by maintaining transaction state for future operations like commit or rollback.

The function performs several key operations:

1. **Subtransaction processing**: Recursively truncates all subtransactions, which are flattened to the toplevel transaction (limiting recursion to one level).

2. **Change removal**: Removes all changes from the transaction's change list, calculating freed memory for batch counter updates, and returning changes to the buffer's free pool.

3. **Streaming state management**: Marks the transaction as streamed based on specific rules:
   - Top-level transactions are always marked as streamed
   - Subtransactions are only marked if they contained changes
   - This prevents sending abort messages for unknown XIDs downstream

4. **Prepared transaction handling**: When  is true, additionally removes tuplecids since they're only needed for invalidation at rollback or commit prepared, not for the prepare phase itself.

5. **Hash table cleanup**: Destroys the (relfilelocator, ctid) hashtable to prevent memory leaks, as this data is no longer needed after truncation.

6. **Serialized data cleanup**: If the transaction was serialized to disk, cleans up the spilled files and updates serialization flags appropriately.

7. **Entry counter reset**: Resets both in-memory and total entry counters to zero.

## Parameters / Member Variables
- : Pointer to the main ReorderBuffer structure managing the reordering operations
- : Pointer to the ReorderBufferTXN structure representing the transaction to be truncated
- : Boolean flag indicating whether the transaction has been decoded at prepare time, controlling whether tuplecids should be removed

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify
  - dlist_container
  - rbtxn_is_known_subxact
  - ReorderBufferTruncateTXN (recursive call)
  - dlist_delete
  - ReorderBufferChangeSize
  - ReorderBufferReturnChange
  - ReorderBufferChangeMemoryUpdate
  - rbtxn_is_toptxn
  - hash_destroy
  - rbtxn_is_serialized
  - ReorderBufferRestoreCleanup
- Called from (representative examples):
  - ReorderBufferStreamCommit
  - ReorderBufferResetTXN
  - CHANGES_THRESHOLD (in reorderbuffer.c)

## Notes and Other Information
- This is a static function, accessible only within reorderbuffer.c
- The function is recursive but limited to one level deep due to subtransaction flattening
- Unlike full cleanup, this preserves the transaction structure for later processing
- Memory counter updates are batched for efficiency
- The streaming flag logic ensures proper coordination with downstream replication consumers
- Tuplecid removal is conditional based on the prepared state to optimize memory usage
- The function maintains important flags like RBTXN_IS_SERIALIZED_CLEAR for accurate statistics
- Part of PostgreSQL's logical replication infrastructure for transaction state management