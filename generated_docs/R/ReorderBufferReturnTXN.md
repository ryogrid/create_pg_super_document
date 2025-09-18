# ReorderBufferReturnTXN

## Location
src/backend/replication/logical/reorderbuffer.c: 455 - 502

## Overview
Frees and cleans up a ReorderBufferTXN structure, deallocating all associated memory and clearing cache references.

## Definition


## Detailed Description
ReorderBufferReturnTXN performs complete cleanup of a transaction structure. It first clears any cache references in the reorder buffer that point to this transaction, then systematically deallocates all dynamically allocated data within the transaction including the global transaction ID (gid), tuple command ID hash table, invalidation arrays, and toast-related data. The function includes an assertion to ensure all changes have been properly deallocated before freeing the transaction itself.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer containing the transaction
- `txn`: Pointer to the ReorderBufferTXN structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (called multiple times for different fields)
  - [hash_destroy](../h/hash_destroy.md)
  - [ReorderBufferToastReset](ReorderBufferToastReset.md)
- Called from (representative examples):
  - IsInsertOrUpdate  
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)

## Notes and Other Information
- This is a static (internal) function, not part of the public API
- Clears the last transaction cache (by_txn_last_xid/by_txn_last_txn) if this transaction was cached
- Safely handles NULL pointers for optional fields (gid, tuplecid_hash, invalidations)
- Includes defensive programming with assertion to verify all changes were deallocated (size == 0)
- Calls ReorderBufferToastReset to clean up any TOAST-related hash tables
- Complementary function to ReorderBufferGetTXN
- Memory for the transaction structure itself is freed with pfree, not returned to a pool