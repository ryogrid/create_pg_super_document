# SnapBuildDistributeSnapshotAndInval

## Location
src/backend/replication/logical/snapbuild.c: 870 - 966

## Overview
Distributes new snapshots and invalidation messages to all in-progress transactions during logical decoding to ensure they see current catalog contents after a transaction commits.

## Definition
```c
static void SnapBuildDistributeSnapshotAndInval(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid)
```

## Detailed Description
This function is a critical component of PostgreSQL logical decoding that ensures catalog consistency across concurrent transactions. When a transaction that has modified system catalogs commits, all other in-progress transactions must be updated to see the new catalog state.

The function performs these key operations:
1. Iterates through all top-level transactions currently being decoded
2. Skips transactions without base snapshots or prepared transactions
3. Distributes fresh snapshots to eligible transactions so they see current catalog contents
4. Propagates invalidation messages from the committed transaction to other in-progress transactions

This mechanism is essential because in-progress transactions must use catalog contents that are compatible with newer catalog changes. Without this distribution, transactions could use stale catalog information leading to inconsistent decoding results.

The invalidation message distribution ensures that cached catalog information is properly invalidated, forcing transactions to rebuild their catalog caches with current data.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure managing snapshot building state
- `lsn`: Log Sequence Number where the snapshot and invalidations should be applied
- `xid`: Transaction ID of the transaction that just committed and triggered this distribution

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_iter](../d/dlist_iter.md), dlist_foreach, dlist_container (doubly-linked list operations)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (transaction structure)
  - [ReorderBufferXidHasBaseSnapshot](../R/ReorderBufferXidHasBaseSnapshot.md)
  - rbtxn_prepared, rbtxn_skip_prepared (transaction state checks)
  - [SnapBuildSnapIncRefcount](SnapBuildSnapIncRefcount.md)
  - ReorderBufferAddSnapshot
  - [ReorderBufferGetInvalidations](../R/ReorderBufferGetInvalidations.md)
  - ReorderBufferAddDistributedInvalidations
  - SharedInvalidationMessage (invalidation message structure)
- Called from (representative examples):
  - SnapBuildCommitTxn (snapbuild.c:1235)

## Notes and Other Information
- Function is declared static, indicating internal use within snapbuild.c
- Handles subtransactions correctly through ReorderBufferAssignChild() mechanism
- Skips prepared transactions as they should not see new catalog contents
- Only distributes invalidations from the specific committing transaction to avoid duplicate processing
- Critical for maintaining MVCC semantics and catalog consistency during logical replication
- Part of the snapshot building infrastructure that enables consistent logical decoding