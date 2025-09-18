# ReorderBufferTransferSnapToParent

## Location
src/backend/replication/logical/reorderbuffer.c: 1161 - 1214

## Overview
Transfers a base snapshot from a subtransaction to its parent top-level transaction when the subtransaction has an earlier or the only base snapshot.

## Definition
```c
static void ReorderBufferTransferSnapToParent(ReorderBufferTXN *txn, ReorderBufferTXN *subtxn)
```

## Detailed Description
This function manages the transfer of base snapshots from subtransactions to their parent top-level transactions during logical replication. The transfer occurs when either:
1. The top-level transaction has no base snapshot
2. The subtransactions base snapshot has an earlier LSN than the top-level transactions base snapshot

This situation can arise when there are no changes in the top-level transaction but there are changes in the subtransaction, or when the first change in the subtransaction has an earlier LSN than the first change in the top-level transaction and their kinship is discovered later.

The function ensures that only top-level transactions receive further snapshots by transferring the appropriate snapshot upward and clearing the subtransactions snapshot regardless of whether a transfer occurs. This optimization avoids queueing extra snapshots to transactions known as subtransactions.

## Parameters / Member Variables
- `txn`: The top-level parent transaction that may receive the transferred snapshot
- `subtxn`: The subtransaction whose base snapshot is being considered for transfer

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildSnapDecRefcount](../S/SnapBuildSnapDecRefcount.md) (decrements snapshot reference count)
  - [dlist_delete](../d/dlist_delete.md) (removes nodes from doubly-linked lists)
  - [dlist_insert_before](../d/dlist_insert_before.md) (inserts nodes into doubly-linked lists)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (transaction structure type)
- Called from (representative examples):
  - ReorderBufferAssignChild
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md)
  - IsInsertOrUpdate

## Notes and Other Information
- This is a static function within reorderbuffer.c, used internally for snapshot management
- The function includes an assertion to verify the subtransaction belongs to the specified parent
- After the operation, the subtransactions base_snapshot is always set to NULL and base_snapshot_lsn to InvalidXLogRecPtr
- The function manipulates doubly-linked lists to maintain proper ordering of snapshots by LSN
- This is part of PostgreSQLs logical replication infrastructure for maintaining consistent snapshots across transaction hierarchies