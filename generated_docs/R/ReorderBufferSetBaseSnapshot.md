# ReorderBufferSetBaseSnapshot

## Location
[src/backend/replication/logical/reorderbuffer.c:3201-3231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3201-L3231)

## Overview
ReorderBufferSetBaseSnapshot establishes the base snapshot for a transaction in the reorder buffer, ensuring proper catalog visibility for logical decoding and automatically handling subtransaction delegation to top-level transactions.

## Definition
```c
void ReorderBufferSetBaseSnapshot(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, Snapshot snap)
```

## Detailed Description
This function sets the fundamental snapshot that will be used for catalog visibility throughout a transaction's lifetime during logical decoding. The base snapshot is crucial for maintaining consistent visibility rules when decoding changes from the WAL stream.

The function implements intelligent subtransaction handling: if the provided transaction ID is detected as a subtransaction, it automatically delegates the base snapshot to the top-level transaction instead. This ensures that all subtransactions share the same catalog visibility baseline as their parent transaction.

Once the base snapshot is established, the transaction is added to the reorder buffer's list of transactions ordered by base snapshot LSN, maintaining the proper temporal ordering needed for efficient processing.

## Parameters
- `rb`: Pointer to the ReorderBuffer instance managing transactions
- `xid`: The TransactionId for which to set the base snapshot
- `lsn`: The Log Sequence Number where this base snapshot becomes effective
- `snap`: The Snapshot that will serve as the base catalog visibility reference (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - rbtxn_is_known_subxact
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [AssertTXNLsnOrder](../A/AssertTXNLsnOrder.md)
- Called from (representative examples):
  - [SnapBuildProcessChange](../S/SnapBuildProcessChange.md)
  - [SnapBuildCommitTxn](../S/SnapBuildCommitTxn.md)

## Notes and Other Information
- The function asserts that the provided snapshot is not NULL
- Subtransactions are automatically redirected to their top-level transaction
- The function ensures that no transaction already has a base snapshot set (asserts txn->base_snapshot == NULL)
- The transaction is added to rb->txns_by_base_snapshot_lsn list to maintain LSN ordering
- [AssertTXNLsnOrder](../A/AssertTXNLsnOrder.md) is called to verify the ordering invariants are maintained
- The base snapshot LSN is stored alongside the snapshot for future reference

## Simplified Source

```c
void ReorderBufferSetBaseSnapshot(ReorderBuffer *rb, TransactionId xid,
                                 XLogRecPtr lsn, Snapshot snap)
{
    ReorderBufferTXN *txn;
    bool is_new;

    // Get the transaction, creating if necessary
    txn = ReorderBufferTXNByXid(rb, xid, true, &is_new, lsn, true);

    // If this is a subtransaction, delegate to the top-level transaction
    if (rbtxn_is_known_subxact(txn))
        txn = ReorderBufferTXNByXid(rb, txn->toplevel_xid, false,
                                   NULL, InvalidXLogRecPtr, false);

    // Set the base snapshot and LSN
    txn->base_snapshot = snap;
    txn->base_snapshot_lsn = lsn;

    // Add to the ordered list by base snapshot LSN
    dlist_push_tail(&rb->txns_by_base_snapshot_lsn, &txn->base_snapshot_node);

    // Verify LSN ordering is maintained
    AssertTXNLsnOrder(rb);
}
```