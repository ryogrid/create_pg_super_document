# ReorderBufferAddDistributedInvalidations

## Location
[src/backend/replication/logical/reorderbuffer.c:3460-3517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3460-L3517)

## Overview
Accumulates invalidation messages distributed by committed transactions to in-progress transactions, with overflow protection and memory management.

## Definition
void ReorderBufferAddDistributedInvalidations(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, Size nmsgs, SharedInvalidationMessage *msgs)

## Detailed Description
This function handles the distribution of invalidation messages from committed transactions to in-progress transactions. It operates similarly to ReorderBufferAddInvalidations but specifically manages distributed invalidations with overflow protection. The function accumulates messages in the transaction's invalidations_distributed field until reaching MAX_DISTR_INVAL_MSG_PER_TXN, at which point it marks the transaction as overflowed and frees accumulated messages to prevent excessive memory usage. The function also queues the invalidations as reorder buffer changes for proper replay ordering.

## Parameters / Member Variables
- `rb`: The reorder buffer instance to add distributed invalidations to
- `xid`: Transaction ID that should receive these distributed invalidation messages
- `lsn`: Log Sequence Number where the invalidations were recorded
- `nmsgs`: Number of invalidation messages in the msgs array
- `msgs`: Array of SharedInvalidationMessage structures to be distributed

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - rbtxn_get_toptxn
  - rbtxn_distr_inval_overflowed
  - [ReorderBufferAccumulateInvalidations](ReorderBufferAccumulateInvalidations.md)
  - [ReorderBufferQueueInvalidations](ReorderBufferQueueInvalidations.md)
  - [pfree](../p/pfree.md)
  - MAX_DISTR_INVAL_MSG_PER_TXN
  - RBTXN_DISTR_INVAL_OVERFLOWED
- Called from (representative examples):
  - [SnapBuildDistributeSnapshotAndInval](../S/SnapBuildDistributeSnapshotAndInval.md)

## Notes and Other Information
- Implements overflow protection to prevent excessive memory usage from distributed invalidations
- Uses MAX_DISTR_INVAL_MSG_PER_TXN as the threshold for overflow detection
- When overflow occurs, the RBTXN_DISTR_INVAL_OVERFLOWED flag is set and accumulated messages are freed
- Operates under the top-level transaction context for proper invalidation grouping
- Essential for maintaining cache consistency across concurrent transactions in logical replication
- Memory context switching ensures proper memory management within the reorder buffer context

## Simplified Source

```c
void
ReorderBufferAddDistributedInvalidations(ReorderBuffer *rb, TransactionId xid,
                                        XLogRecPtr lsn, Size nmsgs,
                                        SharedInvalidationMessage *msgs)
{
    ReorderBufferTXN *txn;
    MemoryContext oldcontext;

    // Get transaction and switch to reorder buffer memory context
    txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);
    oldcontext = MemoryContextSwitchTo(rb->context);

    // Work with top-level transaction for proper invalidation grouping
    txn = rbtxn_get_toptxn(txn);

    if (!rbtxn_distr_inval_overflowed(txn)) {
        // Check if adding messages would exceed limit
        if (txn->ninvalidations_distributed + nmsgs >= MAX_DISTR_INVAL_MSG_PER_TXN) {
            // Mark as overflowed and free accumulated messages
            txn->txn_flags |= RBTXN_DISTR_INVAL_OVERFLOWED;
            if (txn->invalidations_distributed) {
                pfree(txn->invalidations_distributed);
                txn->invalidations_distributed = NULL;
                txn->ninvalidations_distributed = 0;
            }
        }
        else {
            // Accumulate distributed invalidations normally
            ReorderBufferAccumulateInvalidations(&txn->invalidations_distributed,
                                                &txn->ninvalidations_distributed,
                                                msgs, nmsgs);
        }
    }

    // Queue invalidations as individual changes for replay
    ReorderBufferQueueInvalidations(rb, xid, lsn, nmsgs, msgs);

    MemoryContextSwitchTo(oldcontext);
}
```