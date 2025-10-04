# ReorderBufferFinishPrepared

## Location
[src/backend/replication/logical/reorderbuffer.c:2883-2967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L2883-L2967)

## Overview
Handles COMMIT PREPARED and ROLLBACK PREPARED operations for two-phase transactions in logical replication.

## Definition
```c
void ReorderBufferFinishPrepared(ReorderBuffer *rb, TransactionId xid,
                                XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
                                XLogRecPtr two_phase_at,
                                TimestampTz commit_time, RepOriginId origin_id,
                                XLogRecPtr origin_lsn, char *gid, bool is_commit)
```

## Detailed Description
ReorderBufferFinishPrepared processes the final phase of two-phase commit transactions, handling both COMMIT PREPARED and ROLLBACK PREPARED operations. The function first preserves prepare-time information, then determines if the transaction needs to be decoded (if it wasnt decoded at prepare time due to missing snapshots or disabled two-phase support). For commits, it may replay the transaction before calling the appropriate callback. Finally, it updates transaction metadata and invokes either commit_prepared or rollback_prepared callbacks, followed by cleanup operations including invalidation execution and transaction cleanup.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing transactions  
- `xid`: Transaction ID of the prepared transaction
- `commit_lsn`: LSN where the commit/rollback record starts
- `end_lsn`: LSN where the commit/rollback record ends
- `two_phase_at`: LSN position where two-phase mode was enabled
- `commit_time`: Timestamp of the commit/rollback operation
- `origin_id`: Replication origin identifier
- `origin_lsn`: LSN at the replication origin
- `gid`: Global transaction identifier
- `is_commit`: Boolean flag indicating commit (true) vs rollback (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - RBTXN_PREPARE (flag constant)
  - [ReorderBufferReplay](ReorderBufferReplay.md)
  - [ReorderBufferExecuteInvalidations](ReorderBufferExecuteInvalidations.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
- Called from (representative examples):
  - [DecodeCommit](../D/DecodeCommit.md)
  - [DecodeAbort](../D/DecodeAbort.md)

## Notes and Other Information
This function implements sophisticated logic to handle transactions that may not have been decoded at prepare time, comparing the transactions final_lsn with two_phase_at to determine if replay is needed. The preservation of prepare-time information (prepare_end_lsn and prepare_time) ensures accurate rollback processing. The function serves as the central dispatch point for finalizing two-phase transactions in PostgreSQL logical replication system.

## Simplified Source

```c
void ReorderBufferFinishPrepared(ReorderBuffer *rb, TransactionId xid,
                                XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
                                XLogRecPtr two_phase_at,
                                TimestampTz commit_time, RepOriginId origin_id,
                                XLogRecPtr origin_lsn, char *gid, bool is_commit)
{
    ReorderBufferTXN *txn;
    XLogRecPtr prepare_end_lsn;
    TimestampTz prepare_time;

    // Look up transaction by XID
    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, commit_lsn, false);

    // Skip unknown transactions
    if (txn == NULL)
        return;

    // Preserve prepare-time information for rollback processing
    prepare_end_lsn = txn->end_lsn;
    prepare_time = txn->xact_time.prepare_time;

    // Store global transaction ID
    txn->gid = pstrdup(gid);

    // Replay transaction if not decoded at prepare time and is a commit
    if ((txn->final_lsn < two_phase_at) && is_commit) {
        txn->txn_flags |= RBTXN_PREPARE;
        Assert(txn->final_lsn != InvalidXLogRecPtr);

        // Use prepare-time info for accurate downstream processing
        ReorderBufferReplay(txn, rb, xid, txn->final_lsn, txn->end_lsn,
                           txn->xact_time.prepare_time, txn->origin_id, txn->origin_lsn);
    }

    // Update transaction with commit/rollback information
    txn->final_lsn = commit_lsn;
    txn->end_lsn = end_lsn;
    txn->xact_time.commit_time = commit_time;
    txn->origin_id = origin_id;
    txn->origin_lsn = origin_lsn;

    // Execute appropriate callback based on operation type
    if (is_commit)
        rb->commit_prepared(rb, txn, commit_lsn);
    else
        rb->rollback_prepared(rb, txn, prepare_end_lsn, prepare_time);

    // Cleanup: execute invalidations and clean up transaction
    ReorderBufferExecuteInvalidations(txn->ninvalidations, txn->invalidations);
    ReorderBufferCleanupTXN(rb, txn);
}
```