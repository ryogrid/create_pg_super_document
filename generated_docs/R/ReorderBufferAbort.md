# ReorderBufferAbort

## Location
[src/backend/replication/logical/reorderbuffer.c:2968-3013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L2968-L3013)

## Overview
Aborts a transaction that possibly has previous changes, purging the transaction and its contents from memory and disk.

## Definition

```c
void
ReorderBufferAbort(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn,
				   TimestampTz abort_time)
```
## Detailed Description
ReorderBufferAbort handles the cleanup of transactions that have actively aborted (i.e., have produced an abort record). This function is designed to be called first for subtransactions and then for the toplevel transaction ID. 

The function performs several key operations:
1. Looks up the transaction by XID in the reorder buffer
2. Records the abort time in the transaction structure
3. For streamed transactions, notifies the remote node about the abort via the stream_abort callback
4. Handles cache invalidation for streamed transactions that may have loaded cache entries during decoding
5. Sets the final LSN and cleans up all transaction data

This is distinct from ReorderBufferAbortOld() which handles implicitly aborted transactions, and ReorderBufferForget() which handles committed transactions that are no longer of interest.

## Parameters / Member Variables
- `*rb`: The ReorderBuffer instance managing the transaction
- `xid`: Transaction ID of the transaction to abort
- `lsn`: Log Sequence Number where the abort occurred
- `abort_time`: Timestamp when the transaction was aborted
## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - rbtxn_is_streamed
  - [ReorderBufferImmediateInvalidation](ReorderBufferImmediateInvalidation.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
- Called from (representative examples):
  - [DecodeAbort](../D/DecodeAbort.md) (in decode.c)

## Notes and Other Information
- Only handles transactions that have actively aborted with an abort record
- For streamed transactions, performs additional cleanup including remote notification and cache invalidation
- Must be called for subtransactions before the toplevel transaction
- Unknown transactions (NULL lookup result) are safely ignored
- The function ensures proper cleanup of both memory and disk-based transaction data

## Simplified Source

```c
void
ReorderBufferAbort(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn,
                   TimestampTz abort_time)
{
    ReorderBufferTXN *txn;

    // Look up the transaction by XID
    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr, false);

    // If transaction not found, nothing to do
    if (txn == NULL)
        return;

    // Record the abort time
    txn->xact_time.abort_time = abort_time;

    // Handle streamed transactions
    if (rbtxn_is_streamed(txn))
    {
        // Notify remote node about the abort
        rb->stream_abort(rb, txn, lsn);

        // Execute invalidation messages to clear cache entries
        // that may have been loaded during this transaction
        if (txn->ninvalidations > 0)
            ReorderBufferImmediateInvalidation(rb, txn->ninvalidations,
                                               txn->invalidations);
    }

    // Set final LSN
    txn->final_lsn = lsn;

    // Clean up transaction data from memory and disk
    ReorderBufferCleanupTXN(rb, txn);
}
```