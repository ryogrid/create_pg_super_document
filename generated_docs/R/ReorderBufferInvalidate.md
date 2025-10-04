# ReorderBufferInvalidate

## Location
[src/backend/replication/logical/reorderbuffer.c:3103-3133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3103-L3133)

## Overview
Invalidates cache for transactions that need to be skipped, specifically designed for prepared transactions where catalog manipulations require cache updates.

## Definition
```c
void ReorderBufferInvalidate(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
```

## Detailed Description
ReorderBufferInvalidate is a special-purpose function designed primarily for prepared transactions that need to be skipped but may have performed catalog manipulations. Unlike ReorderBufferForget(), this function does not clean up the transaction structure - it only processes cache invalidation messages.

The function's key purpose is to ensure that catalog caches remain consistent even when we decide to skip a transaction's contents. This is particularly important for prepared transactions where the transaction structure needs to remain intact for potential future processing, but any catalog changes need to be reflected in the cache system immediately.

The function only processes invalidations if the transaction has a base snapshot, indicating that it was involved in catalog operations.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing the transaction
- `xid`: Transaction ID of the transaction to invalidate
- `lsn`: Log Sequence Number (currently not used in the function body)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - [ReorderBufferImmediateInvalidation](ReorderBufferImmediateInvalidation.md)
- Called from (representative examples):
  - [DecodePrepare](../D/DecodePrepare.md) (in decode.c)

## Notes and Other Information
- Special-purpose function designed for prepared transactions
- Does not clean up the transaction structure, unlike ReorderBufferForget()
- Only processes cache invalidations, not transaction data
- Requires a base snapshot to process invalidations (otherwise asserts ninvalidations == 0)
- Part of the prepared transaction handling workflow where transactions may be skipped but catalog effects must be preserved
- The LSN parameter is accepted but not actively used in the current implementation

## Simplified Source

```c
void ReorderBufferInvalidate(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
{
    ReorderBufferTXN *txn;

    // Find the transaction by XID
    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr, false);

    // Nothing to invalidate if transaction doesn't exist
    if (txn == NULL)
        return;

    // Process cache invalidations if transaction has base snapshot and invalidations
    // This ensures catalog consistency even for skipped transactions
    if (txn->base_snapshot != NULL && txn->ninvalidations > 0)
        ReorderBufferImmediateInvalidation(rb, txn->ninvalidations, txn->invalidations);

    // Note: Unlike ReorderBufferForget(), this does NOT clean up the transaction
    // The transaction structure remains intact for potential future processing
}
```