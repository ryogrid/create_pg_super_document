# ReorderBufferResetTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:2081-2126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L2081-L2126)

## Overview
Helper function that resets a transaction's state after streaming abort scenarios, enabling continued processing of remaining transaction data while preserving necessary state for streaming continuation.

## Definition
```c
static void ReorderBufferResetTXN(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                 Snapshot snapshot_now, CommandId command_id,
                                 XLogRecPtr last_lsn, ReorderBufferChange *specinsert)
```

## Detailed Description
ReorderBufferResetTXN handles the complex scenario where a streaming transaction encounters a concurrent abort (typically of a subtransaction) but needs to continue processing remaining data. The function performs comprehensive cleanup including truncating streamed changes, resetting toast reconstruction resources, and returning any special insert changes. For streaming transactions, it properly stops the current stream and saves the snapshot and command ID state for seamless continuation. This enables robust handling of partial transaction aborts in logical replication streaming scenarios.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance managing the replication state
- `txn`: Transaction to reset and prepare for continued processing
- `snapshot_now`: Current snapshot to preserve for streaming continuation
- `command_id`: Current command ID to preserve for streaming continuation
- `last_lsn`: Last Log Sequence Number processed before reset
- `specinsert`: Special insert change to be returned (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](ReorderBuffer.md) (struct type)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (struct type)
  - CommandId (type)
  - [ReorderBufferChange](ReorderBufferChange.md) (struct type)
  - [ReorderBufferTruncateTXN](ReorderBufferTruncateTXN.md) (truncate transaction changes)
  - rbtxn_prepared (check if transaction is prepared)
  - [ReorderBufferToastReset](ReorderBufferToastReset.md) (reset toast reconstruction)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md) (return change to pool)
  - rbtxn_is_streamed (check if transaction is streamed)
  - [ReorderBufferSaveTXNSnapshot](ReorderBufferSaveTXNSnapshot.md) (save snapshot state)
- Called from (representative examples):
  - CHANGES_THRESHOLD (streaming threshold handling)

## Notes and Other Information
- Essential for handling concurrent abort scenarios during streaming logical replication
- Ensures proper cleanup of resources while preserving necessary state for continuation
- The function maintains transaction consistency by properly stopping streams and saving state
- Includes assertion to verify all changes are properly deallocated after reset
- Critical for robust handling of subtransaction aborts in complex streaming scenarios
- Part of PostgreSQL's advanced streaming logical replication error recovery mechanisms

## Simplified Source

```c
static void ReorderBufferResetTXN(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                 Snapshot snapshot_now, CommandId command_id,
                                 XLogRecPtr last_lsn, ReorderBufferChange *specinsert)
{
    // Clean up streamed changes
    ReorderBufferTruncateTXN(rb, txn, rbtxn_prepared(txn));

    // Reset toast reconstruction resources
    ReorderBufferToastReset(rb, txn);

    // Return special insert change if provided
    if (specinsert != NULL) {
        ReorderBufferReturnChange(rb, specinsert, true);
        specinsert = NULL;
    }

    // For streaming transactions: stop stream and save state for continuation
    if (rbtxn_is_streamed(txn)) {
        rb->stream_stop(rb, txn, last_lsn);
        ReorderBufferSaveTXNSnapshot(rb, txn, snapshot_now, command_id);
    }

    // Verify complete cleanup
    Assert(txn->size == 0);
}
```