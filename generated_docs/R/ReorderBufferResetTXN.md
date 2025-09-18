# ReorderBufferResetTXN

## Location
src/backend/replication/logical/reorderbuffer.c: 2081 - 2126

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
  - ReorderBuffer (struct type)
  - ReorderBufferTXN (struct type)
  - CommandId (type)
  - ReorderBufferChange (struct type)
  - ReorderBufferTruncateTXN (truncate transaction changes)
  - rbtxn_prepared (check if transaction is prepared)
  - ReorderBufferToastReset (reset toast reconstruction)
  - ReorderBufferReturnChange (return change to pool)
  - rbtxn_is_streamed (check if transaction is streamed)
  - ReorderBufferSaveTXNSnapshot (save snapshot state)
- Called from (representative examples):
  - CHANGES_THRESHOLD (streaming threshold handling)

## Notes and Other Information
- Essential for handling concurrent abort scenarios during streaming logical replication
- Ensures proper cleanup of resources while preserving necessary state for continuation
- The function maintains transaction consistency by properly stopping streams and saving state
- Includes assertion to verify all changes are properly deallocated after reset
- Critical for robust handling of subtransaction aborts in complex streaming scenarios
- Part of PostgreSQL's advanced streaming logical replication error recovery mechanisms