# ReorderBufferAddInvalidations

## Location
src/backend/replication/logical/reorderbuffer.c: 3419 - 3459

## Overview
Accumulates invalidation messages from XLOG_XACT_INVALIDATIONS records for later execution, storing them both in the transaction buffer and as reorder buffer changes.

## Definition
void ReorderBufferAddInvalidations(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, Size nmsgs, SharedInvalidationMessage *msgs)

## Detailed Description
This function processes invalidation messages from transaction commit records and handles them in two ways: first, it accumulates all invalidations under the top-level transaction for batch execution, and second, it queues them as individual changes in the reorder buffer. This dual approach allows for both efficient bulk invalidation execution and fine-grained replay during logical replication. The function operates within the reorder buffer's memory context and ensures invalidations are associated with the top-level transaction to enable proper cleanup in cases where transactions are skipped.

## Parameters / Member Variables
- `rb`: The reorder buffer instance to add invalidations to
- `xid`: Transaction ID that generated these invalidation messages
- `lsn`: Log Sequence Number where the invalidations were recorded
- `nmsgs`: Number of invalidation messages in the msgs array
- `msgs`: Array of SharedInvalidationMessage structures to be processed

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid
  - MemoryContextSwitchTo
  - rbtxn_get_toptxn
  - ReorderBufferAccumulateInvalidations
  - ReorderBufferQueueInvalidations
- Called from (representative examples):
  - xact_decode

## Notes and Other Information
- Invalidations are accumulated under the top-level transaction for efficient batch processing
- Memory context is switched to the reorder buffer context for proper memory management
- The function serves dual purposes: accumulation for bulk execution and queuing for replay
- Essential for maintaining cache consistency during logical replication
- Handles both committed transaction invalidations and in-progress transaction requirements