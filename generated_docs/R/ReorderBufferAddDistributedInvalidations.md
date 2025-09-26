# ReorderBufferAddDistributedInvalidations

## Location
src/backend/replication/logical/reorderbuffer.c: 3460 - 3517

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
  - ReorderBufferTXNByXid
  - MemoryContextSwitchTo
  - rbtxn_get_toptxn
  - rbtxn_distr_inval_overflowed
  - ReorderBufferAccumulateInvalidations
  - ReorderBufferQueueInvalidations
  - pfree
  - MAX_DISTR_INVAL_MSG_PER_TXN
  - RBTXN_DISTR_INVAL_OVERFLOWED
- Called from (representative examples):
  - SnapBuildDistributeSnapshotAndInval

## Notes and Other Information
- Implements overflow protection to prevent excessive memory usage from distributed invalidations
- Uses MAX_DISTR_INVAL_MSG_PER_TXN as the threshold for overflow detection
- When overflow occurs, the RBTXN_DISTR_INVAL_OVERFLOWED flag is set and accumulated messages are freed
- Operates under the top-level transaction context for proper invalidation grouping
- Essential for maintaining cache consistency across concurrent transactions in logical replication
- Memory context switching ensures proper memory management within the reorder buffer context