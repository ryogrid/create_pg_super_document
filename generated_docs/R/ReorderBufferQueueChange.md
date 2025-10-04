# ReorderBufferQueueChange

## Location
[src/backend/replication/logical/reorderbuffer.c:806-868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L806-L868)

## Overview
Queues a change into a transaction for later replay upon commit or streaming when memory thresholds are reached during logical replication decoding.

## Definition

```c
void
ReorderBufferQueueChange(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn,
						 ReorderBufferChange *change, bool toast_insert)
```
## Detailed Description
This function is the primary interface for adding changes to transactions during logical replication decoding. It creates or retrieves the transaction by XID, validates that the transaction hasn't been concurrently aborted, and adds the change to the transaction's change list. The function tracks streamable changes, updates memory accounting, processes partial changes for streaming considerations, and enforces memory limits. It serves as the central point where decoded WAL records are converted into transaction changes that can be replayed or streamed to subscribers.

## Parameters / Member Variables
- `*rb`: The ReorderBuffer managing transaction state and memory limits
- `xid`: The transaction ID that this change belongs to
- `lsn`: The LSN (Log Sequence Number) where this change was found in the WAL
- `*change`: The actual change to be queued (insert, update, delete, etc.)
- `toast_insert`: Boolean indicating if this is a TOAST table insert operation
## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md) (gets or creates transaction)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md) (returns change if transaction aborted)
  - rbtxn_get_toptxn (gets top-level transaction)
  - [dlist_push_tail](../d/dlist_push_tail.md) (adds change to transaction's change list)
  - [ReorderBufferChangeMemoryUpdate](ReorderBufferChangeMemoryUpdate.md) (updates memory accounting)
  - [ReorderBufferChangeSize](ReorderBufferChangeSize.md) (calculates change memory size)
  - [ReorderBufferProcessPartialChange](ReorderBufferProcessPartialChange.md) (handles partial change processing)
  - [ReorderBufferCheckMemoryLimit](ReorderBufferCheckMemoryLimit.md) (enforces memory limits)
- Called from (representative examples):
  - [DecodeInsert](../D/DecodeInsert.md) (for INSERT operations)
  - [DecodeUpdate](../D/DecodeUpdate.md) (for UPDATE operations)
  - [DecodeDelete](../D/DecodeDelete.md) (for DELETE operations)
  - [DecodeTruncate](../D/DecodeTruncate.md) (for TRUNCATE operations)
  - [ReorderBufferQueueMessage](ReorderBufferQueueMessage.md) (for logical messages)

## Notes and Other Information
- Automatically creates transactions if they don't exist, marking them as top-level transactions
- Skips processing if the transaction has been concurrently aborted during streaming
- Marks transactions with streamable changes using RBTXN_HAS_STREAMABLE_CHANGE flag
- Streamable change types include: INSERT, UPDATE, DELETE, INTERNAL_SPEC_INSERT, TRUNCATE, and MESSAGE
- Updates both nentries (total changes) and nentries_mem (in-memory changes) counters
- Integrates with the streaming system by processing partial changes and checking memory limits
- Memory accounting tracks the size of each change to enforce logical_decoding_work_mem limits
- Changes are stored in LSN order within each transaction for proper replay sequencing

## Simplified Source

```c
void ReorderBufferQueueChange(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn,
                             ReorderBufferChange *change, bool toast_insert) {
    // Get or create transaction for this XID
    ReorderBufferTXN *txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);

    // Skip if transaction was aborted during streaming
    if (txn->concurrent_abort) {
        ReorderBufferReturnChange(rb, change, false);
        return;
    }

    // Mark transaction as having streamable changes for certain change types
    if (change->action == REORDER_BUFFER_CHANGE_INSERT ||
        change->action == REORDER_BUFFER_CHANGE_UPDATE ||
        change->action == REORDER_BUFFER_CHANGE_DELETE ||
        change->action == REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT ||
        change->action == REORDER_BUFFER_CHANGE_TRUNCATE ||
        change->action == REORDER_BUFFER_CHANGE_MESSAGE) {
        ReorderBufferTXN *toptxn = rbtxn_get_toptxn(txn);
        toptxn->txn_flags |= RBTXN_HAS_STREAMABLE_CHANGE;
    }

    // Set change metadata and add to transaction
    change->lsn = lsn;
    change->txn = txn;
    dlist_push_tail(&txn->changes, &change->node);
    txn->nentries++;
    txn->nentries_mem++;

    // Update memory accounting
    ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, ReorderBufferChangeSize(change));

    // Process partial change for streaming
    ReorderBufferProcessPartialChange(rb, txn, change, toast_insert);

    // Check if memory limits need enforcement
    ReorderBufferCheckMemoryLimit(rb);
}
```