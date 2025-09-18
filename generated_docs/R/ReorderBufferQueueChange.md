# ReorderBufferQueueChange

## Location
src/backend/replication/logical/reorderbuffer.c: 806 - 868

## Overview
Queues a change into a transaction for later replay upon commit or streaming when memory thresholds are reached during logical replication decoding.

## Definition


## Detailed Description
This function is the primary interface for adding changes to transactions during logical replication decoding. It creates or retrieves the transaction by XID, validates that the transaction hasn't been concurrently aborted, and adds the change to the transaction's change list. The function tracks streamable changes, updates memory accounting, processes partial changes for streaming considerations, and enforces memory limits. It serves as the central point where decoded WAL records are converted into transaction changes that can be replayed or streamed to subscribers.

## Parameters / Member Variables
- : The ReorderBuffer managing transaction state and memory limits
- : The transaction ID that this change belongs to
- : The LSN (Log Sequence Number) where this change was found in the WAL
- : The actual change to be queued (insert, update, delete, etc.)
- : Boolean indicating if this is a TOAST table insert operation

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid (gets or creates transaction)
  - ReorderBufferReturnChange (returns change if transaction aborted)
  - rbtxn_get_toptxn (gets top-level transaction)
  - dlist_push_tail (adds change to transaction's change list)
  - ReorderBufferChangeMemoryUpdate (updates memory accounting)
  - ReorderBufferChangeSize (calculates change memory size)
  - ReorderBufferProcessPartialChange (handles partial change processing)
  - ReorderBufferCheckMemoryLimit (enforces memory limits)
- Called from (representative examples):
  - DecodeInsert (for INSERT operations)
  - DecodeUpdate (for UPDATE operations)
  - DecodeDelete (for DELETE operations)
  - DecodeTruncate (for TRUNCATE operations)
  - ReorderBufferQueueMessage (for logical messages)

## Notes and Other Information
- Automatically creates transactions if they don't exist, marking them as top-level transactions
- Skips processing if the transaction has been concurrently aborted during streaming
- Marks transactions with streamable changes using RBTXN_HAS_STREAMABLE_CHANGE flag
- Streamable change types include: INSERT, UPDATE, DELETE, INTERNAL_SPEC_INSERT, TRUNCATE, and MESSAGE
- Updates both nentries (total changes) and nentries_mem (in-memory changes) counters
- Integrates with the streaming system by processing partial changes and checking memory limits
- Memory accounting tracks the size of each change to enforce logical_decoding_work_mem limits
- Changes are stored in LSN order within each transaction for proper replay sequencing