# store_flush_position

## Location
[src/backend/replication/logical/worker.c:3449-3474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3449-L3474)

## Overview
Stores current remote/local LSN pairs in the tracking list for logical replication flush position management.

## Definition

```c
void
store_flush_position(XLogRecPtr remote_lsn, XLogRecPtr local_lsn)
```
## Detailed Description
This function maintains the critical mapping between remote publisher LSNs and local subscriber LSNs that enables safe flush position reporting in logical replication. It works by:

1. **Parallel Worker Filtering**: Immediately returns for parallel apply workers since LSN mapping is maintained exclusively by the leader apply worker to avoid coordination complexity

2. **Memory Context Management**: Switches to the permanent ApplyContext to ensure the LSN mapping persists across message processing cycles

3. **LSN Pair Storage**: Creates and populates a FlushPosition structure containing:
   - : The local LSN corresponding to where the transaction was written locally
   - : The remote LSN from the publisher that corresponds to this transaction

4. **List Management**: Appends the new mapping to the tail of the lsn_mapping doubly-linked list, maintaining chronological order of commits

5. **Context Restoration**: Switches back to ApplyMessageContext for subsequent message processing

This mapping list is essential for the get_flush_position() function to determine which remote LSNs can be safely reported as flushed based on local flush progress.

## Parameters / Member Variables
- : The LSN from the remote publisher that corresponds to this transaction commit
- : The local LSN where this transaction's changes were written on the subscriber

## Dependencies
- Functions called/Symbols referenced:
  - [am_parallel_apply_worker](../a/am_parallel_apply_worker.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
- Called from (representative examples):
  - [pa_xact_finish](../p/pa_xact_finish.md)
  - [apply_handle_prepare](../a/apply_handle_prepare.md)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_commit_internal](../a/apply_handle_commit_internal.md)

## Notes and Other Information
- Only operates on leader apply workers; parallel workers are excluded to maintain single-threaded control over the LSN mapping
- Uses ApplyContext (permanent context) to ensure LSN mappings survive across message processing cycles
- The LSN mappings are stored in chronological order, which is essential for the sequential processing in get_flush_position()
- Critical component of the feedback mechanism that prevents premature flush acknowledgments to the publisher
- Works in conjunction with get_flush_position() to implement safe replication progress reporting
- Called at transaction commit points to establish the remote-to-local LSN correlation needed for accurate flush reporting