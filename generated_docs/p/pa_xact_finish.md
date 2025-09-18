# pa_xact_finish

## Location
[src/backend/replication/logical/applyparallelworker.c:1618-1639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1618-L1639)

## Overview
Completes the processing of a streaming transaction in the leader apply worker, including cleanup, synchronization, and resource management.

## Definition
```c
void pa_xact_finish(ParallelApplyWorkerInfo *winfo, XLogRecPtr remote_lsn)
```

## Detailed Description
This function is responsible for the final phase of transaction processing in PostgreSQL's logical replication parallel apply system. It is called by the leader apply worker to properly complete a streaming transaction. The function performs several critical operations: it unlocks the stream lock to allow the parallel worker to continue processing, waits for the worker to finish (maintaining commit ordering), stores the flush position for replication progress tracking, and finally frees the worker resources. This function is essential for maintaining transaction dependencies and avoiding deadlocks in the parallel apply system.

## Parameters / Member Variables
- `winfo`: Pointer to ParallelApplyWorkerInfo structure containing information about the parallel worker that processed the transaction
- `remote_lsn`: The remote Log Sequence Number (XLogRecPtr) representing the transaction's position, used for flush position tracking

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md)
  - [am_leader_apply_worker](../a/am_leader_apply_worker.md)
  - [pa_unlock_stream](pa_unlock_stream.md)
  - AccessExclusiveLock
  - [pa_wait_for_xact_finish](pa_wait_for_xact_finish.md)
  - XLogRecPtrIsInvalid
  - [store_flush_position](../s/store_flush_position.md)
  - [pa_free_worker](pa_free_worker.md)
- Called from (representative examples):
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)

## Notes and Other Information
- Only valid to call from the leader apply worker context (enforced by Assert)
- Uses AccessExclusiveLock mode when unlocking the stream, indicating exclusive access was required during processing
- The waiting mechanism (pa_wait_for_xact_finish) is crucial for maintaining commit order and preventing transaction dependency issues
- Conditionally stores flush position only when remote_lsn is valid (not XLogRecPtrIsInvalid)
- The flush position storage uses both remote_lsn and winfo->shared->last_commit_end for tracking replication progress
- Resource cleanup via pa_free_worker ensures proper memory management
- Called in various transaction completion scenarios including prepare, commit, and abort operations
- Part of the critical path for maintaining consistency in parallel logical replication streams