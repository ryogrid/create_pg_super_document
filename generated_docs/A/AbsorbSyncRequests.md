# AbsorbSyncRequests

## Location
[src/backend/postmaster/checkpointer.c:1270-1316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L1270-L1316)

## Overview
AbsorbSyncRequests retrieves queued synchronization requests from shared memory and passes them to the sync mechanism for processing during checkpoint operations.

## Definition
void AbsorbSyncRequests(void)

## Detailed Description
This function serves as a critical component in PostgreSQL's checkpoint process by transferring pending sync requests from the shared memory queue to the local sync mechanism. It is specifically designed to be called during CreateCheckPoint operations to ensure all pending fsync requests are processed before the checkpoint begins its fsync phase.

The function implements a copy-and-process strategy to minimize lock contention: it first copies all pending requests from shared memory while holding the CheckpointerCommLock, then processes them after releasing the lock. This approach reduces the time other processes must wait to add new requests to the queue.

A critical design aspect is the use of PANIC semantics once the requests are cleared from shared memory. If the function fails to absorb the requests after clearing them (such as due to hashtable memory exhaustion), the system will PANIC since PostgreSQL cannot safely continue if it cannot fulfill required fsync operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - AmCheckpointerProcess
  - [palloc](../p/palloc.md)
  - memcpy
  - START_CRIT_SECTION
  - END_CRIT_SECTION
  - [RememberSyncRequest](../R/RememberSyncRequest.md)
  - [pfree](../p/pfree.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (CheckpointerCommLock)
- Types used:
  - [CheckpointerRequest](../C/CheckpointerRequest.md)
- Called from:
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [CheckpointWriteDelay](../C/CheckpointWriteDelay.md)
  - [SyncPreCheckpoint](../S/SyncPreCheckpoint.md)
  - [SyncPostCheckpoint](../S/SyncPostCheckpoint.md)
  - [ProcessSyncRequests](../P/ProcessSyncRequests.md)

## Notes and Other Information
- Only executes when called from within the checkpointer process; returns immediately if called from other processes
- Uses critical section semantics to ensure atomicity when clearing the shared memory request queue
- The function must handle all requests it retrieves to maintain system integrity
- Memory allocation for the request copy uses palloc, which could theoretically fail and cause a PANIC
- The shared memory queue is completely cleared in one atomic operation to prevent partial processing
- Export function designed to be callable from CreateCheckPoint in various process contexts