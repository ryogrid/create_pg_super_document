# replorigin_session_advance

## Location
[src/backend/replication/logical/origin.c:1219-1236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1219-L1236)

## Overview
Advances the replication progress tracking for the current session's configured replication origin by updating both local and remote LSN positions in a more efficient manner than the general replorigin_advance() function.

## Definition

```c
void
replorigin_session_advance(XLogRecPtr remote_commit, XLogRecPtr local_commit)
```
## Detailed Description
This function provides an optimized way to update replication progress for the currently active replication origin session. It updates the session's local and remote LSN (Log Sequence Number) positions to track replication progress.

The function performs the same core work as replorigin_advance() but operates directly on the cached session state, making it significantly more efficient since it avoids the overhead of searching through the replication state array to find the correct origin.

The function ensures that LSN values only advance forward (never backward) by checking that the new values are greater than the current stored values before updating them.

## Parameters / Member Variables
- : XLogRecPtr representing the remote commit LSN position to advance to
- : XLogRecPtr representing the local commit LSN position to advance to

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (on session state lock)
  - InvalidRepOriginId (for assertions)
- Called from (representative examples):
  - [EndPrepare](../E/EndPrepare.md)
  - [RecordTransactionCommitPrepared](../R/RecordTransactionCommitPrepared.md)
  - [RecordTransactionAbortPrepared](../R/RecordTransactionAbortPrepared.md)
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md)

## Notes and Other Information
- Requires an active replication origin session (established via replorigin_session_setup())
- More efficient than replorigin_advance() due to cached session state access
- Uses exclusive locking on the individual session state rather than global locks
- LSN values are only updated if the new values are greater than current values
- Critical for maintaining replication progress tracking during transaction commits and aborts
- Primarily used in transaction management code to update progress after transaction completion

## Simplified Source

```c
void replorigin_session_advance(XLogRecPtr remote_commit, XLogRecPtr local_commit)
{
    // Ensure we have a valid replication origin session
    Assert(session_replication_state != NULL);
    Assert(session_replication_state->roident != InvalidRepOriginId);

    // Acquire exclusive lock on the session state
    LWLockAcquire(&session_replication_state->lock, LW_EXCLUSIVE);

    // Update local LSN only if advancing forward
    if (session_replication_state->local_lsn < local_commit)
        session_replication_state->local_lsn = local_commit;

    // Update remote LSN only if advancing forward
    if (session_replication_state->remote_lsn < remote_commit)
        session_replication_state->remote_lsn = remote_commit;

    // Release the lock
    LWLockRelease(&session_replication_state->lock);
}
```