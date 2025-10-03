# replorigin_get_progress

## Location
[src/backend/replication/logical/origin.c:1014-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1014-L1054)

## Overview
Retrieves the current replication progress (remote LSN) for a specified replication origin, with optional WAL flush functionality.

## Definition

```c
XLogRecPtr
replorigin_get_progress(RepOriginId node, bool flush)
```
## Detailed Description
replorigin_get_progress queries the current replication progress for a given replication origin by searching through the replication_states array. It returns the remote LSN that represents how far replication has progressed from the remote node. The function uses shared locks to safely read the state without blocking concurrent operations. If the flush parameter is true and a valid local LSN exists, it ensures that the local WAL is flushed up to that point, providing durability guarantees for the progress information.

## Parameters / Member Variables
- `node`: RepOriginId identifying the replication origin to query
- `flush`: boolean indicating whether to flush WAL up to the local LSN
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [XLogFlush](../X/XLogFlush.md)
  - RepOriginId
  - [ReplicationState](../R/ReplicationState.md) (struct)
  - LW_SHARED
  - InvalidXLogRecPtr
- Called from (representative examples):
  - [AlterSubscription](../A/AlterSubscription.md) (src/backend/commands/subscriptioncmds.c:1467)
  - [pg_replication_origin_progress](../p/pg_replication_origin_progress.md) (src/backend/replication/logical/origin.c:1506)

## Notes and Other Information
- Returns InvalidXLogRecPtr if the specified replication origin is not found
- Uses shared locks for concurrent read access, allowing multiple readers
- The flush parameter provides a way to ensure durability of the progress information
- Essential for monitoring replication lag and ensuring consistency in logical replication
- The function is safe to call concurrently with replorigin_advance operations
- Commonly used by subscription management and monitoring tools to track replication progress

## Simplified Source

```c
XLogRecPtr
replorigin_get_progress(RepOriginId node, bool flush)
{
    int i;
    XLogRecPtr local_lsn = InvalidXLogRecPtr;
    XLogRecPtr remote_lsn = InvalidXLogRecPtr;

    // Prevent concurrent slot operations
    LWLockAcquire(ReplicationOriginLock, LW_SHARED);

    // Search for the replication origin in the states array
    for (i = 0; i < max_replication_slots; i++)
    {
        ReplicationState *state = &replication_states[i];

        if (state->roident == node)
        {
            // Found the origin, read its progress with state lock
            LWLockAcquire(&state->lock, LW_SHARED);
            remote_lsn = state->remote_lsn;
            local_lsn = state->local_lsn;
            LWLockRelease(&state->lock);
            break;
        }
    }

    LWLockRelease(ReplicationOriginLock);

    // If requested, flush WAL to ensure durability
    if (flush && local_lsn != InvalidXLogRecPtr)
        XLogFlush(local_lsn);

    return remote_lsn;
}
```