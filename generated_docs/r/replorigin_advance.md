# replorigin_advance

## Location
[src/backend/replication/logical/origin.c:888-1013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L888-L1013)

## Overview
Advances the replication progress for a specific replication origin, recording that a commit from a remote node has been successfully replayed locally.

## Definition

```c
void
replorigin_advance(RepOriginId node,
				   XLogRecPtr remote_commit, XLogRecPtr local_commit,
				   bool go_backward, bool wal_log)
```
## Detailed Description
replorigin_advance is a core function that updates the replication progress tracking for a specific replication origin. It searches for or creates a replication state slot for the given origin, then updates the remote and local LSN positions to reflect successful replay of a transaction. The function handles concurrent access through lwlocks, supports both forward and backward LSN movement, and optionally logs the change to WAL for durability. It's essential for crash recovery and ensuring that replicated transactions aren't replayed multiple times.

## Parameters / Member Variables
- : RepOriginId identifying the replication origin
- : XLogRecPtr of the commit LSN on the remote node
- : XLogRecPtr of the local commit LSN
- : boolean allowing LSN to move backward (for special cases)
- : boolean indicating whether to write a WAL record for this change

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - ereport (ERROR level)
  - RepOriginId
  - [ReplicationState](../R/ReplicationState.md) (struct)
  - [xl_replorigin_set](../x/xl_replorigin_set.md) (struct)
  - InvalidRepOriginId
  - DoNotReplicateId
  - XLOG_REPLORIGIN_SET
  - RM_REPLORIGIN_ID
- Called from (representative examples):
  - [xact_redo_commit](../x/xact_redo_commit.md) (src/backend/access/transam/xact.c:6152)
  - [replorigin_redo](replorigin_redo.md) (src/backend/replication/logical/origin.c:838)
  - [pg_replication_origin_advance](../p/pg_replication_origin_advance.md) (src/backend/replication/logical/origin.c:1474)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (src/backend/replication/logical/tablesync.c:1487)

## Notes and Other Information
- Requires RowExclusiveLock on pg_replication_origin unless running in recovery
- Skips processing for DoNotReplicateId to avoid unnecessary tracking
- Uses ReplicationOriginLock for concurrent access protection
- Handles race conditions during checkpoints by allowing older values in certain scenarios
- Creates new replication state slots when needed, respecting max_replication_slots limit
- WAL logging is conditional - typically disabled during recovery replay but enabled for direct API calls
- Critical for maintaining replication consistency and preventing duplicate transaction replay

## Simplified Source

```c
void replorigin_advance(RepOriginId node, XLogRecPtr remote_commit,
                       XLogRecPtr local_commit, bool go_backward, bool wal_log)
{
    ReplicationState *replication_state = NULL;
    ReplicationState *free_state = NULL;

    // Skip processing for invalid or special origin IDs
    if (node == InvalidRepOriginId || node == DoNotReplicateId)
        return;

    // Acquire exclusive lock for shared memory access
    LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);

    // Find existing slot for this origin or a free slot
    for (int i = 0; i < max_replication_slots; i++) {
        ReplicationState *curstate = &replication_states[i];

        if (curstate->roident == InvalidRepOriginId && free_state == NULL) {
            free_state = curstate;  // Remember free slot
            continue;
        }

        if (curstate->roident == node) {
            replication_state = curstate;  // Found our slot
            LWLockAcquire(&replication_state->lock, LW_EXCLUSIVE);

            // Ensure slot is not in use by another process
            if (replication_state->acquired_by != 0)
                ereport(ERROR, "origin already active");
            break;
        }
    }

    // Create new slot if needed
    if (replication_state == NULL) {
        if (free_state == NULL)
            ereport(ERROR, "no free replication slots");

        LWLockAcquire(&free_state->lock, LW_EXCLUSIVE);
        replication_state = free_state;
        replication_state->roident = node;
    }

    // Optionally write WAL record for durability
    if (wal_log) {
        xl_replorigin_set xlrec = {
            .remote_lsn = remote_commit,
            .node_id = node,
            .force = go_backward
        };

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, sizeof(xlrec));
        XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_SET);
    }

    // Update LSN positions (avoid going backward unless explicitly allowed)
    if (go_backward || replication_state->remote_lsn < remote_commit)
        replication_state->remote_lsn = remote_commit;

    if (local_commit != InvalidXLogRecPtr &&
        (go_backward || replication_state->local_lsn < local_commit))
        replication_state->local_lsn = local_commit;

    // Release locks
    LWLockRelease(&replication_state->lock);
    LWLockRelease(ReplicationOriginLock);
}
```