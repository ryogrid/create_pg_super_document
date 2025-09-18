# SyncRepWakeQueue

## Location
src/backend/replication/syncrep.c: 907 - 963

## Overview
Wakes up backend processes waiting for synchronous replication by walking through the synchronous replication wait queue and notifying processes whose wait conditions have been satisfied.

## Definition
```c
static int SyncRepWakeQueue(bool all, int mode)
```

## Detailed Description
This function processes the synchronous replication wait queue for a specific mode (commit, write, flush, or apply). It walks through the queue from the head and wakes up backend processes that are waiting for synchronous replication confirmation. The function can operate in two modes:

1. Wake all waiting processes (when all = true)
2. Wake only processes whose waitLSN has been satisfied by the current walsender LSN

For each process that needs to be awakened:
- Removes the process from the wait queue
- Uses a memory barrier to ensure proper ordering of queue removal and state change
- Sets the process state to SYNC_REP_WAIT_COMPLETE
- Wakes the process by setting its latch

The function assumes the queue is ordered by LSN and leverages this for efficient processing.

## Parameters / Member Variables
- `all`: If true, wake all processes in the queue; if false, wake only those satisfied by current LSN
- `mode`: The synchronous replication wait mode (commit, write, flush, or apply)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify (doubly-linked list iteration)
  - dlist_container (container access macro)
  - [dlist_delete_thoroughly](../d/dlist_delete_thoroughly.md) (list element removal)
  - pg_write_barrier (memory barrier for ordering)
  - [SetLatch](SetLatch.md) (process awakening)
  - LWLockHeldByMeInMode (lock assertion)
  - [SyncRepQueueIsOrderedByLSN](SyncRepQueueIsOrderedByLSN.md) (queue validation)
- Called from (representative examples):
  - SyncStandbysDefined (src/backend/replication/syncrep.c:102)
  - [SyncRepReleaseWaiters](SyncRepReleaseWaiters.md) (src/backend/replication/syncrep.c:554, 559, 564)
  - [SyncRepUpdateSyncStandbysDefined](SyncRepUpdateSyncStandbysDefined.md) (src/backend/replication/syncrep.c:983)

## Notes and Other Information
- Requires SyncRepLock to be held in exclusive mode by the caller
- Returns the number of processes that were awakened
- Uses memory barriers to ensure proper ordering of state changes
- Static function scope limits visibility to the syncrep.c compilation unit
- Critical for the performance of synchronous replication as it minimizes wait times