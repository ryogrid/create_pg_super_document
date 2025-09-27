# SyncRepWakeQueue

## Location
[src/backend/replication/syncrep.c:907-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L907-L963)

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
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md) (lock assertion)
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

## Simplified Source

```c
// Simplified version of SyncRepWakeQueue
static int SyncRepWakeQueue(bool all, int mode) {
    volatile WalSndCtlData *walsndctl = WalSndCtl;
    int numprocs = 0;
    dlist_mutable_iter iter;

    // Walk through the wait queue for this mode
    dlist_foreach_modify(iter, &WalSndCtl->SyncRepQueue[mode]) {
        PGPROC *proc = dlist_container(PGPROC, syncRepLinks, iter.cur);

        // Check if we should wake this process
        // Queue is ordered by LSN, so we can stop early if not waking all
        if (!all && walsndctl->lsn[mode] < proc->waitLSN)
            return numprocs;

        // Remove process from the wait queue
        dlist_delete_thoroughly(&proc->syncRepLinks);

        // Ensure queue removal is visible before state change
        pg_write_barrier();

        // Mark the wait as complete
        proc->syncRepState = SYNC_REP_WAIT_COMPLETE;

        // Wake up the waiting process
        SetLatch(&(proc->procLatch));

        numprocs++;
    }

    return numprocs;
}
```

Key simplifications made:
- Removed detailed assertions and validation checks for clarity
- Consolidated comments to focus on the main algorithm
- Emphasized the core flow: check condition → remove from queue → update state → wake process
- Abstracted the memory barrier details while preserving the critical ordering requirement