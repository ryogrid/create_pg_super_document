# SyncRepQueueIsOrderedByLSN

## Location
[src/backend/replication/syncrep.c:1024-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L1024-L1057)

## Overview
A debugging/validation function that verifies the synchronous replication wait queue is properly ordered by LSN (Log Sequence Number) in ascending order.

## Definition

```c
static bool
SyncRepQueueIsOrderedByLSN(int mode)
```
## Detailed Description
This function iterates through the synchronous replication wait queue for a specific mode and validates that all processes in the queue are ordered by their waitLSN values in ascending order. It also ensures that no two processes have the same LSN value. This is primarily used for debugging and assertion purposes to maintain the integrity of the synchronous replication queue ordering, which is critical for proper synchronous replication behavior in PostgreSQL.

The function walks through the doubly-linked list of processes waiting for synchronous replication confirmation and compares each process's waitLSN with the previous one to ensure strict ascending order.

## Parameters / Member Variables
- `mode`: Integer specifying which synchronous replication wait queue to check (must be between 0 and NUM_SYNC_REP_WAIT_MODE-1)
## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (doubly-linked list iteration macro)
  - dlist_container (macro to get container structure from list node)
  - Assert (assertion macro)
- Called from (representative examples):
  - SyncStandbysDefined
  - [SyncRepWaitForLSN](SyncRepWaitForLSN.md)
  - [SyncRepWakeQueue](SyncRepWakeQueue.md)

## Notes and Other Information
- This is a static function used internally within the syncrep.c module
- Returns false if the queue is not properly ordered or if duplicate LSNs are found
- Uses WalSndCtl->SyncRepQueue[mode] to access the specific wait queue
- The function assumes that lastLSN starts at 0, meaning all valid LSNs in the queue should be greater than 0
- This validation is crucial for maintaining the correctness of synchronous replication ordering

## Simplified Source

```c
// Simplified version of SyncRepQueueIsOrderedByLSN
static bool SyncRepQueueIsOrderedByLSN(int mode) {
    // Validate mode parameter
    Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);

    XLogRecPtr lastLSN = 0;
    dlist_iter iter;

    // Check each process in the sync replication queue
    dlist_foreach(iter, &WalSndCtl->SyncRepQueue[mode]) {
        PGPROC *proc = dlist_container(PGPROC, syncRepLinks, iter.cur);

        // Ensure LSNs are in strictly ascending order
        if (proc->waitLSN <= lastLSN) {
            return false;  // Queue is not properly ordered
        }

        lastLSN = proc->waitLSN;
    }

    return true;  // Queue is properly ordered
}
```

Key simplifications made:
- Combined variable declarations for clarity
- Added descriptive comments for each major step
- Simplified the LSN comparison logic explanation
- Focused on the core validation: strict ascending order of LSNs
- Made the return conditions more explicit with comments