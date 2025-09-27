# SyncRepCleanupAtProcExit

## Location
[src/backend/replication/syncrep.c:416-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L416-L444)

## Overview
Performs cleanup of synchronous replication state when a process exits, ensuring that the process is properly removed from any synchronous replication wait queues.

## Definition
```c
void SyncRepCleanupAtProcExit(void)
```

## Detailed Description
This function is called during process exit to ensure proper cleanup of synchronous replication state. It implements a two-phase check optimization to minimize lock contention during normal process termination. The function first performs a lockless check to see if the process is actually queued for synchronous replication, only acquiring the expensive SyncRepLock if removal is necessary.

The double-check pattern (check without lock, acquire lock, check again) prevents race conditions where another process (such as a WAL sender) might remove the current process from the queue between the initial check and lock acquisition. This ensures both correctness and performance during process exit.

If the process is found to be in a synchronous replication queue, the function performs a thorough deletion that both removes the process from the queue and reinitializes its list node links to prevent dangling pointers.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node_is_detached](../d/dlist_node_is_detached.md) (checks if process is queued)
  - [LWLockAcquire](../L/LWLockAcquire.md) (acquires SyncRepLock exclusively)
  - [LWLockRelease](../L/LWLockRelease.md) (releases SyncRepLock)
  - [dlist_delete_thoroughly](../d/dlist_delete_thoroughly.md) (removes and reinitializes list node)
- Called from (representative examples):
  - [ProcKill](../P/ProcKill.md) (during process termination)

## Notes and Other Information
The function implements an important optimization for process exit performance by avoiding lock acquisition in the common case where the process is not waiting for synchronous replication. The double-check pattern is essential to handle race conditions with WAL sender processes that may concurrently remove processes from wait queues. This cleanup is crucial for maintaining queue integrity and preventing memory leaks or dangling references in shared memory structures.

## Simplified Source

```c
// Simplified version of SyncRepCleanupAtProcExit
void SyncRepCleanupAtProcExit(void) {
    // Core logic step 1: Quick check without lock for performance
    // Most processes won't be in sync rep queue, so avoid expensive lock
    if (!dlist_node_is_detached(&MyProc->syncRepLinks)) {

        // Core logic step 2: Acquire exclusive lock for safe removal
        LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

        // Core logic step 3: Double-check to handle race conditions
        // WAL sender might have removed us while we acquired the lock
        if (!dlist_node_is_detached(&MyProc->syncRepLinks)) {
            dlist_delete_thoroughly(&MyProc->syncRepLinks);
        }

        // Core logic step 4: Release the lock
        LWLockRelease(SyncRepLock);
    }
}
```

Key simplifications made:
- Added descriptive comments explaining the optimization strategy
- Clarified the double-check pattern and its purpose
- Explained the race condition scenario with WAL sender
- Focused on the main execution path logic
- Maintained the essential algorithm structure