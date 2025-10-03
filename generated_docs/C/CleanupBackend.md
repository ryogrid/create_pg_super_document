# CleanupBackend

## Location
[src/backend/postmaster/postmaster.c:2791-2874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2791-L2874)

## Overview
CleanupBackend handles the cleanup operations after a PostgreSQL backend process terminates, removing all local state associated with the backend and determining if the termination requires emergency actions.

## Definition

```c
static void
CleanupBackend(int pid,
			   int exitstatus)	/* child's exit status. */
```
## Detailed Description
CleanupBackend is a critical function in PostgreSQL's postmaster process that manages the aftermath of backend process termination. The function first logs the child exit event and then analyzes the exit status to determine the appropriate response. For normal exits (status 0 or 1), it performs orderly cleanup by removing the backend from the active backend list, releasing child slots, and canceling any background worker notifications. However, if the backend dies with an abnormal exit status, it triggers HandleChildCrash to initiate emergency procedures that signal all other backends to terminate quickly. On Windows, the function includes special handling for ERROR_WAIT_NO_CHILDREN (128) which is treated as a non-fatal case due to mutex-related startup issues.

## Parameters / Member Variables
- `pid`: Process ID of the terminated backend process
- `exitstatus`: Exit status code of the terminated backend, used to determine if cleanup was normal or if emergency procedures are needed
## Dependencies
- Functions called/Symbols referenced:
  - [LogChildExit](../L/LogChildExit.md)
  - EXIT_STATUS_0
  - EXIT_STATUS_1
  - [HandleChildCrash](../H/HandleChildCrash.md)
  - [ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md)
  - [ShmemBackendArrayRemove](../S/ShmemBackendArrayRemove.md)
  - [BackgroundWorkerStopNotifications](../B/BackgroundWorkerStopNotifications.md)
  - dlist_foreach_modify
  - dlist_container
  - [dlist_delete](../d/dlist_delete.md)
- Called from (representative examples):
  - [process_pm_child_exit](../p/process_pm_child_exit.md)

## Notes and Other Information
- This function is platform-specific, with special Windows handling for ERROR_WAIT_NO_CHILDREN
- The function distinguishes between normal exits (0, 1) and crash scenarios requiring emergency response
- It's closely related to CleanupBackgroundWorker and changes should be coordinated
- Uses doubly-linked list operations to manage the BackendList
- Critical for maintaining system stability during backend failures

## Simplified Source

```c
// Simplified version of CleanupBackend
static void CleanupBackend(int pid, int exitstatus) {
    // Log the backend termination event
    LogChildExit(DEBUG2, "server process", pid, exitstatus);

    // Handle platform-specific cases (Windows mutex issues)
#ifdef WIN32
    if (exitstatus == ERROR_WAIT_NO_CHILDREN) {
        LogChildExit(LOG, "server process", pid, exitstatus);
        exitstatus = 0;  // Treat as normal exit
    }
#endif

    // Check if this was an abnormal exit requiring emergency action
    if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus)) {
        // Backend crashed - trigger emergency shutdown of all backends
        HandleChildCrash(pid, exitstatus, "server process");
        return;
    }

    // Normal exit - clean up backend resources
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &BackendList) {
        Backend *bp = dlist_container(Backend, elem, iter.cur);

        if (bp->pid == pid) {
            // Release backend slot if not a dead-end connection
            if (!bp->dead_end) {
                if (!ReleasePostmasterChildSlot(bp->child_slot)) {
                    // Cleanup failed - treat as crash
                    HandleChildCrash(pid, exitstatus, "server process");
                    return;
                }
#ifdef EXEC_BACKEND
                ShmemBackendArrayRemove(bp);
#endif
            }

            // Cancel any pending background worker notifications
            if (bp->bgworker_notify) {
                BackgroundWorkerStopNotifications(bp->pid);
            }

            // Remove backend from list and free memory
            dlist_delete(iter.cur);
            pfree(bp);
            break;
        }
    }
}
```

Key simplifications made:
- Removed detailed comments for clarity while keeping essential ones
- Consolidated platform-specific handling into clearer blocks
- Simplified the main logic flow into distinct phases: logging, crash detection, and cleanup
- Focused on the core algorithm: determine exit type, then either crash or clean up
- Maintained all essential error handling and resource management