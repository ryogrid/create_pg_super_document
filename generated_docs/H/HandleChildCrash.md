# HandleChildCrash

## Location
[src/backend/postmaster/postmaster.c:2875-3061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2875-L3061)

## Overview
HandleChildCrash manages the emergency shutdown sequence when a critical PostgreSQL process crashes, cleaning up local state and signaling all remaining child processes to terminate immediately.

## Definition
static void HandleChildCrash(int pid, int exitstatus, const char *procname)

## Detailed Description
HandleChildCrash is a critical emergency response function in PostgreSQL's postmaster that handles the catastrophic failure of any important child process (backend, bgwriter, checkpointer, walwriter, autovacuum, archiver, slot sync worker, or background worker). The function performs a two-phase operation: first, it cleans up the local state associated with the crashed process by removing it from tracking lists and releasing resources; second, it signals all other remaining child processes to terminate quickly using sigquit_child. The function includes special logic to prevent duplicate actions during cascading failures and immediate shutdowns. It systematically processes background workers, regular backends, and various auxiliary processes (startup, bgwriter, checkpointer, walwriter, walreceiver, walsummarizer, autovacuum, archiver, slot sync worker). After cleanup, it transitions the postmaster state to PM_WAIT_BACKENDS and sets FatalError to true, effectively putting the system into emergency shutdown mode.

## Parameters / Member Variables
- : Process ID of the crashed child process to be cleaned up
- : Exit status of the crashed process, used for logging purposes
- : Human-readable name of the process type for logging (e.g., "server process", "background worker")

## Dependencies
- Functions called/Symbols referenced:
  - [LogChildExit](../L/LogChildExit.md)
  - [SetQuitSignalReason](../S/SetQuitSignalReason.md)
  - [ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md)
  - [ShmemBackendArrayRemove](../S/ShmemBackendArrayRemove.md)
  - [sigquit_child](../s/sigquit_child.md)
  - slist_foreach
  - dlist_foreach_modify
  - dlist_container
  - slist_container
  - [dlist_delete](../d/dlist_delete.md)
- Called from (representative examples):
  - [process_pm_child_exit](../p/process_pm_child_exit.md)
  - [CleanupBackend](../C/CleanupBackend.md)
  - [CleanupBackgroundWorker](../C/CleanupBackgroundWorker.md)

## Notes and Other Information
- This function implements PostgreSQL's "fail-fast" philosophy where any critical process failure triggers complete system shutdown
- Uses take_action flag to prevent redundant operations during cascading failures
- Handles both background workers (using singly-linked list) and regular backends (using doubly-linked list) separately
- Sets AbortStartTime to start the timer for forceful process termination if graceful shutdown fails
- Does NOT restart the syslogger process as it's considered non-critical
- Critical for maintaining data integrity by ensuring clean shutdown when corruption might have occurred