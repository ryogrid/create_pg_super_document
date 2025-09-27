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

## Simplified Source

```c
// Simplified version of HandleChildCrash
static void HandleChildCrash(int pid, int exitstatus, const char *procname) {
    bool take_action;

    // Core logic step 1: Determine if we should take action
    // Only act on first crash and not during immediate shutdown
    take_action = !FatalError && Shutdown != ImmediateShutdown;

    // Core logic step 2: Log the crash and announce termination
    if (take_action) {
        LogChildExit(LOG, procname, pid, exitstatus);
        ereport(LOG, (errmsg("terminating any other active server processes")));
        SetQuitSignalReason(PMQUIT_FOR_CRASH);
    }

    // Core logic step 3: Process background workers
    slist_foreach(siter, &BackgroundWorkerList) {
        RegisteredBgWorker *worker = slist_container(RegisteredBgWorker, rw_lnode, siter.cur);

        if (worker->rw_pid == 0)
            continue;  // not running

        if (worker->rw_pid == pid) {
            // Clean up crashed worker
            ReleasePostmasterChildSlot(worker->rw_child_slot);
            dlist_delete(&worker->rw_backend->elem);
            pfree(worker->rw_backend);
            worker->rw_backend = NULL;
            worker->rw_pid = 0;
            worker->rw_child_slot = 0;
        } else {
            // Signal other workers to quit
            if (take_action)
                sigquit_child(worker->rw_pid);
        }
    }

    // Core logic step 4: Process regular backends
    dlist_foreach_modify(iter, &BackendList) {
        Backend *backend = dlist_container(Backend, elem, iter.cur);

        if (backend->pid == pid) {
            // Clean up crashed backend
            if (!backend->dead_end)
                ReleasePostmasterChildSlot(backend->child_slot);
            dlist_delete(iter.cur);
            pfree(backend);
        } else {
            // Signal other backends to quit (skip background workers)
            if (backend->bkend_type != BACKEND_TYPE_BGWORKER && take_action)
                sigquit_child(backend->pid);
        }
    }

    // Core logic step 5: Handle auxiliary processes
    // Startup process
    if (pid == StartupPID) {
        StartupPID = 0;
    } else if (StartupPID != 0 && take_action) {
        sigquit_child(StartupPID);
        StartupStatus = STARTUP_SIGNALED;
    }

    // Handle other auxiliary processes (bgwriter, checkpointer, walwriter, etc.)
    handle_auxiliary_process(&BgWriterPID, pid, take_action);
    handle_auxiliary_process(&CheckpointerPID, pid, take_action);
    handle_auxiliary_process(&WalWriterPID, pid, take_action);
    handle_auxiliary_process(&WalReceiverPID, pid, take_action);
    handle_auxiliary_process(&WalSummarizerPID, pid, take_action);
    handle_auxiliary_process(&AutoVacPID, pid, take_action);
    handle_auxiliary_process(&PgArchPID, pid, take_action);
    handle_auxiliary_process(&SlotSyncWorkerPID, pid, take_action);

    // Core logic step 6: Transition to emergency shutdown mode
    if (Shutdown != ImmediateShutdown)
        FatalError = true;

    // Transition postmaster state to waiting for backends to die
    if (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_RUN ||
        pmState == PM_STOP_BACKENDS || pmState == PM_SHUTDOWN)
        pmState = PM_WAIT_BACKENDS;

    // Start the abort timer for forceful termination
    if (AbortStartTime == 0)
        AbortStartTime = time(NULL);
}

// Helper function for auxiliary process handling (conceptual)
static void handle_auxiliary_process(int *process_pid, int crashed_pid, bool take_action) {
    if (crashed_pid == *process_pid) {
        *process_pid = 0;  // Mark as not running
    } else if (*process_pid != 0 && take_action) {
        sigquit_child(*process_pid);  // Signal to quit
    }
}
```

Key simplifications made:
- Consolidated repetitive auxiliary process handling into a conceptual helper function
- Removed platform-specific EXEC_BACKEND code blocks for clarity
- Simplified iterator and container extraction logic
- Focused on the main execution path without detailed error handling
- Added clear step-by-step comments for the core logic flow
- Abstracted low-level list manipulation details
- Maintained the essential algorithm: identify crashed process, clean up, signal others, transition state