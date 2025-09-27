# CleanupBackgroundWorker

## Location
[src/backend/postmaster/postmaster.c:2696-2790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2696-L2790)

## Overview
Handles the cleanup and resource deallocation when a background worker process exits or crashes, determining whether the process should be restarted or terminated.

## Definition
```c
static bool CleanupBackgroundWorker(int pid, int exitstatus)
```

## Detailed Description
This function searches through the list of registered background workers to find a worker with the given PID, then performs cleanup operations when the worker terminates. It handles both normal termination and crash scenarios:

- **Normal Exit (status 0)**: Marks the worker for termination and prevents restart
- **FATAL Exit (status 1)**: Allowed exit, schedules worker for restart
- **Crash (other statuses)**: Triggers system-wide crash recovery

The function manages resource cleanup including releasing postmaster child slots, removing the worker from backend lists, canceling notifications, and freeing memory. It also determines whether the worker should be restarted based on the exit status and crash timestamp.

## Parameters / Member Variables
- `pid`: Process ID of the exited background worker
- `exitstatus`: Exit status code from the terminated process

## Dependencies
- Functions called/Symbols referenced:
  - `slist_foreach_modify` - Iterates through background worker list
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) - Records crash timestamp for restart timing
  - [HandleChildCrash](../H/HandleChildCrash.md) - Initiates crash recovery for abnormal exits
  - [ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md) - Releases allocated process slot
  - [BackgroundWorkerStopNotifications](../B/BackgroundWorkerStopNotifications.md) - Cancels worker notifications
  - [ReportBackgroundWorkerExit](../R/ReportBackgroundWorkerExit.md) - Reports worker termination
  - [LogChildExit](../L/LogChildExit.md) - Logs worker exit information
  - [dlist_delete](../d/dlist_delete.md) - Removes worker from backend list
- Called from (representative examples):
  - [process_pm_child_exit](../p/process_pm_child_exit.md) - Child exit processing function

## Notes and Other Information
- Returns `true` if the PID belonged to a background worker, `false` otherwise
- Based heavily on `CleanupBackend` but designed specifically for background workers
- Handles Windows-specific exit status translation (ERROR_WAIT_NO_CHILDREN → 0)
- Workers that fail to release their child slot trigger crash recovery
- The function manages background worker restart logic through crash timestamps
- Supports notification cancellation for workers that started other workers
- Integrates with EXEC_BACKEND mode for shared memory management
- Different log levels used based on exit status (DEBUG1 for normal, LOG for abnormal)

## Simplified Source

```c
// Simplified version of CleanupBackgroundWorker
static bool CleanupBackgroundWorker(int pid, int exitstatus) {
    // Search through all registered background workers
    slist_foreach_modify(iter, &BackgroundWorkerList) {
        RegisteredBgWorker *worker = slist_container(RegisteredBgWorker, rw_lnode, iter.cur);

        // Skip if this worker doesn't match the PID
        if (worker->rw_pid != pid)
            continue;

        // Windows-specific: normalize exit status
        #ifdef WIN32
        if (exitstatus == ERROR_WAIT_NO_CHILDREN)
            exitstatus = 0;
        #endif

        // Handle different exit scenarios
        if (EXIT_STATUS_0(exitstatus)) {
            // Normal exit - mark for termination, don't restart
            worker->rw_crashed_at = 0;
            worker->rw_terminate = true;
        } else {
            // Abnormal exit - record crash time for restart logic
            worker->rw_crashed_at = GetCurrentTimestamp();
        }

        // Check for system crash conditions (exit codes other than 0 or 1)
        if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus)) {
            HandleChildCrash(pid, exitstatus, worker_name);
            return true;
        }

        // Release the postmaster child slot
        if (!ReleasePostmasterChildSlot(worker->rw_child_slot)) {
            // Failed to release slot - trigger crash recovery
            HandleChildCrash(pid, exitstatus, worker_name);
            return true;
        }

        // Clean up worker resources
        dlist_delete(&worker->rw_backend->elem);

        // Cancel any notifications this worker was waiting for
        if (worker->rw_backend->bgworker_notify)
            BackgroundWorkerStopNotifications(worker->rw_pid);

        // Free memory and reset worker state
        pfree(worker->rw_backend);
        worker->rw_backend = NULL;
        worker->rw_pid = 0;
        worker->rw_child_slot = 0;

        // Report the worker exit and log it
        ReportBackgroundWorkerExit(&iter);
        LogChildExit(log_level, worker_name, pid, exitstatus);

        return true; // Found and processed the worker
    }

    return false; // PID was not a background worker
}
```

Key simplifications made:
- Removed detailed name buffer construction for clarity
- Consolidated exit status handling logic
- Abstracted platform-specific details with comments
- Simplified the main cleanup sequence
- Focused on the core algorithm: find worker, handle exit status, clean up resources
- Removed EXEC_BACKEND specific code for clarity
- Used descriptive variable names and added flow comments