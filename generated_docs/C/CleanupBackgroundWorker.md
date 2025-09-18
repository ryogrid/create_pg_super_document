# CleanupBackgroundWorker

## Location
src/backend/postmaster/postmaster.c: 2696 - 2790

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
  - `[GetCurrentTimestamp](../G/GetCurrentTimestamp.md)` - Records crash timestamp for restart timing
  - `[HandleChildCrash](../H/HandleChildCrash.md)` - Initiates crash recovery for abnormal exits
  - `[ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md)` - Releases allocated process slot
  - `[BackgroundWorkerStopNotifications](../B/BackgroundWorkerStopNotifications.md)` - Cancels worker notifications
  - `[ReportBackgroundWorkerExit](../R/ReportBackgroundWorkerExit.md)` - Reports worker termination
  - `[LogChildExit](../L/LogChildExit.md)` - Logs worker exit information
  - `[dlist_delete](../d/dlist_delete.md)` - Removes worker from backend list
- Called from (representative examples):
  - `[process_pm_child_exit](../p/process_pm_child_exit.md)` - Child exit processing function

## Notes and Other Information
- Returns `true` if the PID belonged to a background worker, `false` otherwise
- Based heavily on `CleanupBackend` but designed specifically for background workers
- Handles Windows-specific exit status translation (ERROR_WAIT_NO_CHILDREN → 0)
- Workers that fail to release their child slot trigger crash recovery
- The function manages background worker restart logic through crash timestamps
- Supports notification cancellation for workers that started other workers
- Integrates with EXEC_BACKEND mode for shared memory management
- Different log levels used based on exit status (DEBUG1 for normal, LOG for abnormal)