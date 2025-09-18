# BackgroundWorkerStopNotifications

## Location
[src/backend/postmaster/bgworker.c:520-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L520-L546)

## Overview
Cancels SIGUSR1 notifications for a specific PID by clearing the notification PID from all registered background workers that were set to notify that process.

## Definition


## Detailed Description
This function iterates through all registered background workers and clears the bgw_notify_pid field for any workers that were configured to send SIGUSR1 notifications to the specified PID. This is typically called when a backend process is exiting to prevent the postmaster from attempting to send signals to a non-existent process. The function ensures that no orphaned notification references remain after a process terminates, preventing potential issues with signal delivery to invalid PIDs.

## Parameters / Member Variables
- `pid`: The process ID of the exiting backend for which all notification references should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach (macro for iterating through singly-linked lists)
  - slist_container (macro to get container structure from list node)
- Data structures used:
  - pid_t
  - [slist_iter](../s/slist_iter.md)
  - [RegisteredBgWorker](../R/RegisteredBgWorker.md)
  - BackgroundWorkerList (global list of registered background workers)
- Called from (representative examples):
  - [CleanupBackgroundWorker](../C/CleanupBackgroundWorker.md)
  - [CleanupBackend](../C/CleanupBackend.md)

## Notes and Other Information
- This function should only be called from the postmaster process
- Part of the cleanup process when backend processes terminate
- Prevents stale notification PIDs that could cause signal delivery errors
- Uses slist_foreach to safely iterate through the BackgroundWorkerList
- Essential for maintaining consistency in the background worker notification system during process lifecycle management