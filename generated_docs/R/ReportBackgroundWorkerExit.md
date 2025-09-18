# ReportBackgroundWorkerExit

## Location
src/backend/postmaster/bgworker.c: 486 - 519

## Overview
Reports that a background worker has exited by updating the shared memory slot and optionally notifying the requesting process, with logic to handle worker deregistration before notification.

## Definition


## Detailed Description
This function handles the cleanup and notification process when a background worker terminates. It updates the shared memory slot with the worker's final PID (typically 0), and if the worker is marked for termination or configured to never restart, it deregisters the worker before sending notification. The function strategically performs deregistration before notification to minimize race conditions where a requesting process might try to reuse a slot that's not yet available. After cleanup, it sends a SIGUSR1 signal to notify the requesting process (if specified) about the worker's termination.

## Parameters / Member Variables
- `cur`: Pointer to slist_mutable_iter that contains the current position in the registered workers list, used to access the specific RegisteredBgWorker being processed

## Dependencies
- Functions called/Symbols referenced:
  - slist_container (macro to get container structure from list node)
  - ForgetBackgroundWorker (removes worker from registration)
  - kill (system call for sending signals)
  - SIGUSR1 (signal constant)
  - BGW_NEVER_RESTART (constant indicating worker should not restart)
- Data structures used:
  - slist_mutable_iter
  - RegisteredBgWorker
  - BackgroundWorkerSlot
  - BackgroundWorkerData (global shared memory structure)
- Called from (representative examples):
  - CleanupBackgroundWorker

## Notes and Other Information
- This function should only be called from the postmaster process
- Includes an assertion to ensure the shared memory slot index is within valid bounds (< max_worker_processes)
- The function handles both terminating workers and workers configured to never restart by calling ForgetBackgroundWorker
- Strategic ordering of deregistration before notification helps prevent race conditions in slot reuse
- Part of PostgreSQL's background worker lifecycle management, specifically handling the termination phase