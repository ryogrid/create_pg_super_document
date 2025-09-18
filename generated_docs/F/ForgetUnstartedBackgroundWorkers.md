# ForgetUnstartedBackgroundWorkers

## Location
src/backend/postmaster/bgworker.c: 547 - 584

## Overview
Cancels all not-yet-started background worker requests that have waiting processes during database shutdown, notifying the waiting processes and cleaning up the registrations.

## Definition


## Detailed Description
This function is called during normal ("smart" or "fast") database shutdown to handle background workers that were registered but never started. It iterates through all registered background workers and identifies those that haven't been started yet (slot->pid == InvalidPid) and have processes waiting for them (bgw_notify_pid != 0). For each such worker, it completely removes the registration and sends a SIGUSR1 signal to notify the waiting process. This prevents processes from waiting indefinitely for background workers that will never start due to shutdown. The approach of canceling registrations entirely is considered acceptable during shutdown since the server is terminating anyway.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach_modify (macro for safely iterating and modifying singly-linked lists)
  - slist_container (macro to get container structure from list node)
  - [ForgetBackgroundWorker](ForgetBackgroundWorker.md) (removes worker registration)
  - kill (system call for sending signals)
  - SIGUSR1 (signal constant)
  - InvalidPid (constant representing invalid process ID)
- Data structures used:
  - [slist_mutable_iter](../s/slist_mutable_iter.md)
  - [RegisteredBgWorker](../R/RegisteredBgWorker.md)
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md)
  - BackgroundWorkerList (global list of registered background workers)
  - BackgroundWorkerData (global shared memory structure)
- Called from (representative examples):
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)

## Notes and Other Information
- This function should only be called from the postmaster process
- Called specifically during "smart" or "fast" database shutdown scenarios
- Uses slist_foreach_modify to safely iterate and remove items from the list
- Includes assertion to ensure shared memory slot index is within bounds
- Prevents indefinite waiting for background workers that cannot start due to shutdown
- Part of PostgreSQL's graceful shutdown process for background worker management
- The complete cancellation of registrations is intentionally "overkill" but acceptable during shutdown