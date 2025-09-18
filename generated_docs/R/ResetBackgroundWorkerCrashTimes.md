# ResetBackgroundWorkerCrashTimes

## Location
src/backend/postmaster/bgworker.c: 585 - 636

## Overview
Resets background worker crash state after a crash-and-restart cycle, allowing restartable workers to restart immediately while removing non-restartable workers from the system.

## Definition


## Detailed Description
This function handles the cleanup and reset of background worker states following a PostgreSQL crash and restart cycle. It iterates through all registered background workers and takes different actions based on their restart configuration: (1) Workers marked with BGW_NEVER_RESTART are completely removed from the system via ForgetBackgroundWorker, as they should not be relaunched after crashes; (2) Restartable workers have their crash timestamp reset to 0 (allowing immediate restart) and their notification PID cleared (since waiting processes are no longer valid after a crash). The function includes critical assertions to ensure parallel workers are never configured as restartable, as this would corrupt the parallel worker accounting system.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach_modify (macro for safely iterating and modifying singly-linked lists)
  - slist_container (macro to get container structure from list node)
  - [ForgetBackgroundWorker](../F/ForgetBackgroundWorker.md) (removes worker registration)
  - BGW_NEVER_RESTART (constant indicating worker should not restart)
  - BGWORKER_CLASS_PARALLEL (flag indicating parallel worker class)
- Data structures used:
  - [slist_mutable_iter](../s/slist_mutable_iter.md)
  - [RegisteredBgWorker](RegisteredBgWorker.md)
  - BackgroundWorkerList (global list of registered background workers)
- Called from (representative examples):
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)

## Notes and Other Information
- This function should only be called from the postmaster process
- Called specifically after PostgreSQL crash-and-restart cycles
- Uses slist_foreach_modify to safely iterate and potentially remove items from the list
- Contains critical assertion that parallel workers must be marked BGW_NEVER_RESTART to prevent accounting corruption
- Clearing notification PIDs is necessary because waiting processes are invalid after a crash
- Resetting rw_crashed_at to 0 allows immediate restart instead of waiting for bgw_restart_time
- Essential for maintaining system consistency in background worker management after crashes
- Part of PostgreSQL's crash recovery process for background worker subsystem