# ProcKill

## Location
src/backend/storage/lmgr/proc.c: 839 - 971

## Overview
Destroys the per-process data structure for the current process, releasing held LW locks and performing comprehensive cleanup during process termination.

## Definition


## Detailed Description
ProcKill is a comprehensive process cleanup function that handles the orderly termination of a PostgreSQL backend process. This function performs extensive cleanup operations to ensure that no resources are leaked and shared data structures remain consistent when a process exits.

Key cleanup operations include:
1. Safety checks to ensure it's not called in a child process created by system()
2. Cleanup from synchronous replication lists
3. Assertion checks for proper lock release in debug builds
4. Release of any remaining LW locks and condition variables
5. Complex lock group management and cleanup
6. Latch ownership transfer back to local process
7. PGPROC structure cleanup and return to freelists
8. Postmaster notification for proper process lifecycle management
9. Autovacuum launcher notification when needed

The function handles sophisticated scenarios like lock group leadership transfer when the leader exits before group members.

## Parameters / Member Variables
- : Exit code (unused in this function but required by exit callback interface)
- : Datum argument (unused in this function but required by exit callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - [SyncRepCleanupAtProcExit](../S/SyncRepCleanupAtProcExit.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - LWLockReleaseAll
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - LockHashPartitionLockByProc
  - LWLockAcquire
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [SwitchBackToLocalLatch](../S/SwitchBackToLocalLatch.md)
  - [pgstat_reset_wait_event_storage](../p/pgstat_reset_wait_event_storage.md)
  - [DisownLatch](../D/DisownLatch.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - update_spins_per_delay
  - MarkPostmasterChildInactive
  - AmAutoVacuumLauncherProcess
  - AmLogicalSlotSyncWorkerProcess
  - kill
- Called from (representative examples):
  - InitProcess (registered as exit callback)

## Notes and Other Information
- This is a static function, only accessible within proc.c
- Function parameters follow the standard exit callback signature but are not used
- Includes safety check to prevent execution in child processes created by system() calls
- Handles complex lock group scenarios where leaders may exit before members
- Performs different cleanup based on whether the process is under postmaster control
- Updates global spin delay statistics as part of cleanup
- Sends SIGUSR2 to autovacuum launcher if needed to wake it up after worker termination
- The function ensures proper transfer of latch ownership to prevent resource leaks
- Registered as an exit callback during process initialization for automatic cleanup