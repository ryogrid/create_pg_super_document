# CheckRecoveryConsistency

## Location
src/backend/access/transam/xlogrecovery.c: 2175 - 2274

## Overview
CheckRecoveryConsistency determines if WAL recovery has reached a consistent state and enables Hot Standby connections when appropriate consistency and snapshot conditions are met.

## Definition


## Detailed Description
CheckRecoveryConsistency is a critical function that manages the transition from WAL recovery to a consistent database state. It performs several key checks and actions:

1. **Crash Recovery Handling**: For crash recovery (non-archive recovery), consistency is only reached when all WAL has been replayed
2. **Backup Completion Detection**: Checks if recovery has reached the end of a base backup by comparing backupEndPoint with the last replayed LSN
3. **Minimum Recovery Point Validation**: Verifies that recovery has progressed beyond the minimum recovery point required for consistency
4. **Invalid Page Checking**: Runs XLogCheckInvalidPages() to verify no unresolved references to uninitialized pages remain
5. **Tablespace Directory Validation**: Calls CheckTablespaceDirectory() to ensure pg_tblspc contains only symbolic links
6. **Hot Standby Activation**: When all conditions are met (snapshot ready, consistency reached, running under postmaster), enables Hot Standby mode by signaling the postmaster

The function maintains several global state variables and coordinates with the postmaster to allow read-only connections once the database reaches a consistent state.

## Parameters / Member Variables
This function takes no parameters and operates on global recovery state variables and shared memory structures.

## Dependencies
- Functions called/Symbols referenced:
  - ReachedEndOfBackup
  - XLogCheckInvalidPages
  - CheckTablespaceDirectory
  - SendPostmasterSignal
  - XLogRecPtrIsInvalid
  - ereport/elog
- Called from:
  - PerformWalRecovery (src/backend/access/transam/xlogrecovery.c:1696)
  - ApplyWalRecord (src/backend/access/transam/xlogrecovery.c:2051)
  - ReadRecord (src/backend/access/transam/xlogrecovery.c:3248)

## Notes and Other Information
- This is a static function only called from within the xlogrecovery.c module
- The function is called multiple times during recovery to check for consistency at various points
- For archive recovery, consistency depends on reaching minRecoveryPoint and completing any required backup
- The function handles both backup recovery scenarios and regular archive recovery
- Hot Standby activation requires: snapshot readiness, consistency, and running under postmaster
- Global variables managed include reachedConsistency, backupStartPoint, backupEndPoint, and LocalHotStandbyActive
- The function uses minimal locking, assuming it runs in the startup process context
- Detailed logging is provided for backup completion and consistency achievement
- The function coordinates with CheckTablespaceDirectory to ensure proper tablespace structure