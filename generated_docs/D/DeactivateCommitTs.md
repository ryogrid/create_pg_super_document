# DeactivateCommitTs

## Location
src/backend/access/transam/commit_ts.c: 785 - 826

## Overview
Deactivates the commit timestamp tracking module and cleans up all associated data and state when the track_commit_timestamp parameter is turned off.

## Definition


## Detailed Description
DeactivateCommitTs is responsible for safely shutting down the commit timestamp tracking functionality in PostgreSQL. This function is called when the track_commit_timestamp parameter is disabled, which can happen during postmaster startup, standalone-backend startup, or during WAL replay.

The function performs comprehensive cleanup by:
1. Acquiring an exclusive lock on CommitTsLock to ensure thread safety
2. Resetting all shared memory state in commitTsShared to prevent invalid data from being returned
3. Invalidating transaction ID tracking variables in TransamVariables
4. Removing all commit timestamp SLRU files from disk to maintain data consistency
5. Releasing the lock after cleanup is complete

The function is designed to be very thorough in its cleanup to prevent any confusion or data corruption when the feature is later re-enabled after being disabled.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (CommitTsLock, LW_EXCLUSIVE)
  - LWLockRelease (CommitTsLock)
  - SlruScanDirectory (CommitTsCtl, SlruScanDirCbDeleteAll, NULL)
  - TIMESTAMP_NOBEGIN (macro)
  - InvalidTransactionId (constant)
  - InvalidRepOriginId (constant)
  - CommitTsCtl (SLRU control structure)
  - SlruScanDirCbDeleteAll (callback function)

- Called from (representative examples):
  - CompleteCommitTsInitialization
  - CommitTsParameterChange

## Notes and Other Information
- This is a static function, only callable within the commit_ts.c module
- The function uses heavy-handed exclusive locking because it's expected to be called rarely and only on replicas
- All SLRU files are removed to prevent gaps in the file sequence when the feature is later re-enabled
- The function resets both shared memory state and transaction variables to ensure no stale data remains
- No process should be consulting the commit timestamp SLRU when this function is called, as the feature has just been deactivated