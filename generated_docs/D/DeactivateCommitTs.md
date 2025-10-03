# DeactivateCommitTs

## Location
[src/backend/access/transam/commit_ts.c:785-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L785-L826)

## Overview
Deactivates the commit timestamp tracking module and cleans up all associated data and state when the track_commit_timestamp parameter is turned off.

## Definition

```c
static void
DeactivateCommitTs(void)
```
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

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (CommitTsLock, LW_EXCLUSIVE)
  - [LWLockRelease](../L/LWLockRelease.md) (CommitTsLock)
  - [SlruScanDirectory](../S/SlruScanDirectory.md) (CommitTsCtl, SlruScanDirCbDeleteAll, NULL)
  - TIMESTAMP_NOBEGIN (macro)
  - InvalidTransactionId (constant)
  - InvalidRepOriginId (constant)
  - CommitTsCtl (SLRU control structure)
  - [SlruScanDirCbDeleteAll](../S/SlruScanDirCbDeleteAll.md) (callback function)

- Called from (representative examples):
  - [CompleteCommitTsInitialization](../C/CompleteCommitTsInitialization.md)
  - [CommitTsParameterChange](../C/CommitTsParameterChange.md)

## Notes and Other Information
- This is a static function, only callable within the commit_ts.c module
- The function uses heavy-handed exclusive locking because it's expected to be called rarely and only on replicas
- All SLRU files are removed to prevent gaps in the file sequence when the feature is later re-enabled
- The function resets both shared memory state and transaction variables to ensure no stale data remains
- No process should be consulting the commit timestamp SLRU when this function is called, as the feature has just been deactivated

## Simplified Source

```c
// Simplified version of DeactivateCommitTs
static void
DeactivateCommitTs(void)
{
    // Acquire exclusive lock for safe cleanup
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);

    // Reset shared memory state to prevent invalid data
    commitTsShared->commitTsActive = false;
    commitTsShared->xidLastCommit = InvalidTransactionId;
    TIMESTAMP_NOBEGIN(commitTsShared->dataLastCommit.time);
    commitTsShared->dataLastCommit.nodeid = InvalidRepOriginId;

    // Clear transaction tracking variables
    TransamVariables->oldestCommitTsXid = InvalidTransactionId;
    TransamVariables->newestCommitTsXid = InvalidTransactionId;

    // Remove all commit timestamp files to maintain consistency
    (void) SlruScanDirectory(CommitTsCtl, SlruScanDirCbDeleteAll, NULL);

    // Release the lock
    LWLockRelease(CommitTsLock);
}
```

Key simplifications made:
- Condensed detailed comments into brief descriptions of each major step
- Preserved all essential function calls and state changes
- Maintained the critical locking mechanism for thread safety
- Kept the complete cleanup logic while making it more readable
- Removed verbose explanatory comments while preserving core functionality