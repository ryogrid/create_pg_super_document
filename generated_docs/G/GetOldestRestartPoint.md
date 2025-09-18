# GetOldestRestartPoint

## Location
[src/backend/access/transam/xlog.c:9479-9488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L9479-L9488)

## Overview
Returns the redo pointer and timeline ID of the last checkpoint or restartpoint, representing the oldest point in WAL that would be needed if recovery were to restart.

## Definition


## Detailed Description
This function provides access to the redo point information from the most recent checkpoint or restartpoint stored in the control file. The redo pointer represents the oldest WAL location that would need to be replayed if the system were to restart recovery from this checkpoint. The function safely reads this information under the protection of the ControlFileLock to ensure consistency.

The function copies the redo pointer and timeline ID from the control file's checkPointCopy structure, which contains the information from the last successfully completed checkpoint or restartpoint operation.

## Parameters / Member Variables
- : Output parameter - pointer to store the redo LSN (Log Sequence Number) of the last checkpoint
- : Output parameter - pointer to store the timeline ID associated with the checkpoint

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - ControlFile (global variable)
  - LW_SHARED (lock mode constant)
- Called from (representative examples):
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) (in xlogarchive.c)
  - [ExecuteRecoveryCommand](../E/ExecuteRecoveryCommand.md) (in xlogarchive.c)

## Notes and Other Information
- This function is thread-safe as it properly acquires the ControlFileLock in shared mode
- The returned information reflects the state at the time of the last checkpoint/restartpoint
- Location: src/backend/access/transam/xlog.c:9479-9488
- Essential for WAL archival and recovery operations to determine the oldest required WAL segment