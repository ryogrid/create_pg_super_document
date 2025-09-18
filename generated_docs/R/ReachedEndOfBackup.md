# ReachedEndOfBackup

## Location
src/backend/access/transam/xlog.c: 6226 - 6262

## Overview
ReachedEndOfBackup is called when WAL recovery reaches the end of a base backup, updating the control file to reflect that the database has reached a consistent state.

## Definition


## Detailed Description
ReachedEndOfBackup is a callback function invoked by PerformWalRecovery() when the recovery process encounters the end-of-backup WAL record. This occurs during recovery from a base backup when the system replays WAL records up to the point where the backup was completed and the database reached a consistent state.

The function performs several important control file updates:
1. **Consistency Point Update**: Updates minRecoveryPoint to ensure the database won't start at an earlier, potentially inconsistent point
2. **Backup State Cleanup**: Resets backup-related control file fields (backupStartPoint, backupEndPoint, backupEndRequired) to indicate the backup recovery process is complete  
3. **Control File Persistence**: Writes the updated control file to disk to make the changes durable

This function is critical for ensuring that recovery from base backups completes properly and that the database cannot be started in an inconsistent state if recovery is interrupted and restarted.

## Parameters / Member Variables
- : The WAL record pointer marking the end of the backup, used to update the minimum recovery point
- : The timeline ID associated with the end-of-backup record, stored as the minimum recovery point timeline

## Dependencies
- Functions called/Symbols referenced:
  - UpdateControlFile (persists control file changes to disk)
  - InvalidXLogRecPtr (constant used to reset backup start/end points)
- Called from (representative examples):
  - [CheckRecoveryConsistency](../C/CheckRecoveryConsistency.md) (when processing end-of-backup records during recovery)

## Notes and Other Information
- Called as a callback during WAL recovery when processing end-of-backup records
- Acquires ControlFileLock exclusively to ensure atomic updates to control file
- Ensures minRecoveryPoint never moves backward, only forward or stays the same
- Resets all backup-related fields in the control file once backup recovery is complete
- Critical for base backup recovery consistency - prevents starting at inconsistent points
- The function handles the case where minRecoveryPoint might already be ahead (from previous recovery attempts)
- Located in src/backend/access/transam/xlog.c:6226-6262