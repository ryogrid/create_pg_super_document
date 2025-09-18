# SwitchIntoArchiveRecovery

## Location
src/backend/access/transam/xlog.c: 6188 - 6225

## Overview
SwitchIntoArchiveRecovery transitions the database system from crash recovery mode to archive recovery mode, updating control file state and recovery parameters accordingly.

## Definition


## Detailed Description
SwitchIntoArchiveRecovery is a callback function invoked by PerformWalRecovery() when the recovery process needs to transition from crash recovery to archive recovery mode. This typically occurs when the system runs out of WAL files in the pg_wal directory and needs to retrieve additional WAL files from the archive to continue recovery.

The function performs several critical state updates:
1. Changes the control file state from crash recovery to archive recovery mode
2. Updates the minimum recovery point to ensure consistency requirements are met
3. Synchronizes both the persistent control file and shared memory recovery state
4. Enables the startup process to update its local minimum recovery point tracking

This transition is essential for ensuring that archive recovery can proceed safely while maintaining data consistency guarantees.

## Parameters / Member Variables
- : The WAL record pointer where the transition to archive recovery occurs, used to update the minimum recovery point
- : The timeline ID being replayed at the transition point, stored as the minimum recovery point timeline

## Dependencies
- Functions called/Symbols referenced:
  - UpdateControlFile (persists control file changes to disk)
  - DB_IN_ARCHIVE_RECOVERY (control file state constant)
  - RECOVERY_STATE_ARCHIVE (shared memory recovery state)
- Called from (representative examples):
  - [ReadRecord](../R/ReadRecord.md) (when switching recovery modes during WAL reading)

## Notes and Other Information
- Called as a callback during WAL recovery when transitioning between recovery modes
- Acquires ControlFileLock exclusively to ensure atomic updates to control file state
- Updates both persistent control file and volatile shared memory state consistently
- Sets updateMinRecoveryPoint flag to allow startup process local tracking updates
- Critical for maintaining recovery consistency when switching from local WAL replay to archived WAL retrieval
- The function ensures that minRecoveryPoint advances monotonically and is never set to a position earlier than already achieved
- Located in src/backend/access/transam/xlog.c:6188-6225