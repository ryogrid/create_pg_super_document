# CreateEndOfRecoveryRecord

## Location
src/backend/access/transam/xlog.c: 7369 - 7433

## Overview
Marks the end of WAL recovery by inserting a special end-of-recovery record without performing a full checkpoint, allowing the system to transition from recovery to normal operation.

## Definition


## Detailed Description
CreateEndOfRecoveryRecord creates a lightweight end-of-recovery marker in the WAL stream that signifies the completion of crash recovery or archive recovery. Unlike a full checkpoint, this function only writes a single WAL record (XLOG_END_OF_RECOVERY) containing essential timeline and timestamp information.

The function operates independently of ongoing restartpoint operations and is designed to be non-blocking. It captures the current timeline information while holding WAL insertion locks exclusively to ensure consistency, then updates the control file to record the minimum recovery point for future crash recovery scenarios.

This mechanism allows PostgreSQL to transition from recovery mode to normal operation more efficiently than waiting for a complete checkpoint, while still maintaining the necessary recovery guarantees.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress (recovery state check)
  - GetCurrentTimestamp (timestamp capture)
  - WALInsertLockAcquireExclusive/WALInsertLockRelease (WAL coordination)
  - XLogBeginInsert/XLogRegisterData/XLogInsert (WAL record creation)
  - XLogFlush (WAL persistence)
  - UpdateControlFile (control file updates)
  - xl_end_of_recovery (record structure)
  - XLOG_END_OF_RECOVERY (record type constant)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - PerformRecoveryXLogAction

## Notes and Other Information
- Only callable during recovery mode; validates this with RecoveryInProgress()
- Operates independently of concurrent restartpoint operations
- Updates control file's minRecoveryPoint to enable proper timeline handling in future recovery
- Uses critical sections to ensure atomicity of control file updates
- Timeline information is captured atomically under WAL insertion locks
- More lightweight alternative to full checkpoint for ending recovery
- Essential for proper point-in-time recovery and timeline switching functionality