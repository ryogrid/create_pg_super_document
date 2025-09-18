# xlogrecovery_redo

## Location
src/backend/access/transam/xlogrecovery.c: 2072 - 2142

## Overview
xlogrecovery_redo handles specific XLOG resource manager record types that are directly related to WAL recovery operations, including overwrite contrecord and backup end records.

## Definition


## Detailed Description
xlogrecovery_redo is a specialized function that processes XLOG resource manager records that require special handling during WAL recovery. Unlike regular WAL records that are processed by their respective resource managers, these records are handled directly in the recovery subsystem because they relate to recovery control operations.

The function handles two specific record types:

1. **XLOG_OVERWRITE_CONTRECORD**: Verifies and processes records that indicate a continuation record was overwritten during crash recovery. This ensures that skipped records are properly validated and recovery state is correctly updated.

2. **XLOG_BACKUP_END**: Processes the end of a base backup, which marks the point where pg_backup_stop() was executed. This record helps establish data consistency for backup recovery scenarios.

The function performs validation of record payloads and updates global recovery state variables accordingly. It also provides debug logging for backup-related operations.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being processed and associated reading state
- : TimeLineID representing the current replay timeline (passed but not directly used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetRmid
  - XLogRecGetData
  - timestamptz_to_str
  - elog
- Called from:
  - ApplyWalRecord (src/backend/access/transam/xlogrecovery.c:1988)

## Notes and Other Information
- This is a static function only called from within the xlogrecovery.c module
- The function only processes XLOG resource manager records (RM_XLOG_ID)
- XLOG_OVERWRITE_CONTRECORD handling helps recover from incomplete WAL writes during crashes
- XLOG_BACKUP_END processing is crucial for point-in-time recovery from backups
- The function updates global variables like abortedRecPtr, missingContrecPtr, and backupEndPoint
- Debug messages are logged for backup-related operations to aid in recovery monitoring
- Record validation includes checking overwritten LSN consistency for overwrite contrecord types
- The replayTLI parameter is currently unused but maintains interface consistency