# ApplyWalRecord

## Location
src/backend/access/transam/xlogrecovery.c: 1908 - 2071

## Overview
ApplyWalRecord is a subroutine of PerformWalRecovery that applies a single WAL record during recovery, handling timeline switches, transaction ID advancement, and various recovery-specific operations.

## Definition


## Detailed Description
ApplyWalRecord processes and applies a single WAL record during PostgreSQL recovery. The function performs several critical operations:

1. **Error Context Setup**: Establishes error handling callbacks for better error reporting during replay
2. **Transaction ID Management**: Advances the next transaction ID beyond the record's XID to maintain consistency
3. **Timeline Switch Handling**: Detects and processes timeline changes from checkpoint and end-of-recovery records
4. **Recovery State Updates**: Updates shared memory structures to track replay progress
5. **Hot Standby Processing**: Records known assigned transaction IDs when in Hot Standby mode
6. **Record Application**: Delegates actual record processing to appropriate resource managers
7. **Consistency Verification**: Performs backup page consistency checks when enabled
8. **Walsender Notification**: Wakes up physical and logical walsenders based on cascading replication settings
9. **Timeline Cleanup**: Removes obsolete WAL files when switching timelines

The function handles special XLOG records (checkpoints, end-of-recovery) differently and coordinates with various PostgreSQL subsystems during recovery.

## Parameters / Member Variables
- : XLogReaderState pointer containing the current WAL record and reading state
- : XLogRecord pointer to the specific WAL record being applied
- : TimeLineID pointer that tracks the current replay timeline and may be updated during timeline switches

## Dependencies
- Functions called/Symbols referenced:
  - [xlogrecovery_redo](../x/xlogrecovery_redo.md)
  - AdvanceNextFullTransactionIdPastXid
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md)
  - [checkTimeLineSwitch](../c/checkTimeLineSwitch.md)
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - [CheckRecoveryConsistency](../C/CheckRecoveryConsistency.md)
  - [RemoveNonParentXlogFiles](../R/RemoveNonParentXlogFiles.md)
  - [WalSndWakeup](../W/WalSndWakeup.md)
  - [WalRcvForceReply](../W/WalRcvForceReply.md)
  - [XLogPrefetchReconfigure](../X/XLogPrefetchReconfigure.md)
  - GetRmgr
- Called from:
  - [PerformWalRecovery](../P/PerformWalRecovery.md) (src/backend/access/transam/xlogrecovery.c:1822)

## Notes and Other Information
- This is a static function only called from within the xlogrecovery.c module
- The function maintains error context callbacks for detailed error reporting during recovery
- Timeline switches are detected by examining checkpoint and end-of-recovery records
- Hot Standby transaction tracking is performed when the system is in initialized standby state
- Walsender wakeup behavior differs between physical and logical replication
- The function coordinates with the WAL prefetcher to optimize I/O performance
- Backup page consistency checks are optional and controlled by the XLR_CHECK_CONSISTENCY flag
- Timeline switches trigger cleanup of potentially invalid future WAL segments