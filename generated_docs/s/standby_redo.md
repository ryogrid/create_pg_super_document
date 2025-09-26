# standby_redo

## Location
src/backend/storage/ipc/standby.c: 1159 - 1284

## Overview
standby_redo is the main WAL record processing function for standby-related operations during recovery, handling lock acquisition, running transaction snapshots, and cache invalidations.

## Definition


## Detailed Description
This function serves as the central dispatcher for processing WAL records of resource manager type RM_STANDBY_ID during hot standby recovery. It handles three main types of standby-related WAL records: XLOG_STANDBY_LOCK records for acquiring AccessExclusiveLocks, XLOG_RUNNING_XACTS records for maintaining transaction state information, and XLOG_INVALIDATIONS records for cache invalidation messages.

The function first extracts the record type information and verifies that the system is in hot standby mode before processing. For lock records, it iterates through each lock specification and acquires the corresponding AccessExclusiveLocks. For running transaction records, it reconstructs the transaction state and applies it to the process array, also taking the opportunity to report statistics. For invalidation records, it processes committed invalidation messages to maintain cache consistency.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extract record type information)
  - XLogRecHasAnyBlockRefs (verify no backup blocks)
  - XLogRecGetData (extract record data)
  - StandbyAcquireAccessExclusiveLock (acquire locks during recovery)
  - ProcArrayApplyRecoveryInfo (apply transaction state information)
  - pgstat_report_stat (report statistics)
  - ProcessCommittedInvalidationMessages (process cache invalidations)
  - elog (error logging)
- Data structures used:
  - xl_standby_locks (lock acquisition record format)
  - xl_running_xacts (running transactions record format)
  - xl_invalidations (invalidation messages record format)
  - RunningTransactionsData (transaction state structure)
- Record types handled:
  - XLOG_STANDBY_LOCK
  - XLOG_RUNNING_XACTS
  - XLOG_INVALIDATIONS
- Called from (representative examples):
  - WAL replay infrastructure (via RM_STANDBY_ID resource manager)

## Notes and Other Information
- Only processes records when standbyState is not STANDBY_DISABLED
- Asserts that standby records never contain backup blocks
- Handles transaction ID overflow scenarios in running transaction records
- Takes advantage of regular XLOG_RUNNING_XACTS records to report statistics
- Panics on unknown operation codes to ensure system integrity
- Critical component of hot standby functionality in PostgreSQL
- Located in src/backend/storage/ipc/standby.c:1159-1284