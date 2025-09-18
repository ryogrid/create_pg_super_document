# LogCurrentRunningXacts

## Location
src/backend/storage/ipc/standby.c: 1345 - 1404

## Overview
Records an enhanced snapshot of currently running transactions into the Write-Ahead Log (WAL) for standby server replication purposes.

## Definition


## Detailed Description
LogCurrentRunningXacts creates a WAL record containing a snapshot of all currently running transactions. This information is crucial for standby servers to maintain consistent transaction visibility during Hot Standby operations. The function converts a RunningTransactions data structure into an xl_running_xacts WAL record format and logs it with the XLOG_RUNNING_XACTS record type.

The logged snapshot includes transaction counts, subtransaction information, overflow status, and key transaction IDs like the next XID to be assigned, oldest running XID, and latest completed XID. The records are marked as unimportant for durability to avoid triggering unnecessary checkpoint or archiving activity, as they are used primarily for replication consistency rather than crash recovery.

The function ensures the WAL record is eventually synced to disk by using XLogSetAsyncXactLSN(), which marks the LSN for background flushing without blocking the current operation.

## Parameters / Member Variables
- : A RunningTransactions structure containing the current snapshot of running transactions, including transaction counts, subtransaction information, and key transaction IDs

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogSetAsyncXactLSN](../X/XLogSetAsyncXactLSN.md)
  - XLOG_MARK_UNIMPORTANT
  - XLOG_RUNNING_XACTS
  - MinSizeOfXactRunningXacts
  - SUBXIDS_IN_ARRAY
- Called from (representative examples):
  - LogStandbySnapshot

## Notes and Other Information
- The function uses the xl_running_xacts structure which is similar to but separate from RunningTransactionsData to maintain a contiguous memory layout for WAL records
- Records are marked with XLOG_MARK_UNIMPORTANT to prevent unnecessary archival activity
- Debug logging provides detailed information about the snapshot including transaction counts and overflow status
- The function handles subtransaction overflow cases where not all subtransaction IDs can be included in the array
- Located in src/backend/storage/ipc/standby.c:1345-1404