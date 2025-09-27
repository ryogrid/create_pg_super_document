# LogCurrentRunningXacts

## Location
[src/backend/storage/ipc/standby.c:1345-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1345-L1404)

## Overview
Records an enhanced snapshot of currently running transactions into the Write-Ahead Log (WAL) for standby server replication purposes.

## Definition

```c
static XLogRecPtr
LogCurrentRunningXacts(RunningTransactions CurrRunningXacts)
```
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
  - [LogStandbySnapshot](LogStandbySnapshot.md)

## Notes and Other Information
- The function uses the xl_running_xacts structure which is similar to but separate from RunningTransactionsData to maintain a contiguous memory layout for WAL records
- Records are marked with XLOG_MARK_UNIMPORTANT to prevent unnecessary archival activity
- Debug logging provides detailed information about the snapshot including transaction counts and overflow status
- The function handles subtransaction overflow cases where not all subtransaction IDs can be included in the array
- Located in src/backend/storage/ipc/standby.c:1345-1404

## Simplified Source

```c
// Simplified version of LogCurrentRunningXacts
static XLogRecPtr LogCurrentRunningXacts(RunningTransactions CurrRunningXacts) {
    xl_running_xacts xlrec;
    XLogRecPtr recptr;

    // Copy transaction snapshot data to WAL record structure
    xlrec.xcnt = CurrRunningXacts->xcnt;
    xlrec.subxcnt = CurrRunningXacts->subxcnt;
    xlrec.subxid_overflow = (CurrRunningXacts->subxid_status != SUBXIDS_IN_ARRAY);
    xlrec.nextXid = CurrRunningXacts->nextXid;
    xlrec.oldestRunningXid = CurrRunningXacts->oldestRunningXid;
    xlrec.latestCompletedXid = CurrRunningXacts->latestCompletedXid;

    // Prepare WAL record - mark as unimportant for durability
    XLogBeginInsert();
    XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);

    // Register header data
    XLogRegisterData((char *) (&xlrec), MinSizeOfXactRunningXacts);

    // Register transaction ID array if present
    if (xlrec.xcnt > 0) {
        XLogRegisterData((char *) CurrRunningXacts->xids,
                        (xlrec.xcnt + xlrec.subxcnt) * sizeof(TransactionId));
    }

    // Insert WAL record
    recptr = XLogInsert(RM_STANDBY_ID, XLOG_RUNNING_XACTS);

    // Log debug information about the snapshot
    if (xlrec.subxid_overflow) {
        elog(DEBUG2, "snapshot of %d running transactions overflowed",
             CurrRunningXacts->xcnt);
    } else {
        elog(DEBUG2, "snapshot of %d+%d running transaction ids",
             CurrRunningXacts->xcnt, CurrRunningXacts->subxcnt);
    }

    // Schedule asynchronous WAL sync
    XLogSetAsyncXactLSN(recptr);

    return recptr;
}
```

Key simplifications made:
- Removed detailed debug message formatting for clarity
- Condensed the debug logging logic while preserving the overflow vs normal case distinction
- Maintained the essential WAL logging workflow
- Preserved all critical transaction snapshot data copying
- Kept the important XLOG_MARK_UNIMPORTANT flag and async sync behavior