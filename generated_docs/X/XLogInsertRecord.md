# XLogInsertRecord

## Location
[src/backend/access/transam/xlog.c:750-1109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L750-L1109)

## Overview
XLogInsertRecord is the core function responsible for inserting pre-constructed XLOG records into the Write-Ahead Log (WAL), implementing the fundamental WAL insertion mechanism with proper locking and space reservation.

## Definition

```c
XLogRecPtr
XLogInsertRecord(XLogRecData *rdata,
				 XLogRecPtr fpw_lsn,
				 uint8 flags,
				 int num_fpi,
				 bool topxid_included)
```
## Detailed Description
XLogInsertRecord is a low-level routine that inserts an XLOG record represented by a chain of pre-constructed data chunks into the WAL. This function implements a sophisticated two-step process: first reserving space in the WAL buffer, then copying the record data to that reserved space. It handles three different insertion classes: normal records, XLOG_SWITCH records (which require exclusive access), and checkpoint redo records. The function includes critical safety checks for full-page writes, manages WAL insertion locks to coordinate concurrent insertions, and updates various global state variables upon successful insertion.

## Parameters / Member Variables
- : Chain of XLogRecData structures containing the record data, with the first chunk containing the record header
- : Oldest LSN among pages affected by this record that were not included as full-page images; used for full-page write validation
- : Control flags for the record insertion (see XLogSetRecordFlags for details)
- : Number of full-page images included in this record
- : Whether the top-transaction ID is logged with the current subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - [WALInsertLockAcquire](../W/WALInsertLockAcquire.md)
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)
  - [ReserveXLogInsertLocation](../R/ReserveXLogInsertLocation.md)
  - [ReserveXLogSwitch](../R/ReserveXLogSwitch.md)
  - [CopyXLogRecordToWAL](../C/CopyXLogRecordToWAL.md)
  - [XLogInsertAllowed](XLogInsertAllowed.md)
  - [MarkCurrentTransactionIdLoggedIfAny](../M/MarkCurrentTransactionIdLoggedIfAny.md)
  - [XLogFlush](XLogFlush.md)
- Called from (representative examples):
  - [XLogInsert](XLogInsert.md) (from xloginsert.c)

## Notes and Other Information
- Implements the basic WAL rule "write the log before the data" by returning an LSN that must be flushed before affected data pages can be written
- Uses a critical section to ensure atomicity of the insertion process
- Handles three insertion classes with different locking requirements: normal (single lock), switch (exclusive), and checkpoint (exclusive with RedoRecPtr update)
- Includes sophisticated full-page write logic that may cause the function to return InvalidXLogRecPtr, requiring the caller to recalculate and retry
- Updates various global variables including ProcLastRecPtr, XactLastRecEnd, and WAL usage statistics
- Contains extensive debugging support when WAL_DEBUG is enabled

## Simplified Source

```c
// Simplified version of XLogInsertRecord
XLogRecPtr XLogInsertRecord(XLogRecData *rdata,
                           XLogRecPtr fpw_lsn,
                           uint8 flags,
                           int num_fpi,
                           bool topxid_included) {
    XLogCtlInsert *Insert = &XLogCtl->Insert;
    XLogRecord *rechdr = (XLogRecord *) rdata->data;
    uint8 info = rechdr->xl_info & ~XLR_INFO_MASK;
    WalInsertClass class = WALINSERT_NORMAL;
    XLogRecPtr StartPos, EndPos;
    bool inserted;
    TimeLineID insertTLI;

    // Determine special handling requirements
    if (unlikely(rechdr->xl_rmid == RM_XLOG_ID)) {
        if (info == XLOG_SWITCH)
            class = WALINSERT_SPECIAL_SWITCH;
        else if (info == XLOG_CHECKPOINT_REDO)
            class = WALINSERT_SPECIAL_CHECKPOINT;
    }

    // Verify we're allowed to insert WAL records
    if (!XLogInsertAllowed())
        elog(ERROR, "cannot make new WAL entries during recovery");

    insertTLI = XLogCtl->InsertTimeLineID;

    START_CRIT_SECTION();

    if (likely(class == WALINSERT_NORMAL)) {
        // Normal record insertion with shared locks
        WALInsertLockAcquire();

        // Check if full-page writes became necessary
        if (RedoRecPtr != Insert->RedoRecPtr)
            RedoRecPtr = Insert->RedoRecPtr;
        doPageWrites = (Insert->fullPageWrites || Insert->runningBackups > 0);

        if (doPageWrites && fpw_lsn != InvalidXLogRecPtr && fpw_lsn <= RedoRecPtr) {
            // Need to recalculate full-page writes
            WALInsertLockRelease();
            END_CRIT_SECTION();
            return InvalidXLogRecPtr;
        }

        // Reserve space in WAL
        ReserveXLogInsertLocation(rechdr->xl_tot_len, &StartPos, &EndPos,
                                  &rechdr->xl_prev);
        inserted = true;
    } else if (class == WALINSERT_SPECIAL_SWITCH) {
        // XLOG_SWITCH requires exclusive access
        WALInsertLockAcquireExclusive();
        inserted = ReserveXLogSwitch(&StartPos, &EndPos, &rechdr->xl_prev);
    } else {
        // XLOG_CHECKPOINT_REDO requires exclusive access and RedoRecPtr update
        WALInsertLockAcquireExclusive();
        ReserveXLogInsertLocation(rechdr->xl_tot_len, &StartPos, &EndPos,
                                  &rechdr->xl_prev);
        RedoRecPtr = Insert->RedoRecPtr = StartPos;
        inserted = true;
    }

    if (inserted) {
        // Calculate final CRC and copy record to WAL
        pg_crc32c rdata_crc = rechdr->xl_crc;
        COMP_CRC32C(rdata_crc, rechdr, offsetof(XLogRecord, xl_crc));
        FIN_CRC32C(rdata_crc);
        rechdr->xl_crc = rdata_crc;

        CopyXLogRecordToWAL(rechdr->xl_tot_len,
                            class == WALINSERT_SPECIAL_SWITCH, rdata,
                            StartPos, EndPos, insertTLI);

        // Update last important record position if needed
        if ((flags & XLOG_MARK_UNIMPORTANT) == 0) {
            int lockno = holdingAllLocks ? 0 : MyLockNo;
            WALInsertLocks[lockno].l.lastImportantAt = StartPos;
        }
    }

    WALInsertLockRelease();
    END_CRIT_SECTION();

    // Update transaction tracking
    MarkCurrentTransactionIdLoggedIfAny();
    if (topxid_included)
        MarkSubxactTopXidLogged();

    // Update global write request if we crossed page boundary
    if (StartPos / XLOG_BLCKSZ != EndPos / XLOG_BLCKSZ) {
        SpinLockAcquire(&XLogCtl->info_lck);
        if (XLogCtl->LogwrtRqst.Write < EndPos)
            XLogCtl->LogwrtRqst.Write = EndPos;
        SpinLockRelease(&XLogCtl->info_lck);
        RefreshXLogWriteResult(LogwrtResult);
    }

    // Handle XLOG_SWITCH special case
    if (class == WALINSERT_SPECIAL_SWITCH) {
        XLogFlush(EndPos);
        if (inserted)
            EndPos = StartPos + SizeOfXLogRecord;
    }

    // Update global variables and statistics
    ProcLastRecPtr = StartPos;
    XactLastRecEnd = EndPos;
    if (inserted) {
        pgWalUsage.wal_bytes += rechdr->xl_tot_len;
        pgWalUsage.wal_records++;
        pgWalUsage.wal_fpi += num_fpi;
    }

    return EndPos;
}
```

Key simplifications made:
- Preserved the essential three-class insertion logic: normal, switch, checkpoint
- Maintained critical full-page write validation and retry mechanism
- Simplified locking strategy while preserving correctness
- Focused on the core two-phase algorithm: reserve space → copy data
- Removed extensive debugging and error handling details
- Emphasized the critical section protection and transaction tracking