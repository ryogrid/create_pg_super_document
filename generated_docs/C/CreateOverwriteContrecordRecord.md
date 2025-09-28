# CreateOverwriteContrecordRecord

## Location
[src/backend/access/transam/xlog.c:7434-7503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L7434-L7503)

## Overview
Creates a special WAL record to handle the case where a continuation record was missing during recovery, preventing rewind issues for downstream consumers like physical replicas.

## Definition

```c
static XLogRecPtr
CreateOverwriteContrecordRecord(XLogRecPtr aborted_lsn, XLogRecPtr pagePtr,
								TimeLineID newTLI)
```
## Detailed Description
CreateOverwriteContrecordRecord addresses a critical edge case in WAL recovery where a continuation record expected at the start of a WAL page is missing or corrupted. When this occurs, recovery must end and normal WAL writing must resume, but simply resuming at the start of the broken record would cause problems for downstream consumers (physical replicas) that are not prepared to handle rewind scenarios.

The function writes an XLOG_OVERWRITE_CONTRECORD at the position where the missing continuation record should have been, and marks the page header with XLP_FIRST_IS_OVERWRITE_CONTRECORD flag. This special marker allows xlogreader to detect and handle this situation appropriately during subsequent recovery operations.

The function operates under strict constraints: it can only be called at the end of recovery, when no other backends are writing WAL, and the insert position must be precisely at the start of the problematic page after its header.

## Parameters / Member Variables
- `aborted_lsn`: The LSN of the beginning of the incomplete record that was being read
- `pagePtr`: The starting position of the WAL page where the overwrite record will be inserted
- `newTLI`: The timeline ID for the new record insertion

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (recovery state validation)
  - [GetXLogInsertRecPtr](../G/GetXLogInsertRecPtr.md) (current WAL position)
  - [WALInsertLockAcquire](../W/WALInsertLockAcquire.md)/WALInsertLockRelease (WAL coordination)
  - [GetXLogBuffer](../G/GetXLogBuffer.md) (WAL page buffer access)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterData/XLogInsert (WAL record creation)
  - [XLogFlush](../X/XLogFlush.md) (WAL persistence)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (timestamp capture)
  - [xl_overwrite_contrecord](../x/xl_overwrite_contrecord.md) (record structure)
  - XLP_FIRST_IS_OVERWRITE_CONTRECORD (page header flag)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- Only callable during recovery mode and validates this constraint strictly
- Requires precise positioning at page boundaries with proper alignment checks
- Sets special page header flag (XLP_FIRST_IS_OVERWRITE_CONTRECORD) for xlogreader detection
- Prevents timeline rewind issues that would break physical replication
- Operates in single-threaded context where no concurrent WAL insertion occurs
- Critical for maintaining WAL stream consistency across recovery boundaries
- The record includes both the aborted LSN and timestamp for forensic purposes
- Essential for proper handling of WAL corruption or incomplete writes during recovery

## Simplified Source

```c
// Simplified version of CreateOverwriteContrecordRecord
static XLogRecPtr
CreateOverwriteContrecordRecord(XLogRecPtr aborted_lsn, XLogRecPtr pagePtr, TimeLineID newTLI) {
    xl_overwrite_contrecord xlrec;
    XLogRecPtr recptr;
    XLogPageHeader pagehdr;
    XLogRecPtr startPos;

    // Core validation: Only allowed during recovery
    if (!RecoveryInProgress())
        elog(ERROR, "can only be used at end of recovery");

    // Validate page alignment
    if (pagePtr % XLOG_BLCKSZ != 0)
        elog(ERROR, "invalid position for missing continuation record");

    // Calculate expected start position after page header
    startPos = pagePtr;
    if (XLogSegmentOffset(startPos, wal_segment_size) == 0)
        startPos += SizeOfXLogLongPHD;  // Long header at segment start
    else
        startPos += SizeOfXLogShortPHD; // Short header mid-segment

    // Verify current insert position matches expected position
    recptr = GetXLogInsertRecPtr();
    if (recptr != startPos)
        elog(ERROR, "invalid WAL insert position for OVERWRITE_CONTRECORD");

    START_CRIT_SECTION();

    // Step 1: Mark page header with special overwrite flag
    WALInsertLockAcquire();
    pagehdr = (XLogPageHeader) GetXLogBuffer(pagePtr, newTLI);
    pagehdr->xlp_info |= XLP_FIRST_IS_OVERWRITE_CONTRECORD;
    WALInsertLockRelease();

    // Step 2: Create and insert the overwrite record
    XLogBeginInsert();
    xlrec.overwritten_lsn = aborted_lsn;
    xlrec.overwrite_time = GetCurrentTimestamp();
    XLogRegisterData((char *) &xlrec, sizeof(xl_overwrite_contrecord));
    recptr = XLogInsert(RM_XLOG_ID, XLOG_OVERWRITE_CONTRECORD);

    // Verify record was inserted at expected position
    if (ProcLastRecPtr != startPos)
        elog(ERROR, "OVERWRITE_CONTRECORD inserted to unexpected position");

    // Step 3: Ensure record is durably written
    XLogFlush(recptr);

    END_CRIT_SECTION();

    return recptr;
}
```

Key simplifications made:
- Removed detailed LSN formatting from error messages for clarity
- Consolidated position validation logic with clear comments
- Simplified critical section flow into three main steps
- Added descriptive comments explaining header size selection logic
- Abstracted complex error message formatting while preserving error conditions
- Maintained all essential validation and safety checks
- Preserved the core algorithm: validate → mark page → insert record → flush