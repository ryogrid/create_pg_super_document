# XlogReadTwoPhaseData

## Location
[src/backend/access/transam/twophase.c:1404-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1404-L1458)

## Overview
XlogReadTwoPhaseData reads two-phase commit state data directly from the Write-Ahead Log (WAL) at a specified LSN position.

## Definition

```c
static void
XlogReadTwoPhaseData(XLogRecPtr lsn, char **buf, int *len)
```
## Detailed Description
XlogReadTwoPhaseData provides the capability to retrieve two-phase commit state data directly from WAL records, which is used as an alternative to reading from disk files during certain recovery scenarios. It allocates and configures an XLog reader, seeks to the specified LSN, reads and validates the record to ensure it's a PREPARE record, then copies the data payload into a newly allocated buffer. This function is critical for checkpoint operations that move 2PC data from WAL to persistent files and for recovery operations that need to access prepare state before it's been written to disk.

## Parameters / Member Variables
- `lsn`: XLogRecPtr specifying the WAL position where the two-phase state data is located
- `**buf`: char** output parameter that receives a pointer to the palloc'd buffer containing the state data
- `*len`: int* optional output parameter that receives the length of the data (can be NULL if length is not needed)
## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](XLogReaderAllocate.md)
  - [XLogBeginRead](XLogBeginRead.md)
  - [XLogReadRecord](XLogReadRecord.md)
  - XLogRecGetRmid
  - XLogRecGetInfo
  - XLogRecGetDataLen
  - XLogRecGetData
  - [XLogReaderFree](XLogReaderFree.md)
  - [read_local_xlog_page](../r/read_local_xlog_page.md)
  - [wal_segment_open](../w/wal_segment_open.md)
  - [wal_segment_close](../w/wal_segment_close.md)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [CheckPointTwoPhase](../C/CheckPointTwoPhase.md)
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [LookupGXact](../L/LookupGXact.md)

## Notes and Other Information
- Static function (internal to twophase.c module)
- Can access WAL during normal operation, similar to WALSender or Logical Decoding
- Used during checkpoint operations to move 2PC data from WAL to twophase files
- Validates that the record at the specified LSN is indeed a XLOG_XACT_PREPARE record
- Allocates memory for the returned buffer using palloc - caller must free
- Provides detailed error messages with LSN information for debugging
- Critical for recovery scenarios where prepare state hasn't been written to disk yet
- Uses XL_ROUTINE macro for setting up WAL reading callbacks
- Part of the WAL-based two-phase commit state management system

## Simplified Source

```c
// Simplified version of XlogReadTwoPhaseData
static void
XlogReadTwoPhaseData(XLogRecPtr lsn, char **buf, int *len)
{
    XLogRecord *record;
    XLogReaderState *xlogreader;
    char *errormsg;

    // Step 1: Allocate WAL reader with necessary callbacks
    xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
                                   XL_ROUTINE(.page_read = &read_local_xlog_page,
                                             .segment_open = &wal_segment_open,
                                             .segment_close = &wal_segment_close),
                                   NULL);
    if (!xlogreader)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("Failed while allocating a WAL reading processor")));

    // Step 2: Position reader at target LSN and read the record
    XLogBeginRead(xlogreader, lsn);
    record = XLogReadRecord(xlogreader, &errormsg);

    if (record == NULL)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not read two-phase state from WAL at %X/%X",
                             LSN_FORMAT_ARGS(lsn))));

    // Step 3: Validate this is a two-phase prepare record
    if (XLogRecGetRmid(xlogreader) != RM_XACT_ID ||
        (XLogRecGetInfo(xlogreader) & XLOG_XACT_OPMASK) != XLOG_XACT_PREPARE)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("expected two-phase state data is not present in WAL")));

    // Step 4: Extract data length and copy to output buffer
    if (len != NULL)
        *len = XLogRecGetDataLen(xlogreader);

    *buf = palloc(sizeof(char) * XLogRecGetDataLen(xlogreader));
    memcpy(*buf, XLogRecGetData(xlogreader), sizeof(char) * XLogRecGetDataLen(xlogreader));

    // Step 5: Clean up resources
    XLogReaderFree(xlogreader);
}
```

Key simplifications made:
- Consolidated error handling branches for readability
- Removed detailed error message variations while preserving essential error reporting
- Added step-by-step comments to clarify the logical flow
- Maintained all core functionality: allocation, reading, validation, data extraction, cleanup
- Preserved critical validation checks for record type and resource management
- Simplified error messages to focus on the essential information