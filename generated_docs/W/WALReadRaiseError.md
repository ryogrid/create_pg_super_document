# WALReadRaiseError

## Location
[src/backend/access/transam/xlogutils.c:1020-1043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L1020-L1043)

## Overview
Backend-specific error handling function that converts WAL read failures into PostgreSQL ERROR messages with appropriate error codes and context information.

## Definition
void WALReadRaiseError(WALReadError *errinfo)

## Detailed Description
This utility function serves as a centralized error handler for WAL reading operations performed by WALRead(). It examines the error information structure and raises appropriate PostgreSQL ERROR conditions with detailed diagnostic messages. The function handles two primary error scenarios: system-level read failures (negative read values indicating system errors) and incomplete reads (zero bytes read when data was expected).

The function constructs meaningful error messages that include the WAL segment filename, offset position, and specific failure details to aid in debugging and troubleshooting. For system errors, it preserves the original errno and uses errcode_for_file_access() to generate appropriate error codes. For data corruption scenarios (incomplete reads), it uses ERRCODE_DATA_CORRUPTED.

## Parameters / Member Variables
- : WALReadError structure containing detailed information about the read failure, including segment details, requested/actual read amounts, errno values, and offset information

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFileName](../X/XLogFileName.md)
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [read_local_xlog_page_guts](../r/read_local_xlog_page_guts.md)
  - [summarizer_read_local_xlog_page](../s/summarizer_read_local_xlog_page.md)
  - [logical_read_xlog_page](../l/logical_read_xlog_page.md)
  - [XLogSendPhysical](../X/XLogSendPhysical.md)

## Notes and Other Information
- This function always raises an ERROR, meaning it never returns normally to the caller
- The function constructs WAL segment filenames using XLogFileName for error reporting
- Distinguishes between two error types: system read errors (wre_read < 0) and incomplete reads (wre_read == 0)
- System errors preserve the original errno and use file access error codes
- Incomplete read errors are treated as data corruption and use ERRCODE_DATA_CORRUPTED
- Error messages include precise offset information and read/request byte counts for debugging
- File location: src/backend/access/transam/xlogutils.c:1020-1043

## Simplified Source

```c
// Simplified version of WALReadRaiseError
void WALReadRaiseError(WALReadError *errinfo) {
    WALOpenSegment *seg = &errinfo->wre_seg;
    char fname[MAXFNAMELEN];

    // Generate WAL segment filename for error message
    XLogFileName(fname, seg->ws_tli, seg->ws_segno, wal_segment_size);

    if (errinfo->wre_read < 0) {
        // System read error - preserve errno
        errno = errinfo->wre_errno;
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read from WAL segment %s, offset %d: %m",
                        fname, errinfo->wre_off)));
    }
    else if (errinfo->wre_read == 0) {
        // Incomplete read - data corruption
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("could not read from WAL segment %s, offset %d: read %d of %d",
                        fname, errinfo->wre_off, errinfo->wre_read,
                        errinfo->wre_req)));
    }
}
```

Key simplifications made:
- Function is already well-structured for error handling
- Distinguishes between system errors and data corruption
- Provides detailed error context including filename, offset, and byte counts
- Always raises ERROR, never returns normally