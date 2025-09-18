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