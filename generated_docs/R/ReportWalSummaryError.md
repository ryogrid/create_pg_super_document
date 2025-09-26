# ReportWalSummaryError

## Location
[src/backend/backup/walsummary.c:322-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummary.c#L322-L346)

## Overview
An error-reporting callback function designed for use with CreateBlockRefTableReader to handle and report errors encountered during WAL summary processing.

## Definition
void ReportWalSummaryError(void *callback_arg, char *fmt, ...)

## Detailed Description
This function serves as a standardized error reporting mechanism for WAL summary operations. It accepts printf-style format strings and arguments, constructs a complete error message, and reports it through PostgreSQL's standard error reporting system with a DATA_CORRUPTED error code.

The function uses a dynamic string buffer approach to handle variable-length error messages. It employs a retry mechanism to ensure that the complete formatted message fits in the buffer, automatically expanding the buffer size if needed. This robust approach prevents truncation of important error information.

All errors reported through this function are classified as data corruption errors, which is appropriate for WAL summary parsing failures since such issues typically indicate corrupted or malformed summary files.

## Parameters / Member Variables
- callback_arg: A void pointer for callback context (currently unused but maintained for callback interface compatibility)
- fmt: A printf-style format string for the error message
- ...: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md) (PostgreSQL string utility function)
  - [appendStringInfoVA](../a/appendStringInfoVA.md) (PostgreSQL string formatting function)
  - [enlargeStringInfo](../e/enlargeStringInfo.md) (PostgreSQL string buffer expansion function)
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg_internal](../e/errmsg_internal.md) (PostgreSQL internal error message function)
  - ERRCODE_DATA_CORRUPTED (PostgreSQL error code constant)
- Called from:
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)

## Notes and Other Information
- Uses a variadic function interface to accept printf-style formatting
- Implements a robust buffer expansion strategy to handle messages of any length
- Always reports errors with ERRCODE_DATA_CORRUPTED classification
- Uses errmsg_internal rather than errmsg, indicating these are internal system errors
- The callback_arg parameter is currently unused but provides extensibility for future context-specific error handling
- This callback pattern allows the block reference table reader to report errors in a standardized way without being tightly coupled to PostgreSQL's error system