# SlruReportIOError

## Location
[src/backend/access/transam/slru.c:1045-1119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1045-L1119)

## Overview
Reports I/O errors that occur during SLRU (Simple Least Recently Used) page operations, providing detailed error messages for different types of file access failures.

## Definition


## Detailed Description
SlruReportIOError is a static internal function that handles error reporting after SLRU physical page I/O operations fail. It constructs detailed error messages based on the specific type of I/O failure that occurred, including file operations like open, seek, read, write, fsync, and close. The function calculates the segment number and page offset from the given page number, constructs the file path, and reports an appropriate error message using PostgreSQL's error reporting system.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing configuration and state information for the SLRU cache
- `pageno`: Logical page number that experienced the I/O error
- `xid`: Transaction ID associated with the failed operation (used in error messages)

## Dependencies
- Functions called/Symbols referenced:
  - [SlruFileName](SlruFileName.md)
  - ereport/elog (error reporting functions)
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - data_sync_elevel
- Constants used:
  - SLRU_PAGES_PER_SEGMENT
  - SLRU_OPEN_FAILED, SLRU_SEEK_FAILED, SLRU_READ_FAILED, SLRU_WRITE_FAILED, SLRU_FSYNC_FAILED, SLRU_CLOSE_FAILED
- Called from:
  - [SimpleLruReadPage](SimpleLruReadPage.md)
  - [SlruInternalWritePage](SlruInternalWritePage.md)
  - [SimpleLruDoesPhysicalPageExist](SimpleLruDoesPhysicalPageExist.md)
  - [SimpleLruWriteAll](SimpleLruWriteAll.md)

## Notes and Other Information
- This function relies on global variables `slru_errno` and `slru_errcause` to determine the specific error type and system errno value
- Different error cases produce different error message formats, with some including system error details (%m)
- For fsync failures, it uses `data_sync_elevel(ERROR)` which may adjust the error level based on configuration
- The function is designed to be called after cleaning up shared-memory state following an I/O failure
- File path construction uses segment-based naming convention where each segment contains SLRU_PAGES_PER_SEGMENT pages