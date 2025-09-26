# FileZero

## Location
[src/backend/storage/file/fd.c:2321-2365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2321-L2365)

## Overview
FileZero fills a specified region of a file with zero bytes, providing efficient zeroing functionality for file operations.

## Definition
```c
int FileZero(File file, off_t offset, off_t amount, uint32 wait_event_info)
```

## Detailed Description
FileZero efficiently zeros out a contiguous region of a file by writing zero bytes to the specified offset and amount. The function validates the file descriptor, ensures file accessibility, and uses PostgreSQL's optimized zero-writing function with proper wait event reporting. This operation is commonly used for file extension, initialization of new file regions, and ensuring clean data areas. The function handles partial writes and sets appropriate error codes when disk space issues occur.

## Parameters / Member Variables
- `file`: Virtual file descriptor representing the target file
- `offset`: Starting byte position in the file where zeroing should begin
- `amount`: Number of bytes to zero from the offset position
- `wait_event_info`: Event information used for wait event reporting during the operation

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the virtual file descriptor
  - FileAccess: Ensures the file is accessible and handles VFD management
  - pgstat_report_wait_start: Reports the start of a wait event for monitoring
  - pg_pwrite_zeros: PostgreSQL's optimized function for writing zeros at a specific offset
  - pgstat_report_wait_end: Reports the end of the wait event
  - DO_DB: Debug logging macro
  - INT64_FORMAT: Macro for formatting 64-bit integers in log messages
- Called from (representative examples):
  - FileFallocate: Used as a fallback when fallocate() is not available
  - mdzeroextend: During zero-extension of MD storage files

## Notes and Other Information
- Returns 0 on success, -1 on failure with errno set appropriately
- Handles partial writes by checking if the actual written amount matches the requested amount
- Sets errno to ENOSPC (no space left on device) when partial writes occur without other error indicators
- Includes comprehensive debug logging showing file details, offset, and amount
- Part of PostgreSQL's Virtual File Descriptor (VFD) system
- Critical for maintaining data integrity by ensuring clean initialization of file regions
- Wait event reporting enables monitoring of potentially long-running zero operations