# FileFallocate

## Location
[src/backend/storage/file/fd.c:2366-2405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2366-L2405)

## Overview
FileFallocate attempts to reserve file space using the POSIX fallocate system call, falling back to zero-filling if fallocate is unavailable or unsupported.

## Definition
```c
int FileFallocate(File file, off_t offset, off_t amount, uint32 wait_event_info)
```

## Detailed Description
FileFallocate provides efficient space allocation for files by attempting to use the POSIX posix_fallocate() system call when available. This function reserves disk space without actually writing data, which is more efficient than zero-filling for large allocations. When posix_fallocate() is not implemented by the operating system or fails with EINVAL/EOPNOTSUPP errors, the function gracefully falls back to using FileZero() to achieve the same result through explicit zero-writing. The function includes proper wait event reporting and handles interruption signals by retrying the operation.

## Parameters / Member Variables
- `file`: Virtual file descriptor representing the target file
- `offset`: Starting byte position in the file where space allocation should begin
- `amount`: Number of bytes to allocate from the offset position
- `wait_event_info`: Event information used for wait event reporting during the operation

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the virtual file descriptor
  - [FileAccess](FileAccess.md): Ensures the file is accessible and handles VFD management
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md): Reports the start of a wait event for monitoring
  - posix_fallocate: POSIX system call for efficient space allocation (when available)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md): Reports the end of the wait event
  - [FileZero](FileZero.md): Fallback function for zero-filling when fallocate is unavailable
  - DO_DB: Debug logging macro
  - INT64_FORMAT: Macro for formatting 64-bit integers in log messages
- Called from (representative examples):
  - [mdzeroextend](../m/mdzeroextend.md): During zero-extension of MD storage files to optimize space allocation

## Notes and Other Information
- Returns 0 on success, -1 on failure with errno set appropriately
- Conditionally compiled based on HAVE_POSIX_FALLOCATE availability
- Handles EINTR (interrupted system call) by retrying the operation
- Falls back to FileZero() when posix_fallocate() returns EINVAL or EOPNOTSUPP
- Preserves errno for compatibility with PostgreSQL's error reporting mechanisms (%m printing)
- More efficient than FileZero() for large allocations when posix_fallocate() is supported
- Part of PostgreSQL's optimization strategy for file space management
- Includes comprehensive debug logging for troubleshooting space allocation issues
- Critical for performance in scenarios involving large file extensions or pre-allocation