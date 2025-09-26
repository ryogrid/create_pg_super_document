# pg_ftruncate

## Location
[src/backend/storage/file/fd.c:700-716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L700-L716)

## Overview
A PostgreSQL static wrapper function around the system ftruncate() call that handles EINTR interrupts with automatic retry logic.

## Definition
```c
static int pg_ftruncate(int fd, off_t length)
```

## Detailed Description
The pg_ftruncate function provides a reliable interface to truncate an open file to a specified length by wrapping the system ftruncate() call with interrupt handling. The function automatically retries the operation if it is interrupted by a signal (EINTR), ensuring that the truncation operation completes successfully even in the presence of signal interruptions. This is a static helper function used internally within PostgreSQL's file descriptor management system.

## Parameters / Member Variables
- `fd`: File descriptor of the open file to truncate
- `length`: The desired length to truncate the file to (in bytes)

## Dependencies
- Functions called/Symbols referenced:
  - ftruncate (system call)
  - EINTR (errno constant)
- Called from (representative examples):
  - pg_truncate
  - FileTruncate

## Notes and Other Information
- Returns 0 on success, -1 on error with errno set appropriately
- Static function scope - only accessible within fd.c
- Implements automatic retry on EINTR signal interruption
- Does not perform additional error checking beyond the system call
- Part of PostgreSQL's internal file management utilities
- Used as a building block for higher-level file truncation operations