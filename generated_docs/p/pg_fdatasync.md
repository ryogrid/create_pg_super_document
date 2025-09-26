# pg_fdatasync

## Location
src/backend/storage/file/fd.c: 477 - 499

## Overview
A PostgreSQL wrapper around the system fdatasync() call that respects the global enableFsync setting and handles EINTR interrupts gracefully.

## Definition

```c
struct stat st;
```
## Detailed Description
The pg_fdatasync function provides a controlled interface to synchronize data for a file descriptor to storage. Unlike pg_fsync which synchronizes both data and metadata, pg_fdatasync only synchronizes the data portion, which can be more efficient when metadata updates are not critical. The function incorporates PostgreSQL's fsync control mechanism - if enableFsync is disabled (typically for testing or specific configurations), the function returns immediately without performing any sync operation. It also implements retry logic to handle EINTR signals that may interrupt the fdatasync system call.

## Parameters / Member Variables
- `fd`: File descriptor to synchronize data for

## Dependencies
- Functions called/Symbols referenced:
  - fdatasync (system call)
  - EINTR (errno constant)
- Called from (representative examples):
  - issue_xlog_fsync
  - PG_O_DIRECT (referenced in fd.h)

## Notes and Other Information
- Returns 0 on success or if enableFsync is disabled
- Returns -1 on error with errno set appropriately
- Automatically retries on EINTR interruption
- Part of PostgreSQL's file descriptor management system in fd.c
- More efficient than pg_fsync when only data synchronization is needed