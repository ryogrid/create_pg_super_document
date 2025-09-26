# pg_fsync_no_writethrough

## Location
[src/backend/storage/file/fd.c:438-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L438-L457)

## Overview
PostgreSQL's standard fsync implementation that performs file synchronization without writethrough mode, respecting the enableFsync configuration setting.

## Definition
int pg_fsync_no_writethrough(int fd)

## Detailed Description
pg_fsync_no_writethrough provides PostgreSQL's default file synchronization behavior without writethrough semantics. The function acts as a wrapper around the standard fsync() system call with two key enhancements: it respects PostgreSQL's enableFsync configuration setting (allowing fsync to be disabled for testing or development), and it automatically retries the fsync operation if interrupted by a signal (EINTR).

When enableFsync is disabled, the function immediately returns 0 without performing any actual synchronization, which is useful for testing scenarios where durability guarantees are not required. When enabled, it calls the system fsync() and handles signal interruptions gracefully by retrying the operation.

## Parameters / Member Variables
- fd: The file descriptor to synchronize to persistent storage

## Dependencies
- Functions called/Symbols referenced:
  - fsync (system call for file synchronization)
  - enableFsync (global configuration variable)
  - EINTR (signal interruption errno value)
- Called from (representative examples):
  - pg_fsync (as the default synchronization method)
  - issue_xlog_fsync
  - PG_O_DIRECT (header reference)

## Notes and Other Information
- This is the default synchronization method used by pg_fsync when writethrough mode is not configured
- The function handles signal interruptions (EINTR) by automatically retrying the fsync operation
- Can be completely disabled via the enableFsync configuration setting, useful for testing and development
- Does not provide writethrough semantics, which may allow disk drive caches to delay actual writes to platters
- Part of PostgreSQL's configurable durability system that balances performance and data safety requirements