# pg_fsync_writethrough

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:615-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L615-L629)

## Overview
A PostgreSQL storage layer function that performs write-through filesystem synchronization using platform-specific full fsync operations when available.

## Definition


## Detailed Description
The pg_fsync_writethrough function provides write-through filesystem synchronization functionality in PostgreSQL. It attempts to ensure that data is not only written to the OS cache but is actually persisted to physical storage media. On platforms that support F_FULLFSYNC (primarily macOS), it uses fcntl with F_FULLFSYNC to perform a complete synchronization that bypasses OS-level caching. On platforms without this capability, it returns an error with ENOSYS. The function respects the global enableFsync setting and returns immediately if fsync operations are disabled.

## Parameters / Member Variables
- : File descriptor of the file to synchronize

## Dependencies
- Functions called/Symbols referenced:
  - fcntl (system call with F_FULLFSYNC when available)
  - enableFsync (global variable controlling fsync behavior)
- Called from (representative examples):
  - [issue_xlog_fsync](../i/issue_xlog_fsync.md) (WAL synchronization)
  - pg_fsync (general fsync wrapper)
  - [test_sync](../t/test_sync.md) (pg_test_fsync utility)
  - STOP_TIMER macro (performance testing)

## Notes and Other Information
- Only available on platforms with F_FULLFSYNC support (mainly macOS)
- Returns 0 on success, -1 on failure
- Sets errno to ENOSYS on unsupported platforms
- Part of PostgreSQL's abstraction layer for different fsync behaviors
- More aggressive than regular fsync as it bypasses OS-level write caching
- Used primarily for critical data integrity operations like WAL synchronization