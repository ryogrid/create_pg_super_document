# pg_fsync

## Location
src/backend/storage/file/fd.c: 386 - 437

## Overview
PostgreSQL's main fsync wrapper function that performs file synchronization with or without writethrough mode based on configuration.

## Definition
int pg_fsync(int fd)

## Detailed Description
pg_fsync is PostgreSQL's central file synchronization function that ensures data is written to persistent storage. The function acts as a dispatcher that chooses between writethrough and non-writethrough fsync modes based on the wal_sync_method configuration setting. 

In debug builds, the function includes extensive validation to ensure file descriptors have appropriate access modes for fsync operations - files must be opened with write permissions (not O_RDONLY) while directories must be opened with O_RDONLY. This validation helps catch portability issues across different operating systems that have varying requirements for fsync().

The function returns the result of the underlying fsync operation, which is 0 on success or -1 on failure (with errno set appropriately).

## Parameters / Member Variables
- fd: The file descriptor to synchronize to persistent storage

## Dependencies
- Functions called/Symbols referenced:
  - fstat (for validation in debug builds)
  - fcntl (for validation in debug builds)
  - S_ISDIR (macro for directory detection)
  - pg_fsync_writethrough (when writethrough mode is configured)
  - pg_fsync_no_writethrough (default synchronization method)
  - wal_sync_method (global configuration variable)
- Called from (representative examples):
  - WriteControlFile
  - XLogFileInitInternal
  - FileSync
  - fsync_fname_ext
  - durable_rename
  - SlruPhysicalWritePage

## Notes and Other Information
- The function includes conditional compilation for systems that support writethrough fsync (HAVE_FSYNC_WRITETHROUGH)
- Debug builds (USE_ASSERT_CHECKING) include extensive validation of file descriptor access modes
- The validation logic helps ensure portability across operating systems with different fsync requirements
- This is the primary entry point for all PostgreSQL file synchronization operations
- Performance-critical as it's called frequently during WAL writing and checkpointing operations
- The choice between writethrough and non-writethrough modes affects both performance and durability guarantees