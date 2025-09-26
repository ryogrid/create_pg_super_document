# datadir_fsync_fname

## Location
src/backend/storage/file/fd.c: 3756 - 3768

## Overview
A callback function that performs actual fsync operations on files and directories during data directory synchronization, with built-in progress reporting and error tolerance for unreadable files.

## Definition

```c
static void
datadir_fsync_fname(const char *fname, bool isdir, int elevel)
```
## Detailed Description
datadir_fsync_fname is a callback function designed to be used with walkdir() during the actual fsync phase of data directory synchronization. This function is called after the pre_sync_fname phase and performs the critical task of ensuring all data is written to persistent storage.

The function is a thin wrapper around fsync_fname_ext() that adds progress reporting and specifies error handling behavior. It's specifically configured to silently ignore errors related to unreadable files, which helps prevent startup failures due to permission issues or special files that cannot be synchronized.

This function represents the second phase of the two-phase synchronization strategy used by SyncDataDirectory, where the first phase (pre_sync_fname) optimizes performance by flushing data to kernel buffers, and this phase ensures the data actually reaches disk.

Key characteristics:
- Handles both regular files and directories
- Reports progress with elapsed time and current file path
- Uses error-tolerant fsync with silent handling of access errors
- Critical for data durability during PostgreSQL startup

## Parameters / Member Variables
- : Full path to the file or directory being synchronized
- : Boolean flag indicating whether the path is a directory
- : Error reporting level for logging synchronization issues

## Dependencies
- Functions called/Symbols referenced:
  - ereport_startup_progress: Report progress during startup synchronization
  - fsync_fname_ext: Extended fsync function with configurable error handling
- Called from (representative examples):
  - SyncDataDirectory: During the main fsync phase of data directory synchronization

## Notes and Other Information
- Works in conjunction with pre_sync_fname to implement a two-phase sync strategy
- The 'true' parameter passed to fsync_fname_ext enables silent handling of unreadable files
- Essential for PostgreSQL's crash recovery and data integrity guarantees
- Progress reporting helps monitor long-running synchronization operations during startup
- Error tolerance is critical for production environments where some files may have restricted access
- Called multiple times by SyncDataDirectory: once for the main data directory, once for pg_wal (if symlinked), and once for tablespaces
- Part of the startup sequence that ensures any pending writes from previous sessions reach disk before new operations begin