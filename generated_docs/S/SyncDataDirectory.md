# SyncDataDirectory

## Location
src/backend/storage/file/fd.c: 3541 - 3654

## Overview
Issues fsync recursively on PGDATA and all its contents, or alternatively uses syncfs for filesystem-wide synchronization, ensuring that all pending writes reach disk during database startup recovery.

## Definition

```c
struct stat st;
```
## Detailed Description
SyncDataDirectory is a critical function used during PostgreSQL startup to synchronize the entire data directory to persistent storage. The function handles the possibility that there are issued-but-unsynced writes pending against the data directory from a previous session.

The function operates in different modes based on the  setting:
1. **syncfs mode** (Linux only): Uses syncfs() to sync entire filesystems, which is more efficient
2. **fsync mode**: Recursively walks through all directories and files, issuing individual fsync() calls

The function carefully handles symlinks - it follows symlinks only for pg_wal and directories immediately under pg_tblspc, but ignores other symlinks as they are presumed to point at files PostgreSQL is not responsible for syncing.

Error handling is designed to be non-fatal - errors are logged but don't abort startup, preventing failures due to harmless cases like read-only files in the data directory.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - lstat: Check if pg_wal is a symlink
  - S_ISLNK: Test symlink status
  - begin_startup_progress_phase: Progress reporting during startup
  - do_syncfs: Perform syncfs() operation (Linux only)
  - AllocateDir/ReadDirExtended/FreeDir: Directory traversal
  - walkdir: Recursive directory walking
  - pre_sync_fname: Pre-sync hint callback
  - datadir_fsync_fname: fsync callback function
- Called from (representative examples):
  - StartupXLOG: During WAL recovery startup

## Notes and Other Information
- The function assumes it's already chdir'd into PGDATA
- Can be completely skipped if fsync is disabled via enableFsync
- Uses conditional compilation for Linux-specific syncfs functionality
- Implements a two-phase approach when using fsync: pre-sync hints followed by actual fsync operations
- Special handling for pg_wal symlinks and tablespace directories under pg_tblspc
- Progress reporting is integrated for startup monitoring
- Error tolerance design ensures startup robustness even with permission or access issues