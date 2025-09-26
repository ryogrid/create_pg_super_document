# durable_unlink

## Location
src/backend/storage/file/fd.c: 869 - 899

## Overview
A crash-safe wrapper around the unlink(2) system call that ensures file deletion is durably persisted to storage by synchronizing the parent directory after the unlink operation.

## Definition
int durable_unlink(const char *fname, int elevel)

## Detailed Description
durable_unlink provides crash-safe file deletion by combining the standard unlink system call with a subsequent fsync operation on the parent directory. This ensures that the file deletion will persist even in the event of a system crash, preventing the system from being left in an inconsistent state where the file appears to exist but may actually be deleted or vice versa.

The function implements a simple but effective strategy:
1. Perform the actual file deletion using unlink(2)
2. Fsync the parent directory to ensure the directory entry removal is persistent on disk

This approach guarantees that once the function returns successfully, the file deletion is durable and will survive system crashes.

## Parameters / Member Variables
- `fname`: Path to the file to be removed
- `elevel`: Error logging level to use for any error messages (e.g., ERROR, WARNING)

## Dependencies
- Functions called/Symbols referenced:
  - unlink
  - fsync_parent_path
- Called from (representative examples):
  - InstallXLogFileSegment
  - RemoveXlogFile
  - StartupXLOG

## Notes and Other Information
- Essential for maintaining PostgreSQL's crash recovery guarantees when removing files
- Primarily used in WAL (Write-Ahead Logging) file management operations
- The function returns 0 on success and -1 on failure, but errno is not guaranteed to be valid upon return
- Much simpler than durable_rename as it only needs to sync the parent directory after deletion
- Critical for ensuring that old WAL files and other temporary files are properly removed in a crash-safe manner
- The parent directory fsync ensures that the directory metadata changes (removing the file entry) are persistent