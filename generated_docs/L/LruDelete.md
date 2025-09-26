# LruDelete

## Location
src/backend/storage/file/fd.c: 1284 - 1309

## Overview
LruDelete closes a virtual file descriptor and removes it from the LRU chain, performing both physical file closure and VFD cache cleanup.

## Definition

```c
static void
LruDelete(File file)
```
## Detailed Description
This function performs a complete removal of a virtual file descriptor from PostgreSQL's VFD cache system. Unlike the Delete function which only removes a VFD from the LRU chain, LruDelete also closes the actual underlying file descriptor and updates the system's file descriptor count.

The function first attempts to close the underlying file descriptor using the standard close() system call. If the close fails, it logs an error message but continues with cleanup to avoid corrupting internal state - the philosophy being that it's better to leak a file descriptor than to have inconsistent internal bookkeeping.

After closing (or attempting to close) the file, it marks the VFD as closed by setting fd to VFD_CLOSED, decrements the global nfile counter, and finally calls Delete to remove the VFD from the LRU chain.

The error logging behavior differs based on the file type: temporary files (FD_TEMP_FILE_LIMIT flag set) log at LOG level, while regular files use data_sync_elevel(LOG) which may result in higher severity logging.

## Parameters / Member Variables
- : The File index (VFD index) to close and remove from the LRU chain

## Dependencies
- Functions called/Symbols referenced:
  - File (typedef for VFD index)
  - Vfd (VFD structure type)
  - VfdCache (global VFD cache array)
  - close (system call to close file descriptor)
  - FD_TEMP_FILE_LIMIT (flag for temporary files)
  - data_sync_elevel (function for determining error log level)
  - VFD_CLOSED (constant indicating closed state)
  - Delete (function to remove from LRU chain)
  - DO_DB (debug macro)
  - elog (logging function)
  - Assert (assertion macro)
- Called from (representative examples):
  - AllocateDesc
  - ReleaseLruFile
  - FileInvalidate
  - closeAllVfds

## Notes and Other Information
- Static function, only accessible within the fd.c source file
- Includes assertion to prevent deletion of VFD index 0 (invalid/sentinel value)
- Handles close() failures gracefully to maintain internal consistency
- Different logging levels for temporary vs regular files on close failure
- Decrements global nfile counter to track open file descriptor count
- Combines file descriptor closure with LRU chain removal in single operation
- Used when VFDs need to be completely removed from the cache, not just reordered