# FileClose

## Location
[src/backend/storage/file/fd.c:1975-2074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1975-L2074)

## Overview
FileClose closes a virtual file descriptor and performs cleanup operations including temporary file deletion, resource owner deregistration, and VFD slot recycling.

## Definition

```c
struct stat filestats;
```
## Detailed Description
FileClose is a comprehensive file closing function in PostgreSQL's virtual file descriptor system. It handles both regular and temporary files with the following key operations:

1. **File Closure**: If the file is currently open, it closes the underlying OS file descriptor and removes the file from the LRU (Least Recently Used) cache ring
2. **Temporary File Management**: For temporary files, it subtracts the file size from the global temporary_files_size counter
3. **File Deletion**: Files marked with FD_DELETE_AT_CLOSE flag (typically temporary files) are physically deleted from the filesystem
4. **Resource Cleanup**: The function unregisters the file from its resource owner and returns the VFD slot to the free list for reuse
5. **Error Handling**: Provides appropriate error reporting for close failures, with different severity levels for temporary vs. permanent files

The function is designed to be safe even during error conditions, with careful ordering of operations to prevent resource leaks.

## Parameters / Member Variables
- : The virtual file descriptor (File type) to be closed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the file descriptor
  - FileIsNotOpen: Checks if file is currently open
  - close: System call to close the file descriptor
  - Delete: Removes file from LRU ring
  - unlink: System call to delete temporary files
  - ReportTemporaryFileUsage: Reports temporary file usage statistics
  - ResourceOwnerForgetFile: Unregisters file from resource owner
  - FreeVfd: Returns VFD slot to free list
  - data_sync_elevel: Determines error severity level
- Called from (representative examples):
  - BufFileClose: Buffer file management
  - mdclose: Magnetic disk storage manager
  - CleanupTempFiles: Temporary file cleanup during process exit
  - ReorderBufferIterTXNFinish: Logical replication cleanup

## Notes and Other Information
- The function handles both temporary and permanent files with different error severity levels
- Temporary file deletion is logged for monitoring purposes
- The function is designed to be idempotent and safe to call during error recovery
- Critical for proper resource management in PostgreSQL's file descriptor virtualization system
- Files marked for deletion have their flag cleared early to prevent infinite loops during error handling