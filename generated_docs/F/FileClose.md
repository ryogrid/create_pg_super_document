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
  - [Delete](../D/Delete.md): Removes file from LRU ring
  - unlink: System call to delete temporary files
  - [ReportTemporaryFileUsage](../R/ReportTemporaryFileUsage.md): Reports temporary file usage statistics
  - [ResourceOwnerForgetFile](../R/ResourceOwnerForgetFile.md): Unregisters file from resource owner
  - [FreeVfd](FreeVfd.md): Returns VFD slot to free list
  - [data_sync_elevel](../d/data_sync_elevel.md): Determines error severity level
- Called from (representative examples):
  - [BufFileClose](../B/BufFileClose.md): Buffer file management
  - [mdclose](../m/mdclose.md): Magnetic disk storage manager
  - [CleanupTempFiles](../C/CleanupTempFiles.md): Temporary file cleanup during process exit
  - [ReorderBufferIterTXNFinish](../R/ReorderBufferIterTXNFinish.md): Logical replication cleanup

## Notes and Other Information
- The function handles both temporary and permanent files with different error severity levels
- Temporary file deletion is logged for monitoring purposes
- The function is designed to be idempotent and safe to call during error recovery
- Critical for proper resource management in PostgreSQL's file descriptor virtualization system
- Files marked for deletion have their flag cleared early to prevent infinite loops during error handling

## Simplified Source

```c
// Simplified version of FileClose
void FileClose(File file) {
    Vfd *vfd_entry;

    Assert(FileIsValid(file));
    DO_DB(elog(LOG, "FileClose: %d (%s)", file, VfdCache[file].fileName));

    vfd_entry = &VfdCache[file];

    // Step 1: Close the underlying OS file descriptor if open
    if (!FileIsNotOpen(file)) {
        if (close(vfd_entry->fd) != 0) {
            // Use different error levels for temp vs permanent files
            int error_level = (vfd_entry->fdstate & FD_TEMP_FILE_LIMIT) ? LOG : data_sync_elevel(LOG);
            elog(error_level, "could not close file \"%s\": %m", vfd_entry->fileName);
        }

        --nfile;
        vfd_entry->fd = VFD_CLOSED;
        Delete(file);  // Remove from LRU ring
    }

    // Step 2: Handle temporary file size accounting
    if (vfd_entry->fdstate & FD_TEMP_FILE_LIMIT) {
        temporary_files_size -= vfd_entry->fileSize;
        vfd_entry->fileSize = 0;
    }

    // Step 3: Delete temporary files marked for deletion
    if (vfd_entry->fdstate & FD_DELETE_AT_CLOSE) {
        struct stat file_stats;
        int stat_result;

        // Clear flag early to prevent infinite loops during error handling
        vfd_entry->fdstate &= ~FD_DELETE_AT_CLOSE;

        // Get file stats before deletion for reporting
        stat_result = stat(vfd_entry->fileName, &file_stats);

        // Delete the file
        if (unlink(vfd_entry->fileName)) {
            ereport(LOG, (errcode_for_file_access(),
                         errmsg("could not delete file \"%s\": %m", vfd_entry->fileName)));
        }

        // Report temporary file usage statistics
        if (stat_result == 0) {
            ReportTemporaryFileUsage(vfd_entry->fileName, file_stats.st_size);
        } else {
            ereport(LOG, (errcode_for_file_access(),
                         errmsg("could not stat file \"%s\": %m", vfd_entry->fileName)));
        }
    }

    // Step 4: Clean up resource ownership and return VFD slot to free list
    if (vfd_entry->resowner) {
        ResourceOwnerForgetFile(vfd_entry->resowner, file);
    }

    FreeVfd(file);
}
```

Key simplifications made:
- Renamed vfdP to vfd_entry for clarity
- Added step-by-step comments organizing the main phases
- Simplified error handling logic while preserving functionality
- Consolidated stat error handling
- Maintained all essential cleanup operations
- Preserved the careful ordering of operations for error safety