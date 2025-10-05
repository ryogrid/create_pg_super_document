# FileTruncate

## Location
[src/backend/storage/file/fd.c:2423-2457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2423-L2457)

## Overview
FileTruncate truncates a file to the specified size, with special handling for temporary file size tracking and accounting.

## Definition
```c
int FileTruncate(File file, off_t offset, uint32 wait_event_info)
```

## Detailed Description
FileTruncate reduces the size of a file to the specified offset by using PostgreSQL's ftruncate wrapper function. The function validates the file descriptor, ensures file accessibility, and performs the truncation operation with proper wait event reporting. For temporary files, it includes special accounting logic that updates the global temporary_files_size counter and the cached file size in the VFD entry. This ensures accurate tracking of temporary file space usage across the system. The truncation is performed atomically and any data beyond the specified offset is permanently removed.

## Parameters / Member Variables
- `file`: Virtual file descriptor representing the file to be truncated
- `offset`: New size of the file in bytes; the file will be truncated at this position
- `wait_event_info`: Event information used for wait event reporting during the operation

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the virtual file descriptor
  - [FileAccess](FileAccess.md): Ensures the file is accessible and handles VFD management
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md): Reports the start of a wait event for monitoring
  - [pg_ftruncate](../p/pg_ftruncate.md): PostgreSQL's ftruncate wrapper that performs the actual truncation
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md): Reports the end of the wait event
  - DO_DB: Debug logging macro
  - FD_TEMP_FILE_LIMIT: Flag indicating the file is a temporary file with size tracking
- Called from (representative examples):
  - [BufFileTruncateFileSet](../B/BufFileTruncateFileSet.md): When truncating buffered file sets
  - [mdtruncate](../m/mdtruncate.md): In MD storage manager for relation truncation operations

## Notes and Other Information
- Returns 0 on success, or the error code from pg_ftruncate() on failure
- Includes special accounting for temporary files by updating temporary_files_size global variable
- Updates the cached fileSize in the VFD entry for temporary files to maintain consistency
- The temporary file handling is protected by assertions to ensure proper file state
- Data beyond the specified offset is permanently lost after truncation
- Part of PostgreSQL's Virtual File Descriptor (VFD) system
- Critical for space management, especially during relation maintenance and temporary file cleanup
- Wait event reporting allows monitoring of potentially long-running truncation operations
- Used extensively in storage management for both permanent and temporary file maintenance

## Simplified Source

```c
int FileTruncate(File file, off_t offset, uint32 wait_event_info) {
    // Validate file descriptor
    Assert(FileIsValid(file));

    // Ensure file is accessible
    int returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    // Perform truncation with wait event reporting
    pgstat_report_wait_start(wait_event_info);
    returnCode = pg_ftruncate(VfdCache[file].fd, offset);
    pgstat_report_wait_end();

    // Update temporary file size tracking if truncated
    if (returnCode == 0 && VfdCache[file].fileSize > offset) {
        // Adjust global temporary file size counter
        temporary_files_size -= VfdCache[file].fileSize - offset;
        VfdCache[file].fileSize = offset;
    }

    return returnCode;
}
```