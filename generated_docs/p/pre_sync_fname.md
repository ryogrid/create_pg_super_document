# pre_sync_fname

## Location
[src/common/file_utils.c:337-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L337-L377)

## Overview
Provides hint to the operating system to prepare a file for an upcoming fsync operation by flushing dirty pages from memory to storage device buffers, optimizing subsequent synchronization performance.

## Definition


**Note**: This documentation covers the frontend version in . There is also a backend version in  with slightly different signature and error handling.

## Detailed Description
The  function implements a performance optimization technique for file synchronization operations. It provides advance notice to the operating system that a file will soon be fsync'd, allowing the OS to start flushing dirty pages from memory to storage device buffers.

This function is only a hint and does not guarantee data persistence - actual synchronization must still be performed with fsync(). The hint can significantly improve performance during bulk synchronization operations by parallelizing the write operations.

Platform-specific implementations:
- **Linux**: Uses  with SYNC_FILE_RANGE_WRITE flag
- **POSIX systems**: Falls back to  with POSIX_FADV_DONTNEED
- **Other platforms**: Not supported (PG_FLUSH_DATA_WORKS not defined)

Error handling is minimal since this is only an optimization hint - failures are ignored and don't affect correctness.

## Parameters / Member Variables
- : File path to hint for upcoming synchronization
- : Boolean flag indicating if the path refers to a directory (directories are handled specially for some error cases)

## Dependencies
- Functions called/Symbols referenced:
  - open/close (POSIX file operations)
  - sync_file_range (Linux-specific)
  - posix_fadvise (POSIX)
  - PG_BINARY
- Called from (representative examples):
  - [sync_pgdata](../s/sync_pgdata.md) (via walkdir)
  - [sync_dir_recurse](../s/sync_dir_recurse.md) (via walkdir)
  - [SyncDataDirectory](../S/SyncDataDirectory.md) (backend version)

## Notes and Other Information
- This function is only compiled when PG_FLUSH_DATA_WORKS is defined at build time
- The function is declared , making it internal to file_utils.c
- Return value: 0 for success/ignored errors, -1 for critical errors (frontend version)
- [Backend](../B/Backend.md) version uses PostgreSQL's file management functions and includes progress reporting
- Access permission errors (EACCES) and directory access errors (EISDIR) are silently ignored
- The optimization is most beneficial for large files and bulk operations like database initialization or backup restoration
- Errors in the hint operation are ignored because the hint is optional for correctness
- Used as a callback function passed to walkdir() for directory tree traversal

## Simplified Source

```c
// Simplified version of pre_sync_fname (backend version)
static void pre_sync_fname(const char *fname, bool isdir, int elevel) {
    int fd;

    // Skip directories - they can't be flushed
    if (isdir)
        return;

    // Report progress during data directory sync
    ereport_startup_progress("syncing data directory (pre-fsync), current path: %s", fname);

    // Open file for reading
    fd = OpenTransientFile(fname, O_RDONLY | PG_BINARY);
    if (fd < 0) {
        // Ignore permission errors, report others
        if (errno == EACCES)
            return;
        ereport(elevel, "could not open file \"%s\"", fname);
        return;
    }

    // Hint to OS: flush dirty pages to storage buffers
    // This is just an optimization hint - errors are ignored
    pg_flush_data(fd, 0, 0);

    // Close the file
    if (CloseTransientFile(fd) != 0)
        ereport(elevel, "could not close file \"%s\"", fname);
}
```

Key simplifications made:
- Removed detailed error code formatting (errcode_for_file_access(), errmsg())
- Simplified progress reporting message format
- Added clear comments explaining the purpose of each major step
- Highlighted that pg_flush_data() is an optimization hint that ignores errors
- Consolidated error handling logic while preserving essential checks
- Maintained the core logic flow: skip directories → report progress → open file → flush data → close file