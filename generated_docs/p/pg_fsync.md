# pg_fsync

## Location
[src/backend/storage/file/fd.c:386-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L386-L437)

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
  - [pg_fsync_writethrough](pg_fsync_writethrough.md) (when writethrough mode is configured)
  - [pg_fsync_no_writethrough](pg_fsync_no_writethrough.md) (default synchronization method)
  - wal_sync_method (global configuration variable)
- Called from (representative examples):
  - [WriteControlFile](../W/WriteControlFile.md)
  - [XLogFileInitInternal](../X/XLogFileInitInternal.md)
  - [FileSync](../F/FileSync.md)
  - [fsync_fname_ext](../f/fsync_fname_ext.md)
  - [durable_rename](../d/durable_rename.md)
  - [SlruPhysicalWritePage](../S/SlruPhysicalWritePage.md)

## Notes and Other Information
- The function includes conditional compilation for systems that support writethrough fsync (HAVE_FSYNC_WRITETHROUGH)
- Debug builds (USE_ASSERT_CHECKING) include extensive validation of file descriptor access modes
- The validation logic helps ensure portability across operating systems with different fsync requirements
- This is the primary entry point for all PostgreSQL file synchronization operations
- Performance-critical as it's called frequently during WAL writing and checkpointing operations
- The choice between writethrough and non-writethrough modes affects both performance and durability guarantees

## Simplified Source

```c
// Simplified version of pg_fsync
int pg_fsync(int fd) {
    // Debug builds: Validate file descriptor access modes
    // Files must be opened with write permissions, directories with read-only
    #if !defined(WIN32) && defined(USE_ASSERT_CHECKING)
    struct stat st;
    if (fstat(fd, &st) == 0) {
        int desc_flags = fcntl(fd, F_GETFL) & O_ACCMODE;
        if (S_ISDIR(st.st_mode))
            Assert(desc_flags == O_RDONLY);  // Directories: read-only
        else
            Assert(desc_flags != O_RDONLY);  // Files: writable
    }
    #endif

    // Choose fsync method based on configuration
    #if defined(HAVE_FSYNC_WRITETHROUGH)
    if (wal_sync_method == WAL_SYNC_METHOD_FSYNC_WRITETHROUGH)
        return pg_fsync_writethrough(fd);
    else
    #endif
        return pg_fsync_no_writethrough(fd);
}
```

Key simplifications made:
- Condensed debug validation logic while preserving essential checks
- Simplified comments to explain the purpose of each section
- Removed detailed portability comments for clarity
- Maintained the core dispatch logic between writethrough and non-writethrough modes
- Kept the conditional compilation structure intact