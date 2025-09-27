# fsync_fname_ext

## Location
[src/backend/storage/file/fd.c:3794-3869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3794-L3869)

## Overview
A comprehensive file/directory synchronization function that safely fsyncs files or directories with appropriate error handling for platform-specific behaviors.

## Definition
```c
int fsync_fname_ext(const char *fname, bool isdir, bool ignore_perm, int elevel)
```

## Detailed Description
This function provides a robust wrapper around fsync operations that handles the cross-platform differences in file and directory synchronization. It opens the specified file or directory with appropriate flags, performs the fsync operation, and handles various platform-specific error conditions gracefully. The function is designed to be tolerant of permission errors when requested and logs errors at a caller-specified level.

The function handles several OS-specific behaviors:
- Some OSes require directories to be opened read-only while others don't allow fsync on read-only files
- Windows returns EACCES when trying to open directories
- Some systems don't allow fsync on directories and return EBADF or EINVAL

## Parameters / Member Variables
- `fname`: The path to the file or directory to be synced
- `isdir`: Boolean flag indicating whether the target is a directory (true) or file (false)
- `ignore_perm`: Boolean flag to ignore permission errors (EACCES) when opening files
- `elevel`: Error logging level to use for reporting errors (e.g., ERROR, WARNING)

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTransientFile](../O/OpenTransientFile.md): Opens the file/directory with transient file management
  - [pg_fsync](../p/pg_fsync.md): PostgreSQL's fsync wrapper function
  - [CloseTransientFile](../C/CloseTransientFile.md): Closes the transient file descriptor
  - PG_BINARY: Binary file flag for cross-platform compatibility
  - ereport: Error reporting function
  - [errcode_for_file_access](../e/errcode_for_file_access.md): Error code generation for file access errors

- Called from (representative examples):
  - [fsync_fname](fsync_fname.md): Simpler fsync wrapper function
  - [durable_rename](../d/durable_rename.md): File rename with durability guarantees
  - [datadir_fsync_fname](../d/datadir_fsync_fname.md): Data directory specific fsync
  - [fsync_parent_path](fsync_parent_path.md): Parent directory synchronization

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Handles platform-specific directory fsync limitations gracefully
- Uses transient file descriptors to avoid file descriptor leaks
- Provides flexible error reporting through configurable error levels
- Critical for ensuring data durability in PostgreSQL's storage operations

## Simplified Source

```c
// Simplified version of fsync_fname_ext
int fsync_fname_ext(const char *fname, bool isdir, bool ignore_perm, int elevel) {
    int fd;
    int flags;
    int returncode;

    // Set appropriate open flags based on file vs directory
    flags = PG_BINARY;
    if (!isdir) {
        flags |= O_RDWR;    // Files need write access for fsync
    } else {
        flags |= O_RDONLY;  // Directories opened read-only
    }

    // Open the file/directory
    fd = OpenTransientFile(fname, flags);

    // Handle platform-specific directory and permission errors
    if (fd < 0 && isdir && (errno == EISDIR || errno == EACCES)) {
        return 0;  // Some OSes don't allow opening directories
    } else if (fd < 0 && ignore_perm && errno == EACCES) {
        return 0;  // Ignore permission errors if requested
    } else if (fd < 0) {
        ereport(elevel, (errmsg("could not open file \"%s\": %m", fname)));
        return -1;
    }

    // Perform the fsync operation
    returncode = pg_fsync(fd);

    // Handle platform-specific fsync errors for directories
    if (returncode != 0 && !(isdir && (errno == EBADF || errno == EINVAL))) {
        int save_errno = errno;
        (void) CloseTransientFile(fd);
        errno = save_errno;
        ereport(elevel, (errmsg("could not fsync file \"%s\": %m", fname)));
        return -1;
    }

    // Close the file descriptor
    if (CloseTransientFile(fd) != 0) {
        ereport(elevel, (errmsg("could not close file \"%s\": %m", fname)));
        return -1;
    }

    return 0;
}
```

Key simplifications made:
- Added clear comments explaining OS-specific behavior handling
- Condensed complex error handling while preserving essential safety checks
- Maintained the platform-specific workarounds for directory operations
- Preserved the flexible error reporting mechanism
- Kept the essential open-fsync-close workflow with proper error handling