# BasicOpenFile

## Location
[src/backend/storage/file/fd.c:1084-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1084-L1105)

## Overview
A convenience wrapper function that opens a file using default permissions by calling BasicOpenFilePerm with PostgreSQL's standard file creation mode.

## Definition
```c
int BasicOpenFile(const char *fileName, int fileFlags)
```

## Detailed Description
BasicOpenFile provides a simplified interface for opening files when the default PostgreSQL file permissions are acceptable. It internally calls BasicOpenFilePerm with `pg_file_create_mode` as the permission parameter, eliminating the need for callers to specify file permissions explicitly.

This function is commonly used throughout PostgreSQL for opening files where the standard database file permissions (typically 0600 - readable and writable by owner only) are appropriate. It's particularly useful for opening WAL files, control files, and other PostgreSQL-managed files that should have consistent permissions.

## Parameters / Member Variables
- `fileName`: Path to the file to be opened
- `fileFlags`: File access flags (O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [BasicOpenFilePerm](BasicOpenFilePerm.md)
  - pg_file_create_mode (global variable for default file permissions)
- Called from (representative examples):
  - [XLogFileInitInternal](../X/XLogFileInitInternal.md)
  - [XLogFileInit](../X/XLogFileInit.md)  
  - [XLogFileOpen](../X/XLogFileOpen.md)
  - [WriteControlFile](../W/WriteControlFile.md)
  - [ReadControlFile](../R/ReadControlFile.md)
  - [XLogFileRead](../X/XLogFileRead.md)
  - [wal_segment_open](../w/wal_segment_open.md)
  - [WalSndSegmentOpen](../W/WalSndSegmentOpen.md)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md)
  - [update_controlfile](../u/update_controlfile.md)

## Notes and Other Information
- This is a thin wrapper around BasicOpenFilePerm for convenience
- Uses PostgreSQL's standard file creation mode (pg_file_create_mode) automatically
- Commonly used for WAL files, control files, and configuration files
- Returns a file descriptor on success, or -1 on error
- The actual file opening logic and error handling is implemented in BasicOpenFilePerm
- Helps maintain consistent file permissions across PostgreSQL-managed files

## Simplified Source

```c
// Simplified version of BasicOpenFile
int BasicOpenFile(const char *fileName, int fileFlags) {
    // Use default PostgreSQL file permissions (pg_file_create_mode)
    return BasicOpenFilePerm(fileName, fileFlags, pg_file_create_mode);
}

// Simplified version of the underlying BasicOpenFilePerm function
int BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode) {
    int fd;

tryAgain:
    // Attempt to open the file with specified flags and mode
    fd = open(fileName, fileFlags, fileMode);

    if (fd >= 0) {
        // Success - return the file descriptor
        return fd;
    }

    // Handle "out of file descriptors" error
    if (errno == EMFILE || errno == ENFILE) {
        // Log the issue and try to free up file descriptors
        ereport(LOG, (errmsg("out of file descriptors: %m; release and retry")));

        // Try to release an unused file descriptor
        if (ReleaseLruFile()) {
            goto tryAgain;  // Retry the open operation
        }
    }

    // Failure - return error code
    return -1;
}
```

Key simplifications made:
- Removed platform-specific O_DIRECT handling code for clarity
- Removed detailed static assertions and flag collision checks
- Simplified error handling to focus on the main retry logic
- Abstracted the file descriptor release mechanism
- Focused on the main execution path: open file, handle descriptor shortage, retry if possible