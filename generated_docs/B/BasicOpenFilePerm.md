# BasicOpenFilePerm

## Location
[src/backend/storage/file/fd.c:1106-1182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1106-L1182)

## Overview
A robust file opening function that acts as a safe replacement for the standard open() system call, with automatic file descriptor recovery and platform-specific direct I/O handling.

## Definition
```c
int BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
```

## Detailed Description
BasicOpenFilePerm serves as PostgreSQL's primary file opening interface, providing critical enhancements over the standard open() system call:

1. **Automatic FD Recovery**: When open() fails with EMFILE/ENFILE (too many open files), it attempts to free file descriptors by calling ReleaseLruFile() and retries the operation
2. **Direct I/O Support**: Handles platform-specific direct I/O implementation, including F_NOCACHE emulation on systems that don't support O_DIRECT natively
3. **Resource Management**: Integrates with PostgreSQL's file descriptor tracking and management systems

The function includes platform-specific code to handle PG_O_DIRECT flag:
- On systems with native O_DIRECT support, passes it directly to open()
- On macOS and other systems, simulates direct I/O using F_NOCACHE via fcntl()

This function is designed to be the primary (ideally only) direct interface to open() in the PostgreSQL backend, with most code using higher-level VFD (Virtual File Descriptor) abstractions for better resource management.

## Parameters / Member Variables
- `fileName`: Path to the file to be opened
- `fileFlags`: File access flags (O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, etc.) and PostgreSQL-specific flags like PG_O_DIRECT
- `fileMode`: File permissions to use when creating new files (mode_t)

## Dependencies
- Functions called/Symbols referenced:
  - open (system call)
  - close (system call)
  - fcntl (system call, for F_NOCACHE on some platforms)
  - [ReleaseLruFile](../R/ReleaseLruFile.md)
  - ereport
  - StaticAssertStmt (compile-time assertion)
  - Various O_* constants and PG_O_DIRECT
- Called from (representative examples):
  - [BasicOpenFile](BasicOpenFile.md)
  - [readRecoverySignalFile](../r/readRecoverySignalFile.md)
  - [LruInsert](../L/LruInsert.md)
  - [PathNameOpenFilePerm](../P/PathNameOpenFilePerm.md)
  - [OpenTransientFilePerm](../O/OpenTransientFilePerm.md)

## Notes and Other Information
- Includes retry logic for EMFILE/ENFILE errors by releasing least-recently-used files
- Handles platform differences for direct I/O through conditional compilation
- Contains compile-time assertions to ensure PG_O_DIRECT doesn't collide with standard flags
- Returns file descriptor on success, -1 on failure (following open() semantics)
- Once a FD is returned, caller is responsible for preventing descriptor leaks on ereport()
- Most PostgreSQL code should use VFD layer instead of calling this directly
- Critical for PostgreSQL's file descriptor resource management strategy
- Logs informational messages when attempting FD recovery

## Simplified Source

```c
// Simplified version of BasicOpenFilePerm
int BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode) {
    int fd;

tryAgain:
    // Platform-specific handling for direct I/O
    #ifdef PG_O_DIRECT_USE_F_NOCACHE
        // On systems without native O_DIRECT, remove the flag for open()
        fd = open(fileName, fileFlags & ~PG_O_DIRECT, fileMode);
    #else
        // Standard open() call with all flags
        fd = open(fileName, fileFlags, fileMode);
    #endif

    // Success case - handle direct I/O setup if needed
    if (fd >= 0) {
        #ifdef PG_O_DIRECT_USE_F_NOCACHE
            // Simulate O_DIRECT using F_NOCACHE on macOS-like systems
            if (fileFlags & PG_O_DIRECT) {
                if (fcntl(fd, F_NOCACHE, 1) < 0) {
                    int save_errno = errno;
                    close(fd);
                    errno = save_errno;
                    return -1;
                }
            }
        #endif
        return fd;  // Success!
    }

    // Handle "too many open files" errors with retry logic
    if (errno == EMFILE || errno == ENFILE) {
        int save_errno = errno;

        // Log the issue and attempt to free file descriptors
        ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                     errmsg("out of file descriptors: %m; release and retry")));

        errno = 0;
        if (ReleaseLruFile()) {
            goto tryAgain;  // Try opening the file again
        }
        errno = save_errno;
    }

    return -1;  // Failure
}
```

Key simplifications made:
- Removed compile-time assertion checks for clarity
- Consolidated platform-specific conditional compilation logic
- Simplified error handling flow while preserving retry mechanism
- Added clear comments explaining the main execution paths
- Maintained the core algorithm: open → handle direct I/O → retry on resource exhaustion