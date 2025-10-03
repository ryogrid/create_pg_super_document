# fdatasync

## Location
[src/port/win32fdatasync.c:23-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32fdatasync.c#L23-L51)

## Overview
The  function is a Windows-specific implementation that provides POSIX  functionality for synchronizing a file's data to storage without flushing metadata.

## Definition

```c
int
fdatasync(int fd)
```
## Detailed Description
This function implements the POSIX  system call for Windows platforms where the native function is not available. Unlike  which synchronizes both file data and metadata,  only synchronizes the file's data contents to persistent storage, making it potentially faster for scenarios where metadata synchronization is not required.

The implementation uses Windows NT's native API  with the  flag to achieve data-only synchronization. This provides better performance than full synchronization while still ensuring data durability.

The function handles Windows-specific error mapping and provides proper POSIX-compliant return values and error codes.

## Parameters / Member Variables
- `fd`: The file descriptor of the file to be synchronized. Must be a valid file descriptor obtained from a successful file open operation.
## Dependencies
- Functions called/Symbols referenced:
  -  - Convert C runtime file descriptor to Windows HANDLE
  -  - [Initialize](../I/Initialize.md) NT DLL function pointers
  -  - Windows NT API for selective buffer flushing
  -  - Flag constant for data-only sync
  -  - Convert NT status to DOS error code
  -  - Map DOS error to C runtime errno

- Called from (representative examples):
  -  - PostgreSQL wrapper function in fd.c:485
  -  - File sync testing utility in pg_test_fsync.c:351

## Notes and Other Information
- This function is only compiled and used on Windows platforms as part of the portability layer
- The function is declared in  for Windows builds
- Returns 0 on success, -1 on failure with appropriate errno set
- Handles EINVAL error case by converting Windows NTSTATUS to POSIX errno values
- Part of PostgreSQL's effort to provide cross-platform POSIX compatibility
- The data-only synchronization can provide performance benefits over full fsync in write-heavy scenarios where metadata changes are less frequent
- Used internally by PostgreSQL's file descriptor management system through the  wrapper

## Simplified Source

```c
// Simplified version of fdatasync - Windows implementation
int fdatasync(int fd) {
    IO_STATUS_BLOCK iosb;
    NTSTATUS status;
    HANDLE handle;

    // Convert file descriptor to Windows handle
    handle = (HANDLE) _get_osfhandle(fd);
    if (handle == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    // Initialize NT DLL functions if needed
    if (initialize_ntdll() < 0)
        return -1;

    // Flush only file data (not metadata) to storage
    memset(&iosb, 0, sizeof(iosb));
    status = pg_NtFlushBuffersFileEx(handle,
                                    FLUSH_FLAGS_FILE_DATA_SYNC_ONLY,
                                    NULL, 0, &iosb);

    // Return success or map error to POSIX errno
    if (NT_SUCCESS(status))
        return 0;

    _dosmaperr(pg_RtlNtStatusToDosError(status));
    return -1;
}
```

Key simplifications made:
- Consolidated error handling logic for clarity
- Added descriptive comments explaining each major step
- Maintained the essential Windows-specific implementation details
- Preserved the data-only synchronization semantics that distinguish fdatasync from fsync
- Kept critical error mapping between Windows and POSIX error codes