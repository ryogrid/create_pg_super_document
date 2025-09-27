# pg_pwrite

## Location
[src/port/win32pwrite.c:20-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32pwrite.c#L20-L45)

## Overview
A Windows-specific implementation of the POSIX pwrite() function that writes data to a file at a specified offset without changing the file position pointer.

## Definition
```c
ssize_t pg_pwrite(int fd, const void *buf, size_t size, off_t offset)
```

## Detailed Description
`pg_pwrite` provides a Windows-compatible implementation of the POSIX `pwrite()` system call. This function writes data to a file descriptor at a specified offset without modifying the current file position. It uses the Windows API `WriteFile()` function with an `OVERLAPPED` structure to achieve position-independent writing.

The function is part of PostgreSQL's portability layer, specifically designed to provide POSIX-like file I/O functionality on Windows systems where native `pwrite()` is not available. It handles the conversion between Unix-style file descriptors and Windows file handles, and provides appropriate error mapping.

Key behavioral characteristics:
- Writes up to 1GB of data in a single call to prevent DWORD overflow
- Uses overlapped I/O to specify the write position
- Maps Windows errors to POSIX errno values
- Despite using overlapped I/O, it still changes the actual file position (Windows API limitation)

## Parameters / Member Variables
- `fd`: File descriptor obtained from standard file operations (open, etc.)
- `buf`: Pointer to the buffer containing data to write
- `size`: Number of bytes to write (limited to 1GB maximum)
- `offset`: File offset where writing should begin

## Dependencies
- Functions called/Symbols referenced:
  - `_get_osfhandle` (Windows CRT function)
  - `WriteFile` (Windows API)
  - `GetLastError` (Windows API)  
  - `[_dosmaperr](../d/_dosmaperr.md)` (PostgreSQL error mapping function)
  - `Min` (PostgreSQL macro)
  - `OVERLAPPED.Offset` (Windows structure member)

- Called from (representative examples):
  - `[heap_xlog_logical_rewrite](../h/heap_xlog_logical_rewrite.md)` (src/backend/access/heap/rewriteheap.c:1115)
  - `[SlruPhysicalWritePage](../S/SlruPhysicalWritePage.md)` (src/backend/access/transam/slru.c:990)
  - `[XLogWrite](../X/XLogWrite.md)` (src/backend/access/transam/xlog.c:2428)
  - `[XLogFileInitInternal](../X/XLogFileInitInternal.md)` (src/backend/access/transam/xlog.c:3266)
  - `[XLogWalRcvWrite](../X/XLogWalRcvWrite.md)` (src/backend/replication/walreceiver.c:944)
  - `[AddToDataDirLockFile](../A/AddToDataDirLockFile.md)` (src/backend/utils/init/miscinit.c:1648)
  - `[pg_pwritev](pg_pwritev.md)` (src/include/port/pg_iovec.h:106)
  - Various functions in `pg_test_fsync` utility

## Notes and Other Information
- This function is Windows-specific and located in `src/port/win32pwrite.c`
- The 1GB size limit prevents overflow of the DWORD parameter in WriteFile()
- Unlike POSIX pwrite(), this implementation does change the file position due to Windows API limitations, as noted in the source comment
- Error handling converts Windows error codes to appropriate POSIX errno values using `_dosmaperr()`
- The function returns the number of bytes actually written, or -1 on error
- This is a critical component of PostgreSQL's I/O subsystem on Windows, used extensively for WAL writing, SLRU operations, and other low-level file operations

## Simplified Source

```c
// Simplified version of pg_pwrite
ssize_t pg_pwrite(int fd, const void *buf, size_t size, off_t offset) {
    OVERLAPPED overlapped = {0};
    HANDLE handle;
    DWORD bytes_written;

    // Convert file descriptor to Windows handle
    handle = (HANDLE) _get_osfhandle(fd);
    if (handle == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    // Limit size to prevent DWORD overflow (max 1GB)
    size = Min(size, 1024 * 1024 * 1024);

    // Set the file offset for positioned write
    overlapped.Offset = offset;

    // Perform the write operation
    if (!WriteFile(handle, buf, size, &bytes_written, &overlapped)) {
        _dosmaperr(GetLastError());
        return -1;
    }

    return bytes_written;
}
```

Key simplifications made:
- Renamed `result` variable to `bytes_written` for clarity
- Added descriptive comments for each major step
- Simplified error handling flow
- Focused on the main execution path
- Preserved all essential logic and error checking