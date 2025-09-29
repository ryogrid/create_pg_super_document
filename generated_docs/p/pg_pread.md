# pg_pread

## Location
[src/port/win32pread.c:20-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32pread.c#L20-L48)

## Overview
pg_pread is a PostgreSQL wrapper function for positioned read operations that provides platform-independent behavior across Unix-like systems and Windows.

## Definition

```c
ssize_t
pg_pread(int fd, void *buf, size_t size, off_t offset)
```
## Detailed Description
pg_pread provides a unified interface for reading data from a file descriptor at a specific offset. The function has two distinct implementations:

1. **Unix-like systems**: pg_pread is simply a macro definition that maps directly to the system's standard pread() function, which performs atomic positioned reads without changing the file's current position.

2. **Windows systems**: pg_pread is implemented as a custom function in  that uses Windows' ReadFile() with OVERLAPPED structure to simulate positioned reading. However, unlike the POSIX pread(), the Windows implementation has the side effect of changing the current file position.

The Windows implementation includes several safety measures:
- Validates the file handle using _get_osfhandle()
- Limits read size to 1GB to avoid DWORD overflow
- Maps Windows error codes to errno values using _dosmaperr()
- Handles end-of-file conditions appropriately

## Parameters / Member Variables
- : File descriptor to read from
- : Pointer to buffer where read data will be stored
- : Number of bytes to read (limited to 1GB on Windows)
- : File offset from which to start reading

## Dependencies
- Functions called/Symbols referenced:
  - _get_osfhandle (Windows only)
  - ReadFile (Windows only) 
  - GetLastError (Windows only)
  - [_dosmaperr](../d/_dosmaperr.md) (Windows only)
  - Min (Windows only)
  - pread (Unix systems, via macro)

- Called from (representative examples):
  - [SlruPhysicalReadPage](../S/SlruPhysicalReadPage.md)
  - [XLogPageRead](../X/XLogPageRead.md)  
  - [basebackup_read_file](../b/basebackup_read_file.md)
  - [WALRead](../W/WALRead.md)
  - [pg_preadv](pg_preadv.md) (fallback implementation)
  - reconstruct.c (pg_combinebackup)
  - xlogreader.c (WAL reading)
  - xlogrecovery.c (recovery operations)

## Notes and Other Information
- **Platform Differences**: The most important aspect of pg_pread is the behavioral difference between platforms. On Unix systems, it maintains the current file position unchanged, while on Windows it modifies the file position as a side effect.
- **Error Handling**: The function is frequently used in conjunction with WALReadError structure, which specifically tracks errno values from pg_pread operations.
- **Performance**: Used extensively in I/O-intensive operations like WAL reading, SLRU page access, and backup operations where positioned reads are critical for performance.
- **Thread Safety**: The Windows implementation uses OVERLAPPED I/O which allows for concurrent access, though the file position side effect must be considered in multi-threaded contexts.
- **Size Limitations**: On Windows, reads are limited to 1GB per call to prevent integer overflow issues with the Windows API.

## Simplified Source

```c
// Platform-independent positioned read function (Windows implementation shown)
ssize_t pg_pread(int fd, void *buf, size_t size, off_t offset)
{
    OVERLAPPED overlapped = {0};
    HANDLE handle;
    DWORD result;

    // Get Windows handle from file descriptor
    handle = (HANDLE) _get_osfhandle(fd);
    if (handle == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    // Limit size to prevent DWORD overflow (1GB max)
    size = Min(size, 1024 * 1024 * 1024);

    // Set up overlapped structure for positioned read
    overlapped.Offset = offset;

    // Perform the read operation
    if (!ReadFile(handle, buf, size, &result, &overlapped)) {
        if (GetLastError() == ERROR_HANDLE_EOF)
            return 0;  // End of file

        _dosmaperr(GetLastError());  // Map Windows error to errno
        return -1;
    }

    return result;  // Return number of bytes read
}
```