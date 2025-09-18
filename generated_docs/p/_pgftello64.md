# _pgftello64

## Location
[src/port/win32fseek.c:56-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32fseek.c#L56-L75)

## Overview
Windows-specific wrapper for the ftell() function that provides 64-bit file position support with proper error handling for non-seeking devices.

## Definition
```c
pgoff_t _pgftello64(FILE *stream)
```

## Detailed Description
The `_pgftello64` function is a PostgreSQL-specific wrapper around the standard ftell() function for Windows platforms. Similar to `_pgfseeko64`, it addresses limitations when dealing with non-seeking devices such as pipes or communication devices where ftell() operations are not meaningful or supported.

The function determines the file type using `pgwin32_get_file_type()` and only allows position queries on disk files. For character devices and pipes, it properly sets errno to ESPIPE (illegal seek), and for other file types, it sets errno to EINVAL (invalid argument).

This wrapper ensures consistent behavior and proper error reporting when querying file positions across different device types on Windows platforms.

## Parameters / Member Variables
- `stream`: FILE pointer to the stream for which to get the current position

## Dependencies
- Functions called/Symbols referenced:
  - pgwin32_get_file_type (Windows file type detection function)
  - _ftelli64 (Microsoft's 64-bit ftell implementation)
  - _get_osfhandle (convert C runtime file descriptor to Windows handle)
  - _fileno (get file descriptor from FILE pointer)
- Called from (representative examples):
  - ftello (via macro redefinition in win32_port.h)

## Notes and Other Information
- This function is Windows-specific and located in src/port/win32fseek.c
- Returns current file position as pgoff_t (64-bit) on success, -1 on failure with errno set
- Only supports position queries on FILE_TYPE_DISK; returns ESPIPE for pipes/character devices
- Part of PostgreSQL's Windows compatibility layer
- Companion function to `_pgfseeko64` for file position operations
- Uses Microsoft's _ftelli64() for actual disk file position queries