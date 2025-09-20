# _pgfseeko64

## Location
[src/port/win32fseek.c:31-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32fseek.c#L31-L55)

## Overview
Windows-specific wrapper for the fseek() function that provides 64-bit file offset support with proper error handling for non-seeking devices.

## Definition

```c
int
_pgfseeko64(FILE *stream, pgoff_t offset, int origin)
```
## Detailed Description
The  function is a PostgreSQL-specific wrapper around the standard fseek() function for Windows platforms. It addresses limitations of the standard fseek() function when dealing with non-seeking devices such as pipes or communication devices, where fseek() may not return proper error codes.

The function first determines the file type using  and only allows seeking operations on disk files. For character devices and pipes, it properly sets errno to ESPIPE (illegal seek), and for other file types, it sets errno to EINVAL (invalid argument).

This wrapper ensures consistent and reliable behavior across different device types on Windows, providing better error reporting than the standard library functions.

## Parameters / Member Variables
- : FILE pointer to the stream on which to perform the seek operation
- : 64-bit offset value (pgoff_t) specifying the new position relative to the origin
- : Reference point for the offset (SEEK_SET, SEEK_CUR, or SEEK_END)

## Dependencies
- Functions called/Symbols referenced:
  - pgoff_t (typedef for 64-bit offset)
  - pgwin32_get_file_type (Windows file type detection function)
  - _fseeki64 (Microsoft's 64-bit fseek implementation)
  - _get_osfhandle (convert C runtime file descriptor to Windows handle)
  - _fileno (get file descriptor from FILE pointer)
- Called from (representative examples):
  - fseeko (via macro redefinition in win32_port.h)

## Notes and Other Information
- This function is Windows-specific and located in src/port/win32fseek.c
- Returns 0 on success, -1 on failure with errno set appropriately
- Only supports seeking on FILE_TYPE_DISK; returns ESPIPE for pipes/character devices
- Part of PostgreSQL's Windows compatibility layer
- The function sets errno to ESPIPE for unsupported device types (pipes, character devices)
- Uses Microsoft's _fseeki64() for actual disk file seeking operations