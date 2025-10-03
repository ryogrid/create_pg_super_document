# pgwin32_get_file_type

## Location
[src/port/win32common.c:31-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32common.c#L31-L64)

## Overview
A convenience wrapper function for the Windows GetFileType() API that provides standardized error handling across PostgreSQL's Windows port implementations. This function determines the type of file associated with a given Windows file handle.

## Definition

```c
DWORD
pgwin32_get_file_type(HANDLE hFile)
```
## Detailed Description
The  function serves as a robust wrapper around the Windows GetFileType() API, providing consistent error handling and validation for PostgreSQL's Windows-specific code. It returns the file type associated with a Windows HANDLE and properly handles error conditions that may arise when working with file handles.

The function performs several important validations:
1. Checks for invalid handles, including the special case where stdin/stdout/stderr aren't associated with a stream (returning -2)
2. Calls the underlying GetFileType() API
3. Distinguishes between legitimate FILE_TYPE_UNKNOWN returns and error conditions by checking GetLastError()
4. Maps Windows errors to appropriate errno values using _dosmaperr()

This wrapper is essential for PostgreSQL's Windows port as it ensures consistent behavior when determining file types, which is crucial for file I/O operations and platform-specific optimizations.

## Parameters / Member Variables
- `hFile`: The Windows HANDLE for which to determine the file type. Must be a valid file handle; INVALID_HANDLE_VALUE and the special value -2 (indicating unassociated streams) are rejected with EINVAL.
## Dependencies
- Functions called/Symbols referenced:
  -  (Windows API)
  -  (Windows API)
  -  (Maps Windows errors to errno values)
- Called from (representative examples):
  -  (src/port/win32fseek.c:36)
  -  (src/port/win32fseek.c:61)
  -  (src/port/win32stat.c:267)

## Notes and Other Information
- This function is Windows-specific and part of PostgreSQL's platform abstraction layer
- The function handles the special case where standard I/O handles (stdin, stdout, stderr) return -2 when not associated with a stream, as documented in Microsoft's _get_osfhandle documentation
- Error handling distinguishes between legitimate FILE_TYPE_UNKNOWN results and actual API failures by examining GetLastError()
- Located in src/port/win32common.c:31-64, this function is part of the common Windows utilities used across PostgreSQL's Windows port
- The function sets errno appropriately on error conditions, maintaining consistency with POSIX-style error reporting