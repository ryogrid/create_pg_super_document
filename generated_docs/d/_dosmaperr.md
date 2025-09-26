# _dosmaperr

## Location
src/port/win32error.c: 177 - 214

## Overview
Maps Windows API error codes to equivalent POSIX errno values, providing cross-platform compatibility for error handling in PostgreSQL on Windows systems.

## Definition
```c
void _dosmaperr(unsigned long e)
```
Located in: src/port/win32error.c:176-214

## Detailed Description
The `_dosmaperr` function translates Windows-specific error codes (DWORD values returned by GetLastError()) to standard POSIX errno values. This translation layer allows PostgreSQL code to use standard errno-based error handling mechanisms regardless of the underlying platform.

The function works by:
1. Setting errno to 0 if the input error code is 0 (no error)
2. Iterating through a predefined mapping table (`doserrors`) to find the corresponding POSIX errno value
3. Setting errno to the mapped value and optionally logging the translation
4. If no mapping is found, setting errno to EINVAL and logging an error message

The function includes conditional compilation directives to handle different build contexts (backend vs frontend) and debug modes.

## Parameters / Member Variables
- `e`: The Windows error code (unsigned long) to be mapped to a POSIX errno value

## Dependencies
- Functions called/Symbols referenced:
  - `lengthof`: Macro to get the length of the `doserrors` array
  - `ereport`: PostgreSQL error reporting function (backend builds only)
  - `errmsg_internal`: Internal error message formatting function
  - `fprintf`: Standard C library function for frontend debug output
  - `errno`: Global error number variable

- Called from (representative examples):
  - `dsm_impl.c`: Dynamic shared memory implementation functions
  - `fd.c`: File descriptor management functions
  - `syslogger.c`: System logging functions
  - `collationcmds.c`: Collation command functions
  - `win32common.c`: Common Windows utility functions
  - `dirent.c`: Directory entry functions
  - `dirmod.c`: Directory modification functions
  - `kill.c`: Process termination functions
  - `win32stat.c`: File status functions
  - `open.c`: File opening functions
  - Various other Windows-specific port layer functions

## Notes and Other Information
- This function is Windows-specific and is only compiled on Windows builds
- The mapping table contains 48 common Windows error codes and their POSIX equivalents
- Debug logging behavior differs between backend and frontend builds:
  - Backend builds use `ereport(DEBUG5, ...)` for successful mappings and `ereport(LOG, ...)` for unmapped codes
  - Frontend builds with `FRONTEND_DEBUG` use `fprintf(stderr, ...)` for output
- Unrecognized error codes are mapped to `EINVAL` as a fallback
- The function is declared in `src/include/port/win32_port.h` for use across the PostgreSQL codebase
- Commonly used in conjunction with `GetLastError()` to translate Windows API errors immediately after they occur
- Essential for maintaining consistent error handling behavior across different operating systems in PostgreSQL