# win32_socket_strerror

## Location
src/port/strerror.c: 275 - 310

## Overview
Windows-specific function that handles Winsock error codes, providing human-readable error messages for network-related errors on Windows platforms.

## Definition
```c
static char *win32_socket_strerror(int errnum, char *buf, size_t buflen)
```

## Detailed Description
This function addresses a limitation in Windows' standard strerror() function, which doesn't recognize Winsock error codes (10000-11999 range). It dynamically loads the netmsg.dll library and uses Windows' FormatMessage API to retrieve localized error messages for socket-related errors. The function implements lazy loading of the DLL and provides fallback error messages when the system cannot translate the error code.

## Parameters / Member Variables
- `errnum`: The Winsock error number to convert to a descriptive string
- `buf`: Buffer to store the error message string  
- `buflen`: Size of the provided buffer

## Dependencies
- Functions called/Symbols referenced:
  - Windows API functions: LoadLibraryEx, FormatMessage, GetLastError, ZeroMemory
  - Standard functions: snprintf
  - Windows constants: INVALID_HANDLE_VALUE, DONT_RESOLVE_DLL_REFERENCES, LOAD_LIBRARY_AS_DATAFILE, FORMAT_MESSAGE_*, MAKELANGID, LANG_ENGLISH, SUBLANG_DEFAULT
- Called from (representative examples):
  - [pg_strerror_r](../p/pg_strerror_r.md) (when handling Winsock errors on Windows)
  - strerror_r (alias reference at src/port/strerror.c:27)

## Notes and Other Information
- Static function - internal to strerror.c module
- Windows-only functionality, compiled conditionally for WIN32 platforms
- Uses lazy loading pattern for netmsg.dll to avoid unnecessary library loads
- Handles DLL loading failure gracefully with informative error messages
- Uses FormatMessage with English locale (LANG_ENGLISH, SUBLANG_DEFAULT) for consistent output
- Provides fallback message "unrecognized winsock error [number]" when FormatMessage fails
- Specifically handles Winsock error code range (10000-11999) as referenced in WinError.h
- Uses ZeroMemory to ensure clean buffer initialization
- Located in src/port/strerror.c:275-310