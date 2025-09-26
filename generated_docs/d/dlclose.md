# dlclose

## Location
src/port/win32dlopen.c: 49 - 60

## Overview
Closes a dynamically loaded library handle on Windows, providing POSIX-compatible dynamic library unloading functionality for PostgreSQL.

## Definition
```c
int dlclose(void *handle)
```

## Detailed Description
The `dlclose` function is PostgreSQL's Windows implementation of the standard POSIX `dlclose()` function. It unloads a previously loaded dynamic library by calling the Windows `FreeLibrary()` API. If the unloading operation succeeds, the function clears any previous error messages and returns 0. If the operation fails, it captures the Windows error using `set_dl_error()` and returns 1 to indicate failure. This function is typically used when cleaning up dynamically loaded PostgreSQL extensions or other shared libraries.

## Parameters / Member Variables
- `handle`: Pointer to the library handle obtained from a previous `dlopen()` call, cast to `void*` but internally treated as an `HMODULE`

## Dependencies
- Functions called/Symbols referenced:
  - `FreeLibrary()` (Windows API)
  - `set_dl_error` at Line 53
- Called from (representative examples):
  - `internal_load_library` at src/backend/utils/fmgr/dfmgr.c:265
  - `internal_load_library` at src/backend/utils/fmgr/dfmgr.c:275

## Notes and Other Information
- This is a Windows-specific implementation that provides POSIX `dlclose()` compatibility
- Returns 0 on success, 1 on failure (opposite of many PostgreSQL functions that return 0 on failure)
- On success, clears the error buffer by setting `last_dyn_error[0] = 0`
- On failure, calls `set_dl_error()` to capture and format the Windows system error
- The handle parameter should be a valid `HMODULE` obtained from a previous `dlopen()` call
- Attempting to close an invalid handle will result in an error being set and a return value of 1
- This function is part of PostgreSQL's portable dynamic loading interface that allows the same code to work on both Unix-like systems and Windows