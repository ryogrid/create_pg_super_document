# dlopen

## Location
src/port/win32dlopen.c: 76 - 93

## Overview
Opens and loads a dynamic library on Windows, providing POSIX-compatible dynamic library loading functionality for PostgreSQL with error suppression during loading.

## Definition
```c
void *dlopen(const char *file, int mode)
```

## Detailed Description
The `dlopen` function is PostgreSQL's Windows implementation of the standard POSIX `dlopen()` function. It loads a dynamic library (DLL) from the specified file path using the Windows `LoadLibrary()` API. The function temporarily suppresses popup error dialogs that Windows might show during library loading failures by setting the error mode to `SEM_FAILCRITICALERRORS | SEM_NOOPENFILEERRORBOX`. If the library loads successfully, any previous error messages are cleared and the function returns the library handle. If loading fails, the function captures the Windows error using `set_dl_error()` and returns NULL. This function is fundamental to PostgreSQL's extension system, enabling dynamic loading of shared libraries at runtime.

## Parameters / Member Variables
- `file`: Null-terminated string containing the path to the dynamic library file to load
- `mode`: Loading mode flags (ignored in this Windows implementation, provided for POSIX compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - `SetErrorMode()` (Windows API)
  - `LoadLibrary()` (Windows API)
  - `set_dl_error` at Line 88
- Called from (representative examples):
  - `internal_load_library` at src/backend/utils/fmgr/dfmgr.c:239

## Notes and Other Information
- This is a Windows-specific implementation that provides POSIX `dlopen()` compatibility
- Returns a handle (cast to `void*`) on success, NULL on failure
- The `mode` parameter is ignored since Windows doesn't have equivalent loading modes
- Temporarily disables Windows error popup dialogs during loading to provide consistent behavior
- On success, clears the error buffer by setting `last_dyn_error[0] = 0`
- On failure, calls `set_dl_error()` to capture and format the Windows system error
- The returned handle can be used with `dlsym()` to look up symbols and `dlclose()` to unload the library
- Error suppression flags: `SEM_FAILCRITICALERRORS` prevents critical error dialogs, `SEM_NOOPENFILEERRORBOX` prevents file-not-found dialogs
- Essential for PostgreSQL's pluggable architecture, enabling loading of extensions and user-defined functions
- The file path can be absolute or relative, and Windows will search standard locations if the file is not found in the specified path