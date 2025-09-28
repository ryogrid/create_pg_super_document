# dlsym

## Location
[src/port/win32dlopen.c:61-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32dlopen.c#L61-L75)

## Overview
Retrieves the address of a named symbol (function or variable) from a dynamically loaded library on Windows, providing POSIX-compatible symbol lookup functionality for PostgreSQL.

## Definition
```c
void *dlsym(void *handle, const char *symbol)
```

## Detailed Description
The `dlsym` function is PostgreSQL's Windows implementation of the standard POSIX `dlsym()` function. It looks up the address of a named symbol within a previously loaded dynamic library using the Windows `GetProcAddress()` API. The function takes a library handle (obtained from `dlopen()`) and a symbol name, then returns a pointer to the symbol if found. If the symbol lookup succeeds, any previous error messages are cleared. If the lookup fails, the function captures the Windows error using `set_dl_error()` and returns NULL. This function is essential for PostgreSQL's extension system, allowing dynamic loading of functions from shared libraries.

## Parameters / Member Variables
- `handle`: Pointer to the library handle obtained from a previous `dlopen()` call, cast to `void*` but internally treated as an `HMODULE`
- `symbol`: Null-terminated string containing the name of the symbol to look up

## Dependencies
- Functions called/Symbols referenced:
  - `GetProcAddress()` (Windows API)
  - `[set_dl_error](../s/set_dl_error.md)` at Line 68
  - `symbol` parameter referenced at Lines 61, 65
- Called from (representative examples):
  - `[load_external_function](../l/load_external_function.md)` at src/backend/utils/fmgr/dfmgr.c:123
  - `[lookup_external_function](../l/lookup_external_function.md)` at src/backend/utils/fmgr/dfmgr.c:168
  - `[internal_load_library](../i/internal_load_library.md)` at src/backend/utils/fmgr/dfmgr.c:253
  - `[internal_load_library](../i/internal_load_library.md)` at src/backend/utils/fmgr/dfmgr.c:287

## Notes and Other Information
- This is a Windows-specific implementation that provides POSIX `dlsym()` compatibility
- Returns a pointer to the symbol on success, NULL on failure
- On success, clears the error buffer by setting `last_dyn_error[0] = 0`
- On failure, calls `set_dl_error()` to capture and format the Windows system error
- The returned pointer can be cast to the appropriate function pointer type for calling functions
- Symbol names must match exactly (case-sensitive) as they appear in the library's export table
- Commonly used to load PostgreSQL extension functions dynamically at runtime
- The handle parameter must be a valid `HMODULE` obtained from a successful `dlopen()` call
- This function is critical for PostgreSQL's pluggable architecture, enabling loading of user-defined functions and extensions

## Simplified Source

```c
// Simplified version of dlsym
void *dlsym(void *handle, const char *symbol) {
    // Get symbol address from Windows library
    void *ptr = GetProcAddress((HMODULE) handle, symbol);

    // Handle lookup failure
    if (!ptr) {
        set_dl_error();
        return NULL;
    }

    // Clear previous errors on success
    last_dyn_error[0] = 0;
    return ptr;
}
```

Key simplifications made:
- Preserved core Windows API call to GetProcAddress()
- Maintained essential error handling logic flow
- Kept error state management (setting and clearing errors)
- Simplified variable declarations for clarity
- Added descriptive comments for each logical step