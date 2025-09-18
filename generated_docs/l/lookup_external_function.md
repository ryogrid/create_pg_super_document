# lookup_external_function

## Location
[src/backend/utils/fmgr/dfmgr.c:166-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L166-L183)

## Overview
This function looks up a named function within an already loaded library using a file handle, providing an efficient way to retrieve multiple functions from the same shared library.

## Definition
```c
void *lookup_external_function(void *filehandle, const char *funcname)
```

## Detailed Description
The `lookup_external_function` function provides an efficient mechanism for looking up functions in shared libraries that have already been loaded. Unlike `load_external_function`, this function requires a pre-existing file handle to the loaded library, making it ideal for scenarios where multiple functions need to be retrieved from the same library. It directly uses the system's `dlsym()` function to perform the symbol lookup and returns NULL if the function is not found, without raising any errors.

This function is particularly useful for optimizing performance when working with libraries that contain multiple functions, as it avoids the overhead of repeatedly loading and expanding library names.

## Parameters / Member Variables
- `filehandle`: A handle to an already loaded library file (typically obtained from load_external_function)
- `funcname`: The name of the function to look up within the loaded library

## Dependencies
- Functions called/Symbols referenced:
  - dlsym
- Called from (representative examples):
  - [fetch_finfo_record](../f/fetch_finfo_record.md)

## Notes and Other Information
- This function is part of PostgreSQL's dynamic function management system located in src/backend/utils/fmgr/dfmgr.c
- Returns NULL if the function is not found, without raising any errors (unlike load_external_function which can optionally signal errors)
- More efficient than load_external_function for multiple function lookups from the same library since it bypasses filename expansion and library loading
- The filehandle parameter is typically obtained from a previous call to load_external_function
- Direct wrapper around the POSIX dlsym() system call
- Used primarily in the function manager (fmgr) system for efficient function caching and retrieval