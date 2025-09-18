# expand_dynamic_library_name

## Location
[src/backend/utils/fmgr/dfmgr.c:414-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L414-L468)

## Overview
Expands a dynamic library name to its full path by searching the dynamic library path and handling platform-specific extensions.

## Definition


## Detailed Description
This function attempts to locate a dynamic library by expanding the given library name to its full path. It implements a sophisticated search strategy:

1. First checks if the name contains a directory separator (slash)
2. If no slash is present, searches for the library in the dynamic library path using 
3. If the library has a slash, attempts to substitute libpath macros using  and checks if the resulting file exists
4. If the initial search fails, appends the platform-specific dynamic library suffix (DLSUFFIX) and repeats the search process
5. If all searches fail, returns the original name as-is, allowing the subsequent load attempt to fail with an appropriate error message

The function ensures that the result is always freshly allocated memory using .

## Parameters / Member Variables
- : The library name to expand, which may be a simple name or include path components

## Dependencies
- Functions called/Symbols referenced:
  -  - checks for directory separators in the name
  -  - searches for the library in the dynamic library search path
  -  - expands macros in library paths
  -  - verifies file existence
  -  - creates formatted strings with memory allocation
  -  - duplicates strings with memory allocation
  -  - frees allocated memory
- Called from:
  -  (src/backend/utils/fmgr/dfmgr.c:113)
  -  (src/backend/utils/fmgr/dfmgr.c:153)

## Notes and Other Information
- The function is static, meaning it's only accessible within the dfmgr.c compilation unit
- Uses PostgreSQL's memory management functions (palloc/pfree) for consistent memory handling
- DLSUFFIX is a platform-specific macro that represents the standard dynamic library extension (.so on Linux, .dll on Windows, etc.)
- The function gracefully handles failure by returning the original name, allowing higher-level functions to generate appropriate error messages
- Part of PostgreSQL's dynamic function manager (dfmgr) subsystem responsible for loading external C functions and libraries