# substitute_libpath_macro

## Location
[src/backend/utils/fmgr/dfmgr.c:484-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L484-L514)

## Overview
Substitutes macro placeholders in library paths with their actual system paths, specifically handling the $libdir macro.

## Definition

```c
static char *
substitute_libpath_macro(const char *name)
```
## Detailed Description
This function processes library path names that contain macro placeholders and expands them to their actual system paths. Currently, it recognizes and handles only the "$libdir" macro, which represents PostgreSQL's package library directory path.

The function works as follows:
1. Checks if the path starts with a '$' character (macro indicator)
2. If no '$' is found, returns a duplicate of the original string unchanged
3. Locates the first directory separator or end of string to identify the macro boundary
4. Validates that the macro is exactly "$libdir" - no other macros are supported
5. If validation fails, raises an ERROR with 
6. If successful, replaces "$libdir" with the actual  and concatenates any remaining path components

The result is always a freshly allocated string using PostgreSQL's memory management functions.

## Parameters / Member Variables
- : The library path string that may contain macro placeholders to be substituted

## Dependencies
- Functions called/Symbols referenced:
  -  - locates the first directory separator in the path
  -  - duplicates strings with memory allocation
  -  - calculates string length
  -  - compares string prefixes
  -  - PostgreSQL's error reporting mechanism
  -  - creates formatted strings with memory allocation
  -  - global variable containing PostgreSQL's package library directory
- Called from:
  -  (src/backend/utils/fmgr/dfmgr.c:432, 449)
  -  (src/backend/utils/fmgr/dfmgr.c:551)

## Notes and Other Information
- The function is static, limiting its scope to the dfmgr.c compilation unit
- Only recognizes the "$libdir" macro; other macro names will trigger an error
- The $libdir macro typically expands to the directory containing PostgreSQL's shared libraries
- Uses PostgreSQL's memory management (palloc/pfree) for consistent memory handling
- Part of PostgreSQL's dynamic library loading system, enabling portable library path specifications
- The macro substitution allows library paths to be installation-independent