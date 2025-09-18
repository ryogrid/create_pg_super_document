# load_libraries

## Location
src/backend/utils/init/miscinit.c: 1846 - 1897

## Overview
load_libraries is a static function that loads shared libraries from a comma-separated list, with optional restriction to the plugins directory for security purposes.

## Definition
```c
static void load_libraries(const char *libraries, const char *gucname, bool restricted)
```

## Detailed Description
This internal function serves as the core implementation for loading PostgreSQL extension libraries. It parses a comma-separated list of library names and loads each one using the load_file function. The function supports both unrestricted loading (for trusted shared_preload_libraries) and restricted loading (for session_preload_libraries) where libraries must be located in the $libdir/plugins/ directory for security.

The function performs the following operations:
1. Validates the input string and returns early if empty
2. Creates a modifiable copy of the libraries string
3. Parses the string into individual library paths using SplitDirectoriesString
4. Iterates through each library path
5. For restricted mode, prepends $libdir/plugins/ to relative paths
6. Calls load_file to actually load each library
7. Logs successful library loads at DEBUG1 level
8. Cleans up allocated memory

## Parameters / Member Variables
- `libraries`: Comma-separated string of library names/paths to load
- `gucname`: Name of the GUC variable being processed (used for error reporting)
- `restricted`: Boolean flag indicating whether to restrict libraries to the plugins directory

## Dependencies
- Functions called/Symbols referenced:
  - pstrdup
  - SplitDirectoriesString
  - list_free_deep
  - pfree
  - ereport
  - errcode
  - errmsg
  - lfirst
  - first_dir_separator
  - psprintf
  - load_file
  - errmsg_internal
- Called from (representative examples):
  - process_shared_preload_libraries
  - process_session_preload_libraries

## Notes and Other Information
- This is a static function, so it's only accessible within the miscinit.c file
- The restricted parameter provides a security mechanism to prevent session-level library loading from arbitrary filesystem locations
- Memory management is carefully handled with proper cleanup of temporary allocations
- Syntax errors in the library list are logged but do not cause fatal errors
- The function uses PostgreSQL's memory management functions (pstrdup, pfree) rather than standard C library functions
- Debug logging helps administrators track which libraries have been successfully loaded