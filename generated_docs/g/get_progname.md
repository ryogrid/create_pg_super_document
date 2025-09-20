# get_progname

## Location
[src/port/path.c:651-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L651-L688)

## Overview
Extracts the actual name of the program as called, stripped of .exe suffix if any, and handles platform-specific path separators and drive letters.

## Definition

```c
const char *
get_progname(const char *argv0)
```
## Detailed Description
The  function takes the first command-line argument (typically ) and extracts a clean program name from it. It removes directory path components and handles platform-specific considerations like Windows drive letters and  suffixes. The function allocates memory for the result using , which is intentionally leaked since this function is typically called only once during program initialization.

The function performs the following operations:
1. Strips directory path components using 
2. Handles drive letters on Windows platforms using 
3. Creates a copy of the resulting string to avoid issues with  modifications
4. On Windows and Cygwin, removes the  suffix (case-insensitive)

## Parameters / Member Variables
- : The first command-line argument, typically containing the program path as invoked

## Dependencies
- Functions called/Symbols referenced:
  -  - Finds the last directory separator in a path
  -  - Skips Windows drive letter prefix
  -  - Standard C library function to duplicate strings
  -  - PostgreSQL's case-insensitive string comparison
  -  - Constant defining the executable suffix

- Called from (representative examples):
  -  functions across numerous PostgreSQL utilities and binaries
  -  in logging infrastructure
  -  in option processing utilities

## Notes and Other Information
- The function intentionally leaks memory since it's called only once during program startup
- Handles cross-platform differences between Unix-like systems and Windows
- Critical for proper program identification in logging and error messages
- Used extensively throughout PostgreSQL's command-line utilities for consistent program name handling
- The memory allocation failure results in an abort() call, which could terminate the postmaster process