# get_exec_path

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:341-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L341-L376)

## Overview
Verifies that a PostgreSQL binary is available in the same directory as pg_createsubscriber and ensures it has the same version, returning the absolute path of the program.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The  function is a utility function used by pg_createsubscriber to locate and validate companion PostgreSQL utilities. It performs two critical checks: first, it verifies that the requested program exists in the same directory as pg_createsubscriber itself, and second, it ensures that the found program has the same PostgreSQL version. This version compatibility check is essential for ensuring that all tools used during the subscription creation process are compatible.

The function uses PostgreSQL's standard utility location functions ( and ) to perform the search and validation. If the program is not found or has a version mismatch, the function terminates the process with a fatal error message.

## Parameters
- : The path to the current executable (pg_createsubscriber), used as a reference point to locate other binaries
- : The name of the PostgreSQL program to locate and validate (e.g., "pg_dump", "psql")

## Dependencies
- Functions called/Symbols referenced:
  -  - Allocates memory for the executable path
  -  - Locates and validates the requested program
  -  - Finds the current executable's path for error reporting
  -  - Safe string copying for error path handling
  -  - Logs the found executable path for debugging
- Called from:
  -  structure initialization
  -  function (twice for different utilities)

## Notes and Other Information
- The function is marked as , indicating it's only used within the pg_createsubscriber.c file
- Memory allocated for the executable path should be freed by the caller
- The function will terminate the entire process if the required program is not found or has incompatible version
- Debug logging is enabled to help troubleshoot path resolution issues
- The version string format follows PostgreSQL's standard: "progname (PostgreSQL) version"