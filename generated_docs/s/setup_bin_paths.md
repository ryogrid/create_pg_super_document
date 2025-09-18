# setup_bin_paths

## Location
[src/bin/initdb/initdb.c:2626-2662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2626-L2662)

## Overview
Locates and validates PostgreSQL binary and share directory paths during initialization, ensuring the required postgres backend executable is available and version-compatible.

## Definition
void setup_bin_paths(const char *argv0)

## Detailed Description
This function establishes the paths to essential PostgreSQL directories during initdb execution. It performs several critical tasks: locating the postgres backend executable relative to the initdb binary, verifying version compatibility between initdb and postgres, determining the binary directory path, and establishing the share directory path for data files and templates.

The function implements robust error handling and provides detailed diagnostic messages when required components are missing or incompatible. It ensures that all path information is canonicalized for consistent usage throughout the initialization process. The function also handles both default share path detection and validation of explicitly provided share paths.

## Parameters / Member Variables
- `argv0`: The program name or path used to invoke initdb, used as a reference point for locating other PostgreSQL executables

## Dependencies
- Functions called/Symbols referenced:
  - find_other_exec (PostgreSQL utility for locating executables)
  - find_my_exec (PostgreSQL utility for self-location)
  - strlcpy (Safe string copy utility)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error function)
  - strcpy (C standard library)
  - [last_dir_separator](../l/last_dir_separator.md) (PostgreSQL path utility)
  - [canonicalize_path](../c/canonicalize_path.md) (PostgreSQL path utility)
  - pg_malloc (PostgreSQL memory allocation)
  - [get_share_path](../g/get_share_path.md) (PostgreSQL utility for locating share directory)
  - is_absolute_path (PostgreSQL path validation utility)
- Global constants/variables referenced:
  - PG_BACKEND_VERSIONSTR (Version string for compatibility checking)
  - backend_exec (Global variable to store postgres executable path)
  - bin_path (Global variable to store binary directory path)
  - share_path (Global variable to store share directory path)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/initdb/initdb.c:3449)

## Notes and Other Information
- The function terminates the program with detailed error messages if the postgres executable cannot be found or is version-incompatible
- Both binary and share paths are canonicalized to ensure consistent path handling across platforms
- The function supports both automatic share path detection and explicit share path specification
- When an explicit share path is provided, it must be an absolute path
- Error messages include the full paths to help users diagnose installation or configuration issues
- The postgres executable version must exactly match the initdb version to prevent compatibility issues