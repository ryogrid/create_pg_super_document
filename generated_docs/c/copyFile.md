# copyFile

## Location
[src/bin/pg_upgrade/file.c:82-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/file.c#L82-L99)

## Overview
Copies a PostgreSQL relation file from a source path to a destination path during database upgrade operations, with platform-specific optimizations for Unix-like systems and Windows.

## Definition


## Detailed Description
The  function is part of PostgreSQL's pg_upgrade utility and is responsible for copying relation files during database upgrades. It provides a cross-platform implementation with different strategies for Unix-like systems and Windows:

**Unix/Linux Implementation:**
- Opens source file in read-only binary mode
- Creates destination file with exclusive creation flags to prevent overwriting existing files
- Performs chunked copying using a 50 * BLCKSZ (typically 400KB) buffer for optimal I/O performance  
- Includes comprehensive error handling with descriptive error messages that include schema and relation names
- Properly manages file descriptors and memory cleanup

**Windows Implementation:**
- Uses the Windows CopyFile API with fail-if-exists flag
- Maps Windows error codes to Unix-style errno values
- Provides equivalent error reporting with schema and relation context

The function is designed to be robust and efficient, handling large database files while providing clear diagnostic information when failures occur.

## Parameters / Member Variables
- : Source file path to copy from
- : Destination file path to copy to  
- : SQL schema name of the relation (used only for error messages)
- : SQL relation name (used only for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - xdg-open - opens a file or URL in the user's preferred application

Synopsis

xdg-open { file | URL }

xdg-open { --help | --manual | --version }

Use 'man xdg-open' or 'xdg-open --manual' for additional info. (Unix file operations)
  - / (Unix I/O operations)
  -  (Unix file descriptor cleanup)
  - / (PostgreSQL memory management)
  -  (PostgreSQL error reporting)
  -  (Windows file copying API)
  -  (Windows error mapping)
  -  (Windows error retrieval)
  -  (PostgreSQL binary file mode constant)
  -  (PostgreSQL file creation permissions)

- Called from (representative examples):
  -  (in relfilenumber.c:253 during TRANSFER_MODE_COPY operations)

## Notes and Other Information
- Part of the pg_upgrade utility's file transfer subsystem alongside , , and 
- Uses a large buffer size (50 * BLCKSZ) for efficient copying of potentially large database relation files
- Includes fail-safe behavior by using O_EXCL flag to prevent accidental overwrites
- Error messages include both file paths and SQL object names to aid in troubleshooting upgrade issues
- The function signature is declared in pg_upgrade.h:406 for use throughout the pg_upgrade codebase
- Windows implementation provides equivalent functionality but uses native Windows APIs for better performance and compatibility