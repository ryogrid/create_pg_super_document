# setup_pgdata

## Location
[src/bin/initdb/initdb.c:2589-2625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2589-L2625)

## Overview
Establishes and validates the PostgreSQL data directory path during database initialization, either from command-line options or the PGDATA environment variable.

## Definition
void setup_pgdata(void)

## Detailed Description
This function is responsible for determining and setting up the PostgreSQL data directory path during initdb execution. It implements a hierarchical approach to locate the data directory: first checking if a path was explicitly provided via command-line options, then falling back to the PGDATA environment variable. Once a valid path is identified, the function canonicalizes it to resolve any relative paths or symbolic links, stores both the original and canonical versions, and sets the PGDATA environment variable for subsequent processes.

The function includes comprehensive error handling and user guidance when no data directory can be determined. It also handles platform-specific considerations, particularly Windows path quoting issues, by ensuring the PGDATA environment variable is properly set for child processes.

## Parameters / Member Variables
- No parameters (void function)
- Uses global variables:
  - `pg_data`: Stores the canonical data directory path
  - `pgdata_native`: Stores the original (non-canonicalized) data directory path

## Dependencies
- Functions called/Symbols referenced:
  - getenv (C standard library)
  - strlen (C standard library)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL memory allocation utility)
  - pg_log_error (PostgreSQL logging function)
  - pg_log_error_hint (PostgreSQL logging function with hints)
  - [canonicalize_path](../c/canonicalize_path.md) (PostgreSQL path utility)
  - setenv (POSIX environment function)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error function)
  - exit (C standard library)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/initdb/initdb.c:3418, 3447)

## Notes and Other Information
- The function terminates the program (exit(1)) if no data directory can be determined
- Both original and canonical paths are preserved to handle different use cases
- The PGDATA environment variable is explicitly set to avoid command-line quoting issues on Windows
- The function provides helpful error messages guiding users to use either the -D option or PGDATA environment variable
- [Path](../P/Path.md) canonicalization ensures consistent handling of relative paths, symbolic links, and path separators across platforms