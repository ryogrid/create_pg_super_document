# pgsql_version

## Location
src/backend/utils/adt/version.c: 21 - 24

## Overview
The `pgsql_version` function is a PostgreSQL built-in function that returns the complete PostgreSQL version string as text, providing version information for the currently running PostgreSQL server instance.

## Definition
```c
Datum pgsql_version(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pgsql_version` function serves as the backend implementation for PostgreSQL's SQL `version()` function. It is a simple utility function that retrieves and returns the PostgreSQL version string stored in the `PG_VERSION_STR` macro. This function is registered in the system catalog (`pg_proc.dat`) with OID 89 and is accessible to SQL users through the `version()` function call.

The function follows PostgreSQL's standard function interface pattern, accepting `PG_FUNCTION_ARGS` (which contains no actual arguments in this case) and returning a `Datum`. It uses the PostgreSQL version string compilation constant and converts it to a PostgreSQL text data type for return to the SQL layer.

The function is marked as 'stable' (`provolatile => 's'`) in the catalog, meaning its result can change between different PostgreSQL server sessions but remains constant within a single session.

## Parameters / Member Variables
- This function takes no parameters (empty argument list in SQL: `version()`)

## Dependencies
- Functions called/Symbols referenced:
  - `cstring_to_text`: Converts a C string to PostgreSQL text type
  - `PG_RETURN_TEXT_P`: PostgreSQL macro for returning text data
  - `PG_VERSION_STR`: Compile-time constant containing the version string

- Called from (representative examples):
  - SQL `version()` function calls
  - System catalog registration in `pg_proc.dat` (OID 89)

## Notes and Other Information
- The function is registered in the PostgreSQL system catalog as the `version` SQL function
- It returns the same version string that appears in server logs and various PostgreSQL utilities
- The version string typically includes the major version, minor version, and build information
- This is a read-only function with no side effects
- Located in `src/backend/utils/adt/version.c:21-24`
- Part of PostgreSQL's abstract data type (ADT) utilities for basic system information functions