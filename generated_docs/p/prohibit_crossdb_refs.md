# prohibit_crossdb_refs

## Location
[src/bin/pg_dump/pg_dump.c:1709-1733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1709-L1733)

## Overview
Verifies that the connected database name matches a given database name and terminates the program with an error if they don't match, preventing cross-database references in pg_dump operations.

## Definition

```c
static void
prohibit_crossdb_refs(PGconn *conn, const char *dbname, const char *pattern)
```
## Detailed Description
This function is a safety mechanism in pg_dump that prevents cross-database references, which are not supported in PostgreSQL. It compares the name of the currently connected database (obtained via PQdb()) with a provided database name that was parsed from a user pattern. If the names don't match, it terminates the program with a fatal error message indicating that cross-database references are not implemented.

The function serves as a validation step when processing database object patterns that might contain explicit database qualifiers, ensuring that users don't attempt to reference objects from different databases than the one they're currently connected to.

## Parameters / Member Variables
- `*conn`: PGconn pointer to the current database connection
- `*dbname`: The database name parsed from a user-provided pattern that should match the connected database
- `*pattern`: The original pattern string provided by the user, used in error messages for context
## Dependencies
- Functions called/Symbols referenced:
  - [PQdb](../P/PQdb.md) (libpq function to get database name from connection)
  - [pg_fatal](pg_fatal.md) (error reporting function)
  - strcmp (standard C string comparison)
- Called from (representative examples):
  - fmtQualifiedDumpable
  - [expand_schema_name_patterns](../e/expand_schema_name_patterns.md)  
  - [expand_table_name_patterns](../e/expand_table_name_patterns.md)

## Notes and Other Information
- This is a static function within pg_dump.c, indicating it's only used internally within that module
- The function will terminate the program immediately if a cross-database reference is detected
- PostgreSQL does not support cross-database queries or references, making this validation necessary
- The error message includes the original pattern to help users understand what caused the problem