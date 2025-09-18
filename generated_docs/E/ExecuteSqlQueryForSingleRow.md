# ExecuteSqlQueryForSingleRow

## Location
[src/bin/pg_dump/pg_backup_db.c:305-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L305-L327)

## Overview
A specialized utility function in pg_dump that executes SQL queries expected to return exactly one row, with automatic validation and error reporting.

## Definition
```c
PGresult *ExecuteSqlQueryForSingleRow(Archive *fout, const char *query)
```

## Detailed Description
The `ExecuteSqlQueryForSingleRow` function is a higher-level wrapper around `ExecuteSqlQuery` designed specifically for queries that must return exactly one row of data. This function is commonly used in pg_dump when retrieving specific configuration values, metadata, or performing lookups where exactly one result is expected.

The function first calls `ExecuteSqlQuery` with `PGRES_TUPLES_OK` as the expected status, then validates that the result contains exactly one row using `PQntuples()`. If the query returns zero rows or more than one row, the function calls `pg_fatal()` with an appropriate internationalized error message that handles both singular and plural cases correctly.

This function provides a convenient abstraction for the common pattern of "execute query and expect exactly one result," eliminating the need for callers to perform row count validation manually and ensuring consistent error reporting across the pg_dump codebase.

## Parameters / Member Variables
- `fout`: A pointer to an Archive structure that contains the database connection and other pg_dump context information
- `query`: A null-terminated string containing the SQL query to execute (expected to return exactly one row)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [pg_fatal](../p/pg_fatal.md)
  - ngettext
  - PGRES_TUPLES_OK (constant)

- Called from (representative examples):
  - [_check_database_version](../c/_check_database_version.md)
  - [ConnectDatabase](../C/ConnectDatabase.md)
  - [setup_connection](../s/setup_connection.md)
  - [get_synchronized_snapshot](../g/get_synchronized_snapshot.md)
  - [dumpDatabase](../d/dumpDatabase.md)
  - [get_next_possible_free_pg_type_oid](../g/get_next_possible_free_pg_type_oid.md)
  - [dumpFunc](../d/dumpFunc.md)

## Notes and Other Information
- This function is part of the public API for pg_dump modules, declared in pg_backup_db.h
- Like ExecuteSqlQuery, this function does NOT automatically clean up the PGresult - the caller must call PQclear()
- The function uses pg_fatal() for error reporting, which terminates the program immediately upon validation failure
- The error message uses ngettext() for proper internationalization, providing different messages for singular vs. plural row counts
- This function is particularly useful for metadata queries, configuration lookups, and OID retrievals where exactly one result is semantically required
- The function internally uses ExecuteSqlQuery, inheriting its error handling for connection and execution failures
- Commonly used in binary upgrade scenarios and when retrieving specific database object properties