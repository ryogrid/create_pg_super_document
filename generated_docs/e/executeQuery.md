# executeQuery

## Location
src/bin/pg_dump/pg_dumpall.c: 1977 - 1999

## Overview
Executes a SQL query on a PostgreSQL connection, returning the results or terminating the program on failure.

## Definition
```c
static PGresult *executeQuery(PGconn *conn, const char *query)
```

## Detailed Description
This function is a wrapper around libpq's PQexec that provides robust error handling and logging for SQL query execution. It executes the given query on the specified database connection and expects the result to be a tuple set (PGRES_TUPLES_OK). If the query fails or doesn't return the expected result type, the function logs detailed error information and terminates the program gracefully.

The function implements a fail-fast approach - any query failure is considered fatal for the pg_dumpall operation. This design ensures data consistency and prevents partial dumps that could lead to incomplete database restores.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `query`: SQL query string to execute

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - PQexec
  - PQresultStatus
  - PGRES_TUPLES_OK
  - PQerrorMessage
  - pg_log_error
  - pg_log_error_detail
  - PQfinish
  - exit_nicely
- Called from (representative examples):
  - dropRoles
  - dumpRoles
  - dumpRoleMembership
  - dumpRoleGUCPrivs
  - dropTablespaces
  - dumpTablespaces
  - dropDBs
  - dumpUserConfig
  - expand_dbname_patterns
  - dumpDatabases
  - buildShSecLabels
  - connectDatabase

## Notes and Other Information
- This is a static function within pg_dumpall.c, indicating internal use within that module
- The function only accepts queries that return tuple sets (SELECT statements), not modification queries
- Provides comprehensive logging including both the error message and the original query for debugging
- Uses exit_nicely(1) for graceful program termination on errors
- Part of PostgreSQL's client-side utilities error handling infrastructure
- The function is also used by other PostgreSQL utilities like pg_amcheck, clusterdb, reindexdb, and vacuumdb