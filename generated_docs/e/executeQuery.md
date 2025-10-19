# executeQuery

## Location
[src/bin/pg_dump/pg_dumpall.c:1977-1999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1977-L1999)

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
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - PGRES_TUPLES_OK
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - pg_log_error
  - pg_log_error_detail
  - [PQfinish](../P/PQfinish.md)
  - [exit_nicely](exit_nicely.md)
- Called from (representative examples):
  - [dropRoles](../d/dropRoles.md)
  - [dumpRoles](../d/dumpRoles.md)
  - [dumpRoleMembership](../d/dumpRoleMembership.md)
  - [dumpRoleGUCPrivs](../d/dumpRoleGUCPrivs.md)
  - [dropTablespaces](../d/dropTablespaces.md)
  - [dumpTablespaces](../d/dumpTablespaces.md)
  - [dropDBs](../d/dropDBs.md)
  - [dumpUserConfig](../d/dumpUserConfig.md)
  - [expand_dbname_patterns](expand_dbname_patterns.md)
  - [dumpDatabases](../d/dumpDatabases.md)
  - [buildShSecLabels](../b/buildShSecLabels.md)
  - [connectDatabase](../c/connectDatabase.md)

## Notes and Other Information
- This is a static function within pg_dumpall.c, indicating internal use within that module
- The function only accepts queries that return tuple sets (SELECT statements), not modification queries
- Provides comprehensive logging including both the error message and the original query for debugging
- Uses exit_nicely(1) for graceful program termination on errors
- Part of PostgreSQL's client-side utilities error handling infrastructure
- The function is also used by other PostgreSQL utilities like pg_amcheck, clusterdb, reindexdb, and vacuumdb

## Simplified Source

```c
static PGresult *executeQuery(PGconn *conn, const char *query)
{
    PGresult *res;

    // Log the query being executed
    pg_log_info("executing %s", query);

    // Execute the query
    res = PQexec(conn, query);

    // Check for execution failure or unexpected result type
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        // Log detailed error information
        pg_log_error("query failed: %s", PQerrorMessage(conn));
        pg_log_error_detail("Query was: %s", query);

        // Clean up connection and exit
        PQfinish(conn);
        exit_nicely(1);
    }

    return res;
}
```