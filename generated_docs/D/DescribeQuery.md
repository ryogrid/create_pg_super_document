# DescribeQuery

## Location
[src/bin/psql/common.c:1314-1445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1314-L1445)

## Overview
DescribeQuery describes the result columns of a query without executing it, using PostgreSQL's prepared statement mechanism to parse and analyze the query structure.

## Definition
static bool DescribeQuery(const char *query, double *elapsed_msec)

## Detailed Description
DescribeQuery implements the functionality behind psql's \gdesc command, allowing users to examine the structure of query results without actually executing the query. The function operates by:

1. **Query Preparation**: Uses PQprepare() with an unnamed prepared statement to parse the query without execution
2. **Result Description**: Calls PQdescribePrepared() to obtain metadata about the query's result columns
3. **Metadata Formatting**: Constructs and executes a synthetic query to display column names and types in a user-friendly format
4. **Timing Support**: Records execution time when timing is enabled, though this excludes actual query execution time

The function constructs a formatted output showing column names and their PostgreSQL data types using pg_catalog.format_type() for proper type display. If the query has no result columns, it displays an appropriate message.

## Parameters / Member Variables
- `query`: The SQL query string to be described
- `elapsed_msec`: Output parameter to store the elapsed time for the describe operation

## Dependencies
- Functions called/Symbols referenced:
  - [PQprepare](../P/PQprepare.md)
  - [PQdescribePrepared](../P/PQdescribePrepared.md)
  - [PQexec](../P/PQexec.md)
  - [AcceptResult](../A/AcceptResult.md)
  - [PrintQueryResult](../P/PrintQueryResult.md)
  - [SetResultVariables](../S/SetResultVariables.md)
  - [ClearOrSaveResult](../C/ClearOrSaveResult.md)
  - [PQnfields](../P/PQnfields.md), PQfname, PQftype, PQfmod
  - [PQescapeLiteral](../P/PQescapeLiteral.md), PQfreemem
  - [initPQExpBuffer](../i/initPQExpBuffer.md), printfPQExpBuffer, termPQExpBuffer
- Called from (representative examples):
  - [SendQuery](../S/SendQuery.md) (when pset.gdesc_flag is true)

## Notes and Other Information
- Returns true if the operation executed successfully, false otherwise
- Uses unnamed prepared statements which are invisible to psql users and automatically overwritten by subsequent operations
- Handles timing measurement separately from query execution timing
- Constructs a synthetic VALUES clause query to format the column information nicely
- Properly escapes column names using PQescapeLiteral to prevent SQL injection
- Uses gettext_noop for internationalization of column headers ("Column" and "Type")
- Function is static, only accessible within common.c