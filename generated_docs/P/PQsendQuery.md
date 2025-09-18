# PQsendQuery

## Location
src/interfaces/libpq/fe-exec.c: 1416 - 1421

## Overview
Submits a SQL query to the PostgreSQL server asynchronously without waiting for completion, allowing the client to continue other operations while the query executes.

## Definition
```c
int PQsendQuery(PGconn *conn, const char *query)
```

## Detailed Description
PQsendQuery is the primary asynchronous query submission function in libpq. It sends a SQL query to the PostgreSQL server and returns immediately without waiting for the query to complete. This enables non-blocking query execution, allowing applications to perform other operations while waiting for query results.

The function is essentially a wrapper around PQsendQueryInternal, passing `true` for the error message reset parameter. This means that any previous error messages in the connection object are cleared before sending the new query, providing a clean state for error reporting.

After calling PQsendQuery, applications typically use PQgetResult() to retrieve results when they become available, or use connection polling mechanisms to determine when results are ready.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object
- `query`: C string containing the SQL query to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQueryInternal](PQsendQueryInternal.md) (internal implementation function)
- Called from (representative examples):
  - [libpqrcv_PQexec](../l/libpqrcv_PQexec.md) (replication)
  - [run_command](../r/run_command.md) (pg_amcheck)
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup)
  - [sendCommand](../s/sendCommand.md) (pgbench)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (psql)
  - [PQexec](PQexec.md) (synchronous wrapper)
  - [run_permutation](../r/run_permutation.md) (isolation tester)

## Notes and Other Information
- Returns 1 on successful submission, 0 on error
- This is an exported function, part of libpq's public API
- Resets conn->errorMessage before processing (unlike PQsendQueryContinue)
- Essential for building responsive PostgreSQL client applications
- Often used in combination with PQgetResult() for result retrieval
- Supports PostgreSQL's asynchronous processing model
- The query parameter must be a null-terminated C string
- Does not support parameterized queries (use PQsendQueryParams for that)
- Part of the foundation for more complex features like query pipelining