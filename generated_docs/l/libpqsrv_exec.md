# libpqsrv_exec

## Location
[src/include/libpq/libpq-be-fe-helpers.h:256-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L256-L268)

## Overview
A PQexec() wrapper function that processes interrupts while executing SQL queries, providing a safer alternative for server-side execution.

## Definition

```c
static inline PGresult *
libpqsrv_exec(PGconn *conn, const char *query, uint32 wait_event_info)
```
## Detailed Description
This function serves as a wrapper around PostgreSQL's PQexec() functionality but with enhanced interrupt handling capabilities. It combines PQsendQuery() to initiate the query and libpqsrv_get_result_last() to retrieve results while properly handling interrupts. The function follows the preconditions of PQsendQuery() rather than PQexec(), meaning it doesn't automatically discard prior query results. For queries with long strings relative to TCP buffer size, consider using PQsetnonblocking(conn, 1) to enable interrupt processing during query text transmission.

## Parameters / Member Variables
- : PostgreSQL connection handle to execute the query on
- : SQL query string to execute
- : Wait event information for monitoring and debugging purposes

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQuery](../P/PQsendQuery.md)
  - [libpqsrv_get_result_last](libpqsrv_get_result_last.md)
- Called from (representative examples):
  - No direct references found in current codebase

## Notes and Other Information
- This function cannot process interrupts while pushing query text to the server unless non-blocking mode is enabled
- Has different preconditions compared to standard PQexec() - does not silently discard prior query results
- Returns NULL if PQsendQuery() fails, otherwise returns the result from libpqsrv_get_result_last()
- Located in src/include/libpq/libpq-be-fe-helpers.h:256-268