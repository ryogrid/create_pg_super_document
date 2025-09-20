# PSQLexec

## Location
[src/bin/psql/common.c:620-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L620-L674)

## Overview
PSQLexec is a core function in psql for executing "backdoor" queries - internal queries not directly entered by users, with built-in error handling and optional query echoing.

## Definition

```c
PGresult *
PSQLexec(const char *query)
```
## Detailed Description
PSQLexec provides a standardized way to execute internal SQL queries within psql. It serves as a wrapper around libpq's PQexec() with additional psql-specific functionality:

- Validates database connection before execution
- Supports the -E (echo hidden commands) option by displaying queries when enabled
- Handles query logging to logfile if configured
- Provides PSQL_ECHO_HIDDEN_NOEXEC mode for showing queries without executing them
- Sets up proper cancellation handling during query execution
- Processes query results through AcceptResult() for consistent error handling
- Automatically cleans up failed results

This function is specifically designed for internal psql operations and is not subject to the -e (echo commands) option that affects user-entered queries. The caller is responsible for handling COPY command processing if the query initiates a COPY operation.

## Parameters / Member Variables
- : The SQL query string to be executed

## Dependencies
- Functions called/Symbols referenced:
  - PSQL_ECHO_HIDDEN_OFF (enum constant)
  - PSQL_ECHO_HIDDEN_NOEXEC (enum constant)
  - [SetCancelConn](../S/SetCancelConn.md) (sets up query cancellation)
  - [PQexec](PQexec.md) (libpq function for query execution)
  - [ResetCancelConn](../R/ResetCancelConn.md) (cleans up cancellation setup)
  - [AcceptResult](../A/AcceptResult.md) (validates and processes query results)
  - [ClearOrSaveResult](../C/ClearOrSaveResult.md) (handles failed result cleanup)

- Called from (representative examples):
  - [exec_command_password](../e/exec_command_password.md)
  - [describeAggregates](../d/describeAggregates.md), describeAccessMethods, describeTablespaces
  - Various describe functions for database objects
  - Large object operations (start_lo_xact, finish_lo_xact)
  - Many other internal psql operations

## Notes and Other Information
- Returns NULL if no database connection exists or if result processing fails
- The function assumes CLIENT_ENCODING is not modified by queries executed through this path
- Echo functionality respects both stdout and logfile output when configured
- [Query](../Q/Query.md) cancellation is properly handled to allow Ctrl+C interruption
- Part of psql's internal query execution infrastructure, distinct from user command processing
- Widely used throughout psql's describe and utility functions for metadata queries