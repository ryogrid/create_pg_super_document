# libpqsrv_exec_params

## Location
src/include/libpq/libpq-be-fe-helpers.h: 269 - 289

## Overview
A PQexecParams() wrapper function that processes interrupts while executing parameterized SQL queries, providing enhanced safety for server-side execution.

## Definition
static inline PGresult *libpqsrv_exec_params(PGconn *conn, const char *command, int nParams, const Oid *paramTypes, const char *const *paramValues, const int *paramLengths, const int *paramFormats, int resultFormat, uint32 wait_event_info)

## Detailed Description
This function serves as an interrupt-aware wrapper around PostgreSQL's PQexecParams() functionality. It enables safe execution of parameterized queries in server contexts by combining PQsendQueryParams() for query initiation with libpqsrv_get_result_last() for result retrieval. The function follows the same design principles and preconditions as libpqsrv_exec(), providing consistent interrupt handling behavior for parameterized queries.

## Parameters / Member Variables
- conn: PostgreSQL connection handle for query execution
- command: SQL command string with parameter placeholders
- nParams: Number of parameters in the query
- paramTypes: Array of parameter type OIDs (can be NULL)
- paramValues: Array of parameter values as strings
- paramLengths: Array of parameter lengths (can be NULL for null-terminated strings)
- paramFormats: Array specifying parameter formats (0=text, 1=binary)
- resultFormat: Format for result data (0=text, 1=binary)
- wait_event_info: Wait event information for monitoring purposes

## Dependencies
- Functions called/Symbols referenced:
  - PQsendQueryParams
  - libpqsrv_get_result_last
- Called from (representative examples):
  - No direct references found in current codebase

## Notes and Other Information
- Inherits the same limitations and considerations as libpqsrv_exec() regarding interrupt processing during query transmission
- Returns NULL if PQsendQueryParams() fails, otherwise returns the result from libpqsrv_get_result_last()
- Provides type-safe parameterized query execution with interrupt handling
- Located in src/include/libpq/libpq-be-fe-helpers.h:269-289