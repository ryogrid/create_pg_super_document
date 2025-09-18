# PQsendQueryPrepared

## Location
src/interfaces/libpq/fe-exec.c: 1633 - 1672

## Overview
PQsendQueryPrepared is a public API function that executes a previously prepared SQL statement with parameters using PostgreSQL's extended query protocol in asynchronous mode.

## Definition


## Detailed Description
PQsendQueryPrepared completes the prepare-bind-execute cycle by executing a statement that was previously prepared using PQsendPrepare or PQprepare. This function binds parameter values to the prepared statement and executes it using the extended query protocol. It provides optimal performance for repeatedly executed queries since the statement parsing and planning phases are skipped.

The function validates the connection state and parameters before delegating to PQsendQueryGuts with specific arguments that indicate this is a prepared statement execution (NULL command, non-NULL statement name). It supports the same parameter binding capabilities as PQsendQueryParams but operates on pre-parsed statements for better performance.

## Parameters / Member Variables
- : PostgreSQL connection handle for the database connection
- : Name of the previously prepared statement to execute
- : Number of parameter values to bind to the prepared statement
- : Array of parameter values as strings (NULL elements represent SQL NULL)
- : Array specifying the length of each parameter value (required for binary format)
- : Array specifying format codes for each parameter (0 for text, 1 for binary)
- : Format code for result data (0 for text, 1 for binary)

## Dependencies
- Functions called/Symbols referenced:
  - PQsendQueryStart: Validates connection state and prepares for query sending
  - PQsendQueryGuts: Core implementation for extended query protocol execution
  - PQ_QUERY_PARAM_MAX_LIMIT: Maximum allowed number of parameters
- Called from (representative examples):
  - PQexecPrepared: Synchronous version that waits for execution completion
  - sendCommand: pgbench utility function for performance testing prepared statements
  - process_queued_fetch_requests: pg_rewind utility function
  - Various test functions: libpq_pipeline module test cases for prepared statement testing

## Notes and Other Information
- Executes previously prepared statements without re-parsing, providing optimal performance for repeated queries
- Requires a valid prepared statement name that was created using PQsendPrepare or PQprepare
- Supports both text and binary parameter formats for efficient data transfer
- Parameter count must match the number of parameters defined in the prepared statement
- Uses the extended query protocol and is compatible with pipeline mode operations
- Widely used in high-performance applications that execute the same queries repeatedly with different parameter values
- The prepared statement must exist on the server before calling this function
- More efficient than PQsendQueryParams for repeated execution since parsing overhead is eliminated
- Parameter type information is not needed since it was established during statement preparation