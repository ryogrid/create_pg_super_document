# PQsendQueryParams

## Location
[src/interfaces/libpq/fe-exec.c:1492-1535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1492-L1535)

## Overview
PQsendQueryParams is a public API function that sends a SQL query with parameters using PostgreSQL's extended query protocol in asynchronous mode.

## Definition


## Detailed Description
PQsendQueryParams provides the ability to execute parameterized queries asynchronously using the extended query protocol. Unlike the simple query protocol used by PQsendQuery, this function supports parameter binding, which provides better security against SQL injection attacks and improved performance for repeated queries with different parameter values.

The function validates the connection state and parameter constraints before delegating the actual work to PQsendQueryGuts. It enforces a maximum limit on the number of parameters and uses an unnamed prepared statement for execution. The extended query protocol allows for type-safe parameter passing and supports both text and binary parameter formats.

## Parameters / Member Variables
- : PostgreSQL connection handle for the database connection
- : SQL command string with parameter placeholders (typically using , , etc.)
- : Number of parameters to bind to the query
- : Array of PostgreSQL type OIDs for each parameter (can be NULL for automatic type inference)
- : Array of parameter values as strings (NULL elements represent SQL NULL)
- : Array specifying the length of each parameter value (required for binary format)
- : Array specifying format codes for each parameter (0 for text, 1 for binary)
- : Format code for result data (0 for text, 1 for binary)

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQueryStart](PQsendQueryStart.md): Validates connection state and prepares for query sending
  - [PQsendQueryGuts](PQsendQueryGuts.md): Core implementation for extended query protocol execution
  - PQ_QUERY_PARAM_MAX_LIMIT: Maximum allowed number of parameters
- Called from (representative examples):
  - [PQexecParams](PQexecParams.md): Synchronous version that waits for completion
  - [sendCommand](../s/sendCommand.md): pgbench utility function for performance testing
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md): psql command execution function
  - Various test functions: libpq_pipeline module test cases

## Notes and Other Information
- Uses the extended query protocol which provides better security and performance for parameterized queries
- Supports both text and binary parameter formats for efficient data transfer
- Parameter limit is enforced to prevent resource exhaustion (PQ_QUERY_PARAM_MAX_LIMIT)
- Uses unnamed prepared statements internally, allowing for one-time execution without explicit preparation
- Compatible with pipeline mode operations unlike simple query protocol functions
- Widely used in PostgreSQL client applications and utilities for secure parameter binding
- The function performs input validation before delegating to the lower-level PQsendQueryGuts implementation