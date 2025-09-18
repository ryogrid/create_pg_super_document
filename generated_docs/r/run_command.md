# run_command

## Location
[src/bin/pg_amcheck/pg_amcheck.c:930-961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L930-L961)

## Overview
This function sends a SQL command to the PostgreSQL server asynchronously without waiting for completion, with error handling for command transmission failures.

## Definition


## Detailed Description
The run_command function provides asynchronous command execution functionality for the pg_amcheck utility. It:

1. **Optionally echoes commands** to stdout when the echo option is enabled, allowing users to see what SQL is being executed

2. **Sends commands asynchronously** using PQsendQuery() instead of synchronous PQexec(), enabling parallel execution of multiple commands across different database connections

3. **Handles transmission errors** by logging detailed error information and terminating the program if the command cannot be sent to the server

4. **Delegates result processing** to ParallelSlotHandler functions, which are responsible for handling query results, errors, and completion status

The function is designed for fire-and-forget command execution where the caller expects results to be processed asynchronously by other components of the parallel execution framework.

## Parameters / Member Variables
- : ParallelSlot structure containing the database connection and associated metadata for command execution
- : Null-terminated string containing the SQL command to be executed on the server

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQuery](../P/PQsendQuery.md) (PostgreSQL libpq function for asynchronous command sending)
  - [PQdb](../P/PQdb.md) (retrieves database name from connection)
  - pg_log_error (PostgreSQL logging function for error messages)
  - pg_log_error_detail (PostgreSQL logging function for detailed error information)
  - [ParallelSlot](../P/ParallelSlot.md) (structure type for parallel execution context)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck.c:777, 793)

## Notes and Other Information
- The function is static, meaning it's only accessible within the pg_amcheck.c compilation unit
- Uses asynchronous query execution to support parallel processing of multiple amcheck operations
- Error handling is limited to transmission failures; query execution errors are expected to be handled by the associated ParallelSlotHandler
- The function will terminate the entire program (exit(1)) if command transmission fails, indicating this is considered a fatal error condition
- Supports debugging through the opts.echo flag which prints executed SQL commands to stdout
- Located in src/bin/pg_amcheck/pg_amcheck.c:930-961