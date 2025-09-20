# run_vacuum_command

## Location
[src/bin/scripts/vacuumdb.c:1146-1167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L1146-L1167)

## Overview
This function sends a vacuum or analyze SQL command to the PostgreSQL server asynchronously and handles any execution errors by logging them appropriately.

## Definition

```c
static void
run_vacuum_command(PGconn *conn, const char *sql, bool echo,
				   const char *table)
```
## Detailed Description
The function executes a pre-constructed vacuum/analyze SQL command on the specified database connection. It uses PostgreSQL's asynchronous query interface () to send the command without waiting for completion. The function handles error reporting by distinguishing between table-specific operations and database-wide operations, providing appropriate error messages for each case.

Key behaviors:
- Uses asynchronous query execution for non-blocking operation
- Provides optional command echoing for verbose output
- Differentiates error messages between table-specific and database-wide vacuum operations
- Does not wait for command completion or process results

## Parameters / Member Variables
- : Active PostgreSQL database connection (PGconn pointer)
- : Complete SQL command string to execute (must be properly formatted)
- : Boolean flag to enable command echoing to stdout
- : Table name for error reporting context (NULL for database-wide operations)

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQuery](../P/PQsendQuery.md) (libpq function for asynchronous query execution)
  - [PQdb](../P/PQdb.md) (libpq function to get database name from connection)
  - [PQerrorMessage](../P/PQerrorMessage.md) (libpq function to get error message from connection)
  - printf (standard C library function)
  - pg_log_error (PostgreSQL logging function)
- Called from:
  - [vacuum_one_database](../v/vacuum_one_database.md) (multiple call sites)

## Notes and Other Information
- The function is static and only used within vacuumdb.c
- Uses asynchronous query execution, meaning it returns immediately after sending the command
- Caller is responsible for managing query results and connection state
- Error handling distinguishes between table-specific and database-wide vacuum operations
- The  parameter is used purely for error message context and can be NULL
- Does not validate the SQL command before execution
- Part of the vacuumdb utility's command execution pipeline