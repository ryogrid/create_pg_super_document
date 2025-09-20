# run_simple_command

## Location
[src/bin/pg_rewind/libpq_source.c:192-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L192-L208)

## Overview
Executes a SQL command on a PostgreSQL connection and expects successful completion without returning any result data.

## Definition

```c
static void
run_simple_command(PGconn *conn, const char *sql)
```
## Detailed Description
The  function is a utility function that executes SQL commands that are expected to complete successfully without returning data. It is primarily used for configuration commands like SET statements, DDL commands, or other administrative operations.

The function performs basic error checking by verifying that the command completed with PGRES_COMMAND_OK status. If the command fails for any reason, the function immediately terminates the program using , making it unsuitable for commands where failure recovery is needed.

This function is the counterpart to  - while  is for queries that return data,  is for commands that perform actions without returning result sets.

## Parameters
- : PostgreSQL connection to execute the command on
- : SQL command string that should execute successfully without returning data

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [PQclear](../P/PQclear.md)
  - [pg_fatal](../p/pg_fatal.md)
  - PGRES_COMMAND_OK
- Called from:
  - [init_libpq_conn](../i/init_libpq_conn.md) (multiple times at lines 117, 118, 119, 120, 126)

## Notes and Other Information
- This is a static function, only accessible within the libpq_source.c file
- The function will terminate the program with  if the command fails, providing no error recovery mechanism
- Commonly used for SET commands to configure session parameters (timeouts, read-only mode, etc.)
- Unlike , this function expects no result data and will not validate result set format
- The function automatically cleans up the PGresult structure by calling 
- Typical use cases include configuration commands that must succeed for proper pg_rewind operation