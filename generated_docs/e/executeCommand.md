# executeCommand

## Location
src/bin/pg_dump/pg_dumpall.c: 2000 - 2023

## Overview
Executes a SQL command on a PostgreSQL connection that returns no data, providing error handling and logging for modification operations.

## Definition
```c
static void executeCommand(PGconn *conn, const char *query)
```

## Detailed Description
This function is a companion to executeQuery, designed specifically for SQL commands that don't return data (such as CREATE, DROP, INSERT, UPDATE, DELETE statements). It executes the given command on the specified database connection and expects a PGRES_COMMAND_OK result status. Like executeQuery, it implements fail-fast error handling - any command failure results in program termination with detailed error logging.

The function follows the same error handling pattern as executeQuery but differs in the expected result type and cleanup behavior. It properly cleans up the PGresult after successful execution, whereas executeQuery returns the result for further processing.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `query`: SQL command string to execute (typically DDL or DML statements that don't return data)

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - PGRES_COMMAND_OK
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - pg_log_error
  - pg_log_error_detail
  - [PQfinish](../P/PQfinish.md)
  - [exit_nicely](exit_nicely.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dumpall)
  - [appendQualifiedRelation](../a/appendQualifiedRelation.md)
  - [vacuum_one_database](../v/vacuum_one_database.md)
  - [connect_slot](../c/connect_slot.md)

## Notes and Other Information
- This is a static function within pg_dumpall.c for internal module use
- Complementary to executeQuery - this function handles commands, executeQuery handles queries
- Automatically cleans up PGresult resources after successful execution with PQclear
- Used for DDL and DML operations that don't need to return result sets
- Provides the same comprehensive error logging as executeQuery
- Part of PostgreSQL's client utilities common command execution infrastructure
- Also used by other PostgreSQL utilities like pg_amcheck and vacuumdb for command execution