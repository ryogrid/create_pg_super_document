# executeStatement

## Location
src/bin/pgbench/pgbench.c: 1500 - 1515

## Overview
Executes a SQL statement using PQexec() and terminates the program with exit(1) if the statement fails, providing a simple wrapper for critical SQL operations in pgbench.

## Definition
```c
static void executeStatement(PGconn *con, const char *sql)
```

## Detailed Description
The `executeStatement` function is a utility wrapper around PostgreSQL's libpq PQexec() function that enforces strict error handling for SQL statement execution. It is designed for use in pgbench's initialization phase where SQL statement failures are considered fatal errors that should terminate the program immediately. The function executes the provided SQL statement and checks that it completed successfully (PGRES_COMMAND_OK). If the statement fails for any reason, it logs detailed error information including both the PostgreSQL error message and the actual SQL query that failed, then terminates the program. This approach ensures that pgbench initialization problems are caught early and reported clearly.

## Parameters / Member Variables
- `con`: PostgreSQL database connection handle (PGconn pointer)
- `sql`: Null-terminated string containing the SQL statement to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) (executes SQL statement via libpq)
  - [PQresultStatus](../P/PQresultStatus.md) (checks result status)
  - [PQerrorMessage](../P/PQerrorMessage.md) (retrieves error message from connection)
  - [PQclear](../P/PQclear.md) (frees result memory)
  - pg_log_error (logs error message)
  - pg_log_error_detail (logs additional error details)
  - exit (terminates program on failure)
- Constants used:
  - PGRES_COMMAND_OK (successful command completion status)
- Called from (representative examples):
  - [initDropTables](../i/initDropTables.md) (table initialization)
  - [createPartitions](../c/createPartitions.md) (partition creation)
  - [ddlinfo](../d/ddlinfo.md) (DDL operations)
  - [initTruncateTables](../i/initTruncateTables.md) (table truncation)
  - [initGenerateDataClientSide](../i/initGenerateDataClientSide.md)/ServerSide (data generation)
  - [initVacuum](../i/initVacuum.md) (vacuum operations)
  - [initCreatePKeys](../i/initCreatePKeys.md)/FKeys (constraint creation)

## Notes and Other Information
- This function is intended for initialization-phase SQL operations where failure should be fatal
- Always calls exit(1) on failure, making it unsuitable for runtime transaction processing
- Provides comprehensive error reporting with both PostgreSQL error details and the failing SQL
- Memory management is handled properly with PQclear() for successful results
- Located in src/bin/pgbench/pgbench.c:1500-1515 and widely used throughout pgbench initialization