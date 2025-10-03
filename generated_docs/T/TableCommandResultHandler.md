# TableCommandResultHandler

## Location
[src/fe_utils/parallel_slot.c:540-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L540-L564)

## Overview
A specialized result handler for parallel slot operations that processes command results against tables, handling missing table errors gracefully while failing on other errors.

## Definition

```c
bool
TableCommandResultHandler(PGresult *res, PGconn *conn, void *context)
```
## Detailed Description
TableCommandResultHandler is a ParallelSlotResultHandler implementation specifically designed for database utilities that execute commands (such as VACUUM, REINDEX, etc.) against tables. It handles the common race condition where a table might be dropped between the time a utility compiles its list of tables to process and when it actually attempts to process each table.

The function validates that command results have either PGRES_COMMAND_OK status or represent a missing table error (ERRCODE_UNDEFINED_TABLE). For missing table errors, it logs a warning but allows processing to continue, recognizing that concurrent table drops are a normal occurrence in active databases. For all other errors, it logs the error and returns false to terminate further processing.

This handler is essential for robust parallel database maintenance operations where table availability cannot be guaranteed between discovery and processing phases.

## Parameters / Member Variables
- `*res`: PGresult pointer containing the result from the executed command
- `*conn`: PGconn pointer to the database connection that executed the command
- `*context`: Unused void pointer for additional context (reserved for future use)
## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorField](../P/PQresultErrorField.md)
  - pg_log_error
  - [PQdb](../P/PQdb.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQclear](../P/PQclear.md)
  - PGRES_COMMAND_OK
  - PG_DIAG_SQLSTATE
  - ERRCODE_UNDEFINED_TABLE
- Called from (representative examples):
  - [reindex_one_database](../r/reindex_one_database.md) (src/bin/scripts/reindexdb.c:442)
  - [vacuum_one_database](../v/vacuum_one_database.md) (src/bin/scripts/vacuumdb.c:859, 884)
  - [ParallelSlotClearHandler](../P/ParallelSlotClearHandler.md) (src/include/fe_utils/parallel_slot.h:74)

## Notes and Other Information
- Returns true for successful commands and harmless missing table errors
- Returns false for fatal errors that should stop processing
- Specifically designed to handle race conditions in table-based operations
- Logs errors using pg_log_error for consistent error reporting
- Uses SQL state codes to differentiate between error types
- Part of the PostgreSQL frontend utilities parallel processing framework
- Located in src/fe_utils/parallel_slot.c:540-564