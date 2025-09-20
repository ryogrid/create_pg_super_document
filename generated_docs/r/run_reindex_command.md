# run_reindex_command

## Location
[src/bin/scripts/reindexdb.c:589-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/reindexdb.c#L589-L635)

## Overview
Executes one or more accumulated REINDEX SQL commands against a database connection and provides type-specific error reporting.

## Definition

```c
static void
run_reindex_command(PGconn *conn, ReindexType type, const char *name,
					bool echo, PQExpBufferData *sql)
```
## Detailed Description
This function serves as the execution layer for REINDEX commands that have been previously constructed by gen_reindex_command. It performs the following operations:

1. **Command Echo**: Optionally prints the SQL command(s) to stdout if echo is enabled
2. **Asynchronous Execution**: Uses PQsendQuery to initiate the SQL command execution asynchronously 
3. **Error Handling**: Provides detailed, context-aware error messages based on the reindex type

The function is designed to work with batched SQL commands (multiple REINDEX statements separated by newlines) and provides specific error messages for each supported reindex operation type. It uses asynchronous query execution to enable proper integration with the parallel slot infrastructure used by reindex_one_database.

**Error Reporting**: Each reindex type gets a customized error message format:
- Database: "reindexing of database "<db>" failed"
- Index: "reindexing of index "<name>" in database "<db>" failed"
- Schema: "reindexing of schema "<name>" in database "<db>" failed"
- System: "reindexing of system catalogs in database "<db>" failed"
- Table: "reindexing of table "<name>" in database "<db>" failed"

## Parameters / Member Variables
- : PostgreSQL database connection to execute the command against
- : Type of reindex operation for error message context (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- : Name of the database object being reindexed (used in error messages)
- : Whether to print the SQL command to stdout before execution
- : Buffer containing the SQL command(s) to execute

## Dependencies
- Functions called/Symbols referenced:
  - printf
  - [PQsendQuery](../P/PQsendQuery.md)
  - pg_log_error
  - [PQdb](../P/PQdb.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - ReindexType enums (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- Called from (representative examples):
  - [reindex_one_database](reindex_one_database.md) (reindexdb.c:471)

## Notes and Other Information
- Uses asynchronous query execution (PQsendQuery) rather than synchronous (PQexec) to integrate with parallel processing infrastructure
- The caller is responsible for handling query results and waiting for completion using the parallel slots framework
- Error messages include both the object name and database name for better context
- The function does not handle query results or wait for completion - this is delegated to the parallel slot result handler
- SQL buffer can contain multiple commands separated by newlines for batch processing
- Function only initiates the query - actual error handling from server responses happens in the parallel slot infrastructure