# initialize_worker_spi

## Location
src/test/modules/worker_spi/worker_spi.c: 73 - 137

## Overview
This function initializes the workspace for a PostgreSQL worker process by creating the necessary schema and table structure if they don't already exist.

## Definition


## Detailed Description
The `initialize_worker_spi` function sets up the database environment for a worker_spi background worker by:
1. Starting a transaction and connecting to SPI (Server Programming Interface)
2. Checking if the target schema already exists by querying pg_namespace
3. If the schema doesn't exist, creating both the schema and a table named 'counted' with appropriate structure
4. The created table includes columns for 'type' (with CHECK constraint for 'total' or 'delta' values) and 'value' (integer)
5. Creating a unique index to ensure only one 'total' type record exists
6. Properly managing transaction lifecycle with commit and cleanup

This function is designed to be idempotent - it can be safely called multiple times without causing errors or duplicate schema creation.

## Parameters / Member Variables
- `table`: Pointer to a worktable structure containing schema and table name information
  - `table->schema`: The name of the schema to create/verify
  - `table->name`: The name of the table to create within the schema

## Dependencies
- Functions called/Symbols referenced:
  - SetCurrentStatementStartTimestamp
  - StartTransactionCommand  
  - SPI_connect
  - GetTransactionSnapshot
  - PushActiveSnapshot
  - pgstat_report_activity
  - SPI_execute
  - SPI_getbinval
  - DatumGetInt64
  - resetStringInfo
  - SPI_finish
  - PopActiveSnapshot
  - CommitTransactionCommand
- Called from (representative examples):
  - worker_spi_main

## Notes and Other Information
- This is a static function, only accessible within the worker_spi.c module
- Uses SPI (Server Programming Interface) to execute SQL commands
- Implements proper PostgreSQL transaction management patterns
- Creates a schema with a specific table structure designed for counting operations
- The table structure supports both 'total' and 'delta' record types for incremental counting
- Error handling includes FATAL level logging for various failure conditions
- Location: src/test/modules/worker_spi/worker_spi.c:73-137