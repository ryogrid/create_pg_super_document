# initialize_worker_spi

## Location
[src/test/modules/worker_spi/worker_spi.c:73-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/worker_spi/worker_spi.c#L73-L137)

## Overview
This function initializes the workspace for a PostgreSQL worker process by creating the necessary schema and table structure if they don't already exist.

## Definition

```c
static void
initialize_worker_spi(worktable *table)
```
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
  - [SetCurrentStatementStartTimestamp](../S/SetCurrentStatementStartTimestamp.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)  
  - [SPI_connect](../S/SPI_connect.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [SPI_execute](../S/SPI_execute.md)
  - [SPI_getbinval](../S/SPI_getbinval.md)
  - [DatumGetInt64](../D/DatumGetInt64.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [SPI_finish](../S/SPI_finish.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
- Called from (representative examples):
  - [worker_spi_main](../w/worker_spi_main.md)

## Notes and Other Information
- This is a static function, only accessible within the worker_spi.c module
- Uses SPI (Server Programming Interface) to execute SQL commands
- Implements proper PostgreSQL transaction management patterns
- Creates a schema with a specific table structure designed for counting operations
- The table structure supports both 'total' and 'delta' record types for incremental counting
- Error handling includes FATAL level logging for various failure conditions
- Location: src/test/modules/worker_spi/worker_spi.c:73-137

## Simplified Source

```c
static void
initialize_worker_spi(worktable *table)
{
    StringInfoData buf;

    // Start transaction and connect to SPI
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    pgstat_report_activity(STATE_RUNNING, "initializing worker_spi schema");

    // Check if schema already exists
    initStringInfo(&buf);
    appendStringInfo(&buf, "select count(*) from pg_namespace where nspname = '%s'",
                     table->schema);

    debug_query_string = buf.data;
    int ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
        elog(FATAL, "SPI_execute failed: error code %d", ret);

    if (SPI_processed != 1)
        elog(FATAL, "not a singleton result");

    // Get the count result
    bool isnull;
    int ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                           SPI_tuptable->tupdesc, 1, &isnull));
    if (isnull)
        elog(FATAL, "null result");

    // Create schema and table if they don't exist
    if (ntup == 0) {
        debug_query_string = NULL;
        resetStringInfo(&buf);
        appendStringInfo(&buf,
                        "CREATE SCHEMA \"%s\" "
                        "CREATE TABLE \"%s\" ("
                        "type text CHECK (type IN ('total', 'delta')), "
                        "value integer)"
                        "CREATE UNIQUE INDEX \"%s_unique_total\" ON \"%s\" (type) "
                        "WHERE type = 'total'",
                        table->schema, table->name, table->name, table->name);

        SetCurrentStatementStartTimestamp();
        debug_query_string = buf.data;
        ret = SPI_execute(buf.data, false, 0);

        if (ret != SPI_OK_UTILITY)
            elog(FATAL, "failed to create my schema");
        debug_query_string = NULL;
    }

    // Clean up transaction
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    debug_query_string = NULL;
    pgstat_report_activity(STATE_IDLE, NULL);
}
```