# worker_spi_main

## Location
[src/test/modules/worker_spi/worker_spi.c:138-305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/worker_spi/worker_spi.c#L138-L305)

## Overview
This is the main entry point function for PostgreSQL background worker processes in the worker_spi test module, implementing a continuous loop that performs database operations via SPI.

## Definition
```c
void worker_spi_main(Datum main_arg)
```

## Detailed Description
The `worker_spi_main` function serves as the primary execution function for worker_spi background workers. It:

1. **Initialization Phase**: 
   - Extracts worker index from main_arg parameter
   - Parses database OID, role OID, and flags from bgw_extra data
   - Sets up signal handlers for SIGHUP and SIGTERM
   - Establishes database connection using provided credentials or fallback GUCs

2. **Schema Setup**: 
   - Creates a worktable structure with schema name pattern "schema{N}" where N is the worker index
   - Calls initialize_worker_spi to ensure schema and table exist
   - Quotes identifiers for SQL safety

3. **Main Processing Loop**:
   - Executes a complex SQL query that processes 'delta' records and updates 'total' records
   - Uses PostgreSQL's latch mechanism for efficient sleeping/waiting
   - Handles configuration reloads (SIGHUP) gracefully
   - Manages proper SPI transaction lifecycle for each iteration
   - Reports activity status to PostgreSQL's statistics system

The function implements a worker that consolidates incremental values ('delta' type) into running totals ('total' type) in a dedicated table.

## Parameters / Member Variables
- `main_arg`: Datum containing the worker index (converted to int32) used for generating unique schema names

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md), BackgroundWorkerUnblockSignals
  - [BackgroundWorkerInitializeConnectionByOid](../B/BackgroundWorkerInitializeConnectionByOid.md), BackgroundWorkerInitializeConnection  
  - [initialize_worker_spi](../i/initialize_worker_spi.md), quote_identifier
  - [WaitEventExtensionNew](../W/WaitEventExtensionNew.md), WaitLatch, ResetLatch
  - [SetCurrentStatementStartTimestamp](../S/SetCurrentStatementStartTimestamp.md), StartTransactionCommand
  - [SPI_connect](../S/SPI_connect.md), SPI_execute, SPI_finish
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md), PushActiveSnapshot, PopActiveSnapshot
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md), pgstat_report_activity, pgstat_report_stat
- Called from (representative examples):
  - Referenced by PostgreSQL background worker framework (no direct callers in codebase)

## Notes and Other Information
- This function runs in an infinite loop until terminated by SIGTERM
- Implements proper PostgreSQL background worker patterns including latch-based waiting
- Uses custom wait events for better monitoring and debugging
- Supports both static configuration via GUCs and dynamic configuration via bgw_extra
- The SQL query uses CTEs (Common Table Expressions) for atomic delta processing
- Handles interrupts and configuration reloads during execution
- Reports detailed logging when processing records
- Location: src/test/modules/worker_spi/worker_spi.c:138-305

## Simplified Source

```c
void
worker_spi_main(Datum main_arg)
{
    int index = DatumGetInt32(main_arg);
    worktable *table;
    StringInfoData buf;
    char name[20];
    Oid dboid, roleoid;
    char *p;
    bits32 flags = 0;

    // Initialize worker table structure
    table = palloc(sizeof(worktable));
    sprintf(name, "schema%d", index);
    table->schema = pstrdup(name);
    table->name = pstrdup("counted");

    // Extract database and role OIDs from bgw_extra
    p = MyBgworkerEntry->bgw_extra;
    memcpy(&dboid, p, sizeof(Oid));
    p += sizeof(Oid);
    memcpy(&roleoid, p, sizeof(Oid));
    p += sizeof(Oid);
    memcpy(&flags, p, sizeof(bits32));

    // Set up signal handlers
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    // Connect to database
    if (OidIsValid(dboid))
        BackgroundWorkerInitializeConnectionByOid(dboid, roleoid, flags);
    else
        BackgroundWorkerInitializeConnection(worker_spi_database, worker_spi_role, flags);

    elog(LOG, "%s initialized with %s.%s",
         MyBgworkerEntry->bgw_name, table->schema, table->name);

    // Initialize schema and table
    initialize_worker_spi(table);
    table->schema = quote_identifier(table->schema);
    table->name = quote_identifier(table->name);

    // Build the main SQL query for delta processing
    initStringInfo(&buf);
    appendStringInfo(&buf,
                    "WITH deleted AS (DELETE "
                    "FROM %s.%s "
                    "WHERE type = 'delta' RETURNING value), "
                    "total AS (SELECT coalesce(sum(value), 0) as sum "
                    "FROM deleted) "
                    "UPDATE %s.%s "
                    "SET value = %s.value + total.sum "
                    "FROM total WHERE type = 'total' "
                    "RETURNING %s.value",
                    table->schema, table->name,
                    table->schema, table->name,
                    table->name, table->name);

    // Main worker loop
    for (;;) {
        // Set up wait event and wait
        if (worker_spi_wait_event_main == 0)
            worker_spi_wait_event_main = WaitEventExtensionNew("WorkerSpiMain");

        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  worker_spi_naptime * 1000L, worker_spi_wait_event_main);
        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();

        // Handle configuration reload
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        // Execute the delta consolidation query
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        debug_query_string = buf.data;
        pgstat_report_activity(STATE_RUNNING, buf.data);

        int ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_UPDATE_RETURNING)
            elog(FATAL, "cannot select from table %s.%s: error code %d",
                 table->schema, table->name, ret);

        // Log the current total if updated
        if (SPI_processed > 0) {
            bool isnull;
            int32 val = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                                   SPI_tuptable->tupdesc, 1, &isnull));
            if (!isnull)
                elog(LOG, "%s: count in %s.%s is now %d",
                     MyBgworkerEntry->bgw_name, table->schema, table->name, val);
        }

        // Finish transaction
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        debug_query_string = NULL;
        pgstat_report_stat(true);
        pgstat_report_activity(STATE_IDLE, NULL);
    }
}
```