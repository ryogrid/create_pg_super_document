# vacuum_one_database

## Location
[src/bin/scripts/vacuumdb.c:475-908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L475-L908)

## Overview
Processes vacuum and analyze operations on tables within a single database, supporting both sequential and parallel execution modes with comprehensive version compatibility checking and table filtering capabilities.

## Definition
```c
static void vacuum_one_database(ConnParams *cparams,
                                vacuumingOptions *vacopts,
                                int stage,
                                SimpleStringList *objects,
                                int concurrentCons,
                                const char *progname, bool echo, bool quiet)
```

## Detailed Description
This function is the core workhorse of the vacuumdb utility, responsible for executing vacuum and analyze operations on tables within a single database. It performs extensive validation of vacuum options against the target PostgreSQL server version, dynamically queries the system catalogs to determine which tables to process, and can execute operations either sequentially or in parallel using multiple database connections.

The function handles multiple modes of operation:
- Full vacuum operations with various options (disable-page-skipping, no-index-cleanup, etc.)
- Multi-stage analyze operations for generating optimizer statistics
- Table filtering based on user-specified criteria (specific tables, schemas, exclusions)
- Age-based filtering using transaction ID and multixact ID thresholds
- Parallel execution using multiple database connections for improved performance

Key features include comprehensive server version compatibility checking, dynamic table discovery through catalog queries, parallel execution coordination, and proper error handling throughout the process.

## Parameters / Member Variables
- `cparams`: Database connection parameters structure
- `vacopts`: Structure containing all vacuuming options and flags
- `stage`: Analyze stage number (ANALYZE_NO_STAGE for vacuum, 0-2 for analyze stages)
- `objects`: List of user-specified tables/schemas to process (can be NULL for all tables)
- `concurrentCons`: Number of concurrent connections to use for parallel processing
- `progname`: Program name for error reporting
- `echo`: Whether to echo SQL commands being executed
- `quiet`: Whether to suppress progress messages

## Dependencies
- Functions called/Symbols referenced:
  - [connectDatabase](../c/connectDatabase.md) (establish database connection)
  - [PQserverVersion](../P/PQserverVersion.md) (get PostgreSQL server version)
  - [splitTableColumnsSpec](../s/splitTableColumnsSpec.md) (parse table and column specifications)
  - [executeCommand](../e/executeCommand.md)/executeQuery (execute SQL commands)
  - [ParallelSlotsSetup](../P/ParallelSlotsSetup.md)/ParallelSlotsGetIdle (parallel execution management)
  - [prepare_vacuum_command](../p/prepare_vacuum_command.md)/run_vacuum_command (construct and execute vacuum commands)
  - [fmtQualifiedIdEnc](../f/fmtQualifiedIdEnc.md) (format qualified identifiers)
  - [simple_string_list_append](../s/simple_string_list_append.md) (manage table lists)
- Called from (representative examples):
  - [main](../m/main.md) (vacuumdb main function for single database processing)
  - [vacuum_all_databases](vacuum_all_databases.md) (for processing each database in all-databases mode)

## Notes and Other Information
- Performs extensive PostgreSQL version compatibility checking for various vacuum options
- Constructs complex catalog queries to discover tables matching user criteria
- Supports filtering by table names, schema names, transaction age, and multixact age
- Uses Common Table Expressions (CTEs) for efficient table matching when objects are specified
- Implements parallel processing using ParallelSlot infrastructure
- Handles both regular vacuum operations and multi-stage analyze operations
- Automatically uses SKIP_DATABASE_STATS optimization when supported by the server
- Includes comprehensive error handling and cleanup procedures
- The function is marked static, indicating internal use within vacuumdb.c only

## Simplified Source

```c
static void vacuum_one_database(ConnParams *cparams,
                                vacuumingOptions *vacopts,
                                int stage,
                                SimpleStringList *objects,
                                int concurrentCons,
                                const char *progname, bool echo, bool quiet) {
    PGconn *conn;
    PGresult *res;
    ParallelSlotArray *sa;
    SimpleStringList dbtables = {NULL, NULL};
    PQExpBuffer sql, catalog_query;
    bool failed = false;

    // Connect to the database
    conn = connectDatabase(cparams, progname, echo, false, true);

    // Check server version compatibility for various vacuum options
    if (vacopts->disable_page_skipping && PQserverVersion(conn) < 90600)
        pg_fatal("disable-page-skipping requires PostgreSQL 9.6+");
    // ... (other version checks simplified)

    // Print progress message
    if (!quiet) {
        if (stage != ANALYZE_NO_STAGE)
            printf("processing database \"%s\": %s\n", PQdb(conn), stage_messages[stage]);
        else
            printf("vacuuming database \"%s\"\n", PQdb(conn));
    }

    // Build catalog query to find tables to process
    initPQExpBuffer(&catalog_query);

    // Add table/schema filtering logic if objects specified
    if (objects) {
        // Build CTE for matching specified objects
        for (cell = objects->head; cell; cell = cell->next) {
            // Add table/schema specifications to query
        }
    }

    // Base catalog query for discoverable tables
    appendPQExpBufferStr(&catalog_query,
        "SELECT c.relname, ns.nspname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace ns ON c.relnamespace = ns.oid "
        "WHERE c.relpersistence != 't'");  // Exclude temp tables

    // Add filtering conditions
    if (vacopts->min_xid_age != 0) {
        appendPQExpBuffer(&catalog_query,
            " AND GREATEST(age(c.relfrozenxid), age(t.relfrozenxid)) >= %d",
            vacopts->min_xid_age);
    }

    // Execute catalog query to get table list
    res = executeQuery(conn, catalog_query.data, echo);
    termPQExpBuffer(&catalog_query);

    // Build qualified table name list
    initPQExpBuffer(&sql);
    for (int i = 0; i < PQntuples(res); i++) {
        char *qualified_name = fmtQualifiedIdEnc(PQgetvalue(res, i, 1),
                                                 PQgetvalue(res, i, 0),
                                                 PQclientEncoding(conn));
        simple_string_list_append(&dbtables, qualified_name);
    }
    PQclear(res);

    // Setup parallel execution slots
    if (concurrentCons > PQntuples(res))
        concurrentCons = PQntuples(res);
    if (concurrentCons <= 0)
        concurrentCons = 1;

    sa = ParallelSlotsSetup(concurrentCons, cparams, progname, echo,
                           stage != ANALYZE_NO_STAGE ? stage_commands[stage] : NULL);
    ParallelSlotsAdoptConn(sa, conn);

    // Process each table
    for (cell = dbtables.head; cell != NULL; cell = cell->next) {
        if (CancelRequested) {
            failed = true;
            break;
        }

        ParallelSlot *free_slot = ParallelSlotsGetIdle(sa, NULL);
        if (!free_slot) {
            failed = true;
            break;
        }

        // Prepare and execute vacuum command for this table
        prepare_vacuum_command(&sql, PQserverVersion(free_slot->connection),
                               vacopts, cell->val);
        ParallelSlotSetHandler(free_slot, TableCommandResultHandler, NULL);
        run_vacuum_command(free_slot->connection, sql.data, echo, cell->val);
    }

    // Wait for all operations to complete
    if (!ParallelSlotsWaitCompletion(sa))
        failed = true;

    // Cleanup
    ParallelSlotsTerminate(sa);
    pg_free(sa);
    termPQExpBuffer(&sql);

    if (failed)
        exit(1);
}
```