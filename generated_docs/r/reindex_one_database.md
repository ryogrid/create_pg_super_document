# reindex_one_database

## Location
[src/bin/scripts/reindexdb.c:275-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/reindexdb.c#L275-L505)

## Overview
Performs reindex operations on a single database, handling both serial and parallel execution modes for different object types (database, schema, table, index, system).

## Definition

```c
static void
reindex_one_database(ConnParams *cparams, ReindexType type,
					 SimpleStringList *user_list,
					 const char *progname, bool echo,
					 bool verbose, bool concurrently, int concurrentCons,
					 const char *tablespace)
```
## Detailed Description
This function coordinates reindex operations on a single database and serves as the core logic for the reindexdb utility. It handles different reindex types and execution modes:

**Serial Mode**: For single-connection operations, it processes objects directly using the specified type (database, system, schema, table, or index).

**Parallel Mode**: For concurrent operations, it:
1. Converts high-level types (database, schema) to table-level operations by expanding them into lists of individual relations
2. Sets up parallel slot arrays to manage multiple connections
3. Distributes reindex commands across available parallel slots
4. For index reindexing, groups indices by table to optimize parallel processing

The function includes version compatibility checks for concurrent reindexing (PostgreSQL 12+) and tablespace options (PostgreSQL 14+). It manages connection pools through the parallel slots infrastructure and ensures proper cleanup of resources.

## Parameters / Member Variables
- `*cparams`: Database connection parameters structure
- `type`: Type of reindex operation (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- `*user_list`: List of user-specified objects to reindex (can be NULL for database/system reindex)
- `*progname`: Program name for error reporting
- `echo`: Whether to echo SQL commands to stdout
- `verbose`: Whether to output verbose progress information
- `concurrently`: Whether to use REINDEX CONCURRENTLY
- `concurrentCons`: Number of concurrent connections to use for parallel processing
- `*tablespace`: Target tablespace for rebuilt indexes (PostgreSQL 14+)
## Dependencies
- Functions called/Symbols referenced:
  - [connectDatabase](../c/connectDatabase.md)
  - [PQserverVersion](../P/PQserverVersion.md)
  - [PQfinish](../P/PQfinish.md)
  - [get_parallel_object_list](../g/get_parallel_object_list.md)
  - [ParallelSlotsSetup](../P/ParallelSlotsSetup.md)
  - [ParallelSlotsAdoptConn](../P/ParallelSlotsAdoptConn.md)
  - [ParallelSlotsGetIdle](../P/ParallelSlotsGetIdle.md)
  - [ParallelSlotSetHandler](../P/ParallelSlotSetHandler.md)
  - [gen_reindex_command](../g/gen_reindex_command.md)
  - [run_reindex_command](run_reindex_command.md)
  - [ParallelSlotsWaitCompletion](../P/ParallelSlotsWaitCompletion.md)
  - [ParallelSlotsTerminate](../P/ParallelSlotsTerminate.md)
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [simple_string_list_destroy](../s/simple_string_list_destroy.md)
- Called from (representative examples):
  - [main](../m/main.md) (reindexdb.c:241, 246, 251, 256, 266)
  - [reindex_all_databases](reindex_all_databases.md) (reindexdb.c:850, 855, 860, 865, 875)

## Notes and Other Information
- Exits with status 1 if any reindex operation fails
- For parallel index reindexing, groups indices belonging to the same table together for processing by a single connection to avoid conflicts
- Automatically adjusts the number of concurrent connections based on the actual number of objects to process
- Handles cancellation requests through the global CancelRequested flag
- Memory management includes proper cleanup of dynamically allocated string lists and parallel slot arrays
- The function never returns normally in case of errors - it calls exit(1) for failure cases

## Simplified Source

```c
static void
reindex_one_database(ConnParams *cparams, ReindexType type,
                     SimpleStringList *user_list,
                     const char *progname, bool echo,
                     bool verbose, bool concurrently, int concurrentCons,
                     const char *tablespace)
{
    PGconn *conn;
    SimpleStringList *process_list = user_list;
    bool parallel = concurrentCons > 1;
    bool failed = false;
    int items_count = 0;

    // Connect to database
    conn = connectDatabase(cparams, progname, echo, false, true);

    // Version compatibility checks
    if (concurrently && PQserverVersion(conn) < 120000)
        pg_fatal("concurrently option requires PostgreSQL 12+");

    if (tablespace && PQserverVersion(conn) < 140000)
        pg_fatal("tablespace option requires PostgreSQL 14+");

    // Prepare object list based on reindex type and parallel mode
    if (!parallel) {
        // Serial mode: simple object list preparation
        switch (type) {
            case REINDEX_DATABASE:
            case REINDEX_SYSTEM:
                // Create single-item list with database name
                process_list = pg_malloc0(sizeof(SimpleStringList));
                simple_string_list_append(process_list, PQdb(conn));
                break;
            case REINDEX_INDEX:
            case REINDEX_SCHEMA:
            case REINDEX_TABLE:
                // Use provided user_list directly
                break;
        }
    } else {
        // Parallel mode: expand high-level types to table lists
        switch (type) {
            case REINDEX_DATABASE:
            case REINDEX_SCHEMA:
                // Get list of tables/relations for parallel processing
                process_list = get_parallel_object_list(conn, type, user_list, echo);
                if (process_list == NULL) return; // Nothing to process
                break;
            case REINDEX_INDEX:
                // Special handling for index reindexing with table grouping
                get_parallel_object_list(conn, type, user_list, echo);
                if (user_list->head == NULL) return;
                break;
            case REINDEX_TABLE:
                // Use provided list as-is
                break;
        }
    }

    // Count items and adjust concurrent connections
    for (SimpleStringListCell *cell = process_list->head; cell; cell = cell->next) {
        items_count++;
        if (items_count >= concurrentCons) break;
    }
    concurrentCons = Min(concurrentCons, items_count);

    // Set up parallel processing infrastructure
    ParallelSlotArray *sa = ParallelSlotsSetup(concurrentCons, cparams, progname, echo, NULL);
    ParallelSlotsAdoptConn(sa, conn);

    // Process each object in the list
    SimpleStringListCell *cell = process_list->head;
    while (cell != NULL) {
        if (CancelRequested) {
            failed = true;
            break;
        }

        // Get free parallel slot
        ParallelSlot *free_slot = ParallelSlotsGetIdle(sa, NULL);
        if (!free_slot) {
            failed = true;
            break;
        }

        // Generate and execute reindex command
        PQExpBufferData sql;
        initPQExpBuffer(&sql);
        gen_reindex_command(free_slot->connection, type, cell->val,
                           echo, verbose, concurrently, tablespace, &sql);

        // For parallel index reindexing, group indices of same table
        if (parallel && type == REINDEX_INDEX) {
            // Process all indices of the same table together
            while (/* indices of same table */) {
                // Add additional reindex commands to same SQL buffer
                cell = cell->next;
                gen_reindex_command(free_slot->connection, type, cell->val,
                                   echo, verbose, concurrently, tablespace, &sql);
            }
        }

        run_reindex_command(free_slot->connection, type, cell->val, echo, &sql);
        termPQExpBuffer(&sql);
        cell = cell->next;
    }

    // Wait for all parallel operations to complete
    if (!ParallelSlotsWaitCompletion(sa))
        failed = true;

    // Cleanup resources
    if (process_list != user_list) {
        simple_string_list_destroy(process_list);
        pg_free(process_list);
    }
    ParallelSlotsTerminate(sa);
    pfree(sa);

    if (failed)
        exit(1);
}
```