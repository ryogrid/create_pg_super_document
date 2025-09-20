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
- : Database connection parameters structure
- : Type of reindex operation (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- : List of user-specified objects to reindex (can be NULL for database/system reindex)
- : Program name for error reporting
- : Whether to echo SQL commands to stdout
- : Whether to output verbose progress information
- : Whether to use REINDEX CONCURRENTLY
- : Number of concurrent connections to use for parallel processing
- : Target tablespace for rebuilt indexes (PostgreSQL 14+)

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