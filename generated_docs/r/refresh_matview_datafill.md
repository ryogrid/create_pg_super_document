# refresh_matview_datafill

## Location
src/backend/commands/matview.c: 389 - 447

## Overview
refresh_matview_datafill executes the materialized view's underlying query and sends the result rows to a destination receiver for insertion into the target materialized view.

## Definition
```c
static uint64 refresh_matview_datafill(DestReceiver *dest, Query *query, const char *queryString)
```

## Detailed Description
This function is responsible for executing the SELECT query that defines a materialized view and populating the target table with the results. It performs the complete query execution cycle: rewriting the query, planning it, creating a query descriptor, and executing it through the PostgreSQL executor framework.

The function uses a copied version of the original query to preserve the original query structure, acquires necessary rewrite locks, and ensures proper snapshot handling to see all previously executed query results. It redirects the query output to the provided destination receiver, which handles insertion into the materialized view's storage.

Key operations include snapshot management to ensure consistent data visibility, parallel execution optimization through CURSOR_OPT_PARALLEL_OK, and proper resource cleanup after execution completion.

## Parameters / Member Variables
- `dest`: DestReceiver that handles insertion of query results into the target materialized view
- `query`: Query structure representing the SELECT statement that defines the materialized view
- `queryString`: Original SQL query string for logging, debugging, and planning purposes

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (creates a deep copy of the query structure)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (acquires locks needed for query rewriting)
  - [QueryRewrite](../Q/QueryRewrite.md) (rewrites the query according to rules and views)
  - [pg_plan_query](../p/pg_plan_query.md) (creates an execution plan for the query)
  - CURSOR_OPT_PARALLEL_OK (flag to enable parallel query execution)
  - PushCopiedSnapshot, PopActiveSnapshot (snapshot stack management)
  - GetActiveSnapshot (retrieves current transaction snapshot)
  - UpdateActiveSnapshotCommandId (updates snapshot command ID)
  - [CreateQueryDesc](../C/CreateQueryDesc.md) (creates query descriptor for execution)
  - InvalidSnapshot (represents invalid snapshot constant)
  - [ExecutorStart](../E/ExecutorStart.md) (initializes executor state for query)
  - [ExecutorRun](../E/ExecutorRun.md) (executes the query plan)
  - ForwardScanDirection (scan direction constant)
  - [ExecutorFinish](../E/ExecutorFinish.md) (finalizes executor state)
  - [ExecutorEnd](../E/ExecutorEnd.md) (cleans up executor state)
  - [FreeQueryDesc](../F/FreeQueryDesc.md) (frees query descriptor memory)

- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md) (main materialized view refresh function)

## Notes and Other Information
- The function is declared static, making it internal to the matview.c module
- [Query](../Q/Query.md) rewriting should always result in exactly one SELECT query; more or fewer indicates an error
- [Snapshot](../S/Snapshot.md) management ensures that the query sees results from all previously executed queries within the transaction
- Parallel execution is enabled through CURSOR_OPT_PARALLEL_OK to improve performance on large datasets
- The function returns the number of rows processed, which is used for statistics and completion reporting
- Proper resource cleanup is performed through the executor framework's finish and end functions
- The destination receiver handles the actual insertion mechanism, allowing for different storage strategies