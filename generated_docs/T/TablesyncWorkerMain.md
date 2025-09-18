# TablesyncWorkerMain

## Location
src/backend/replication/logical/tablesync.c: 1737 - 1756

## Overview
TablesyncWorkerMain is the main entry point for logical replication table synchronization workers in PostgreSQL. It sets up and runs the synchronization process for individual tables during logical replication.

## Definition
```c
void TablesyncWorkerMain(Datum main_arg)
```

## Detailed Description
This function serves as the primary entry point for tablesync workers in PostgreSQL's logical replication system. It orchestrates the complete lifecycle of a table synchronization worker by sequentially calling setup, execution, and cleanup functions. The function is designed to be called as a background worker process and handles the synchronization of a single table from the publisher to the subscriber during logical replication setup.

The function follows a simple three-stage process: worker setup, table synchronization execution, and cleanup. It extracts the worker slot identifier from the passed Datum argument and uses it to configure the worker environment before beginning the actual synchronization work.

## Parameters / Member Variables
- `main_arg`: A Datum containing the worker slot number (converted to int32) that identifies which worker slot this process should use

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [SetupApplyOrSyncWorker](../S/SetupApplyOrSyncWorker.md)
  - [run_tablesync_worker](../r/run_tablesync_worker.md)
  - finish_sync_worker
- Called from (representative examples):
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md) (via background worker framework)
  - Referenced in logicalworker.h header

## Notes and Other Information
- This function is the main entry point for tablesync background worker processes
- Located in src/backend/replication/logical/tablesync.c:1737-1756
- Part of PostgreSQL's logical replication infrastructure
- The function is designed to run in a separate background process dedicated to synchronizing a specific table
- Error handling and transaction management are handled by the called functions rather than at this level