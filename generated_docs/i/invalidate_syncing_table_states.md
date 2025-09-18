# invalidate_syncing_table_states

## Location
src/backend/replication/logical/tablesync.c: 281 - 294

## Overview
A syscache invalidation callback function that marks the synchronization table states cache as needing reconstruction when subscription relation mappings change.

## Definition
```c
void invalidate_syncing_table_states(Datum arg, int cacheid, uint32 hashvalue)
```

## Detailed Description
This function serves as a callback registered with the PostgreSQL syscache invalidation mechanism. When the SUBSCRIPTIONRELMAP syscache is invalidated (due to changes in subscription relation states), this callback is invoked to mark the local table states cache as invalid.

The function is extremely simple but critical for cache coherency in logical replication. It sets the global `table_states_validity` variable to `SYNC_TABLE_STATE_NEEDS_REBUILD`, which signals that the next operation requiring table state information must rebuild the cache from the catalog.

This mechanism ensures that apply workers and parallel apply workers maintain consistent views of subscription relation states, which is essential for proper coordination during table synchronization processes.

## Parameters / Member Variables
- `arg`: Datum argument passed by the syscache callback system (unused)
- `cacheid`: ID of the cache that was invalidated (expected to be SUBSCRIPTIONRELMAP)
- `hashvalue`: Hash value of the invalidated entry (unused)

## Dependencies
- Functions called/Symbols referenced:
  - `SYNC_TABLE_STATE_NEEDS_REBUILD` (enum value)
  - `table_states_validity` (static global variable)
- Called from (representative examples):
  - Registered as callback via `CacheRegisterSyscacheCallback()` in:
    - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md) (src/backend/replication/logical/applyparallelworker.c:965)
    - [SetupApplyOrSyncWorker](../S/SetupApplyOrSyncWorker.md) (src/backend/replication/logical/worker.c:4739)

## Notes and Other Information
- This is a PostgreSQL syscache invalidation callback with the standard signature
- The function doesn't use any of its parameters - it simply invalidates the entire cache
- Part of PostgreSQL's cache invalidation infrastructure for maintaining consistency
- Registered for the SUBSCRIPTIONRELMAP syscache to detect changes in subscription relation states
- Essential for proper coordination between multiple logical replication workers
- The invalidation triggers a full rebuild of table state information in `FetchTableStates()`
- Works in conjunction with the `SyncingTablesState` enum values to track cache validity