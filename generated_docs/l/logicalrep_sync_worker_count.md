# logicalrep_sync_worker_count

## Location
[src/backend/replication/logical/launcher.c:861-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L861-L884)

## Overview
Counts the number of registered (not necessarily running) table synchronization workers for a specific subscription in PostgreSQL's logical replication system.

## Definition
```c
int logicalrep_sync_worker_count(Oid subid)
```

## Detailed Description
This function iterates through the logical replication worker pool to count how many table synchronization workers are currently registered for a given subscription. It specifically looks for workers that are identified as table sync workers (via `isTablesyncWorker()`) and matches them against the provided subscription ID. The function requires the caller to hold the LogicalRepWorkerLock to ensure thread-safe access to the worker pool data structures.

## Parameters / Member Variables
- `subid`: The OID (Object Identifier) of the subscription for which to count sync workers

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe
  - [LogicalRepWorker](../L/LogicalRepWorker.md)
  - isTablesyncWorker
- Called from (representative examples):
  - logicalrep_worker_launch
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)

## Notes and Other Information
- The function assumes the caller holds LogicalRepWorkerLock as verified by an assertion
- Only counts registered workers, not necessarily those that are currently running
- Part of PostgreSQL's logical replication infrastructure for managing table synchronization processes
- Returns an integer count of matching workers