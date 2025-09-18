# logicalrep_pa_worker_count

## Location
[src/backend/replication/logical/launcher.c:885-911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L885-L911)

## Overview
Counts the number of registered (but not necessarily running) parallel apply workers for a specific subscription in PostgreSQL's logical replication system.

## Definition
```c
static int logicalrep_pa_worker_count(Oid subid)
```

## Detailed Description
This static function iterates through the logical replication worker pool to count how many parallel apply workers are currently registered for a given subscription. It specifically looks for workers that are identified as parallel apply workers (via `isParallelApplyWorker()`) and matches them against the provided subscription ID. The function requires the caller to hold the LogicalRepWorkerLock to ensure thread-safe access to the worker pool data structures. Parallel apply workers are responsible for applying changes in parallel to improve replication performance.

## Parameters / Member Variables
- `subid`: The OID (Object Identifier) of the subscription for which to count parallel apply workers

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe
  - [LogicalRepWorker](../L/LogicalRepWorker.md)
  - isParallelApplyWorker
- Called from (representative examples):
  - logicalrep_worker_launch

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- The function assumes the caller holds LogicalRepWorkerLock as verified by an assertion
- Only counts registered workers, not necessarily those that are currently running
- Part of PostgreSQL's logical replication infrastructure for managing parallel apply processes
- Parallel apply workers help improve replication performance by processing changes concurrently
- Returns an integer count of matching parallel apply workers