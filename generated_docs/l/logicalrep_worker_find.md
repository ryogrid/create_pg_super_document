# logicalrep_worker_find

## Location
[src/backend/replication/logical/launcher.c:256-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L256-L287)

## Overview
Searches the logical replication worker array to find a worker that matches the specified subscription ID and relation ID, with optional filtering for only running workers.

## Definition

```c
LogicalRepWorker *
logicalrep_worker_find(Oid subid, Oid relid, bool only_running)
```
## Detailed Description
logicalrep_worker_find performs a linear search through the global logical replication worker array to locate a specific worker based on subscription and relation identifiers. The function is designed to find leader apply workers or table synchronization workers, explicitly excluding parallel apply workers from the search results.

The search iterates through all available worker slots (up to max_logical_replication_workers) and examines each worker's subscription ID, relation ID, and status. The function can optionally filter results to return only actively running workers (those with an attached process) when the only_running parameter is true.

The function operates under the assumption that the caller holds the LogicalRepWorkerLock in shared or exclusive mode, as indicated by the Assert statement. This ensures thread-safe access to the worker array during the search operation.

## Parameters / Member Variables
- : Subscription OID to match against worker's subscription ID
- : Relation OID to match against worker's relation ID  
- : If true, only return workers that are currently running (have an attached process)
- Returns:  - Pointer to matching worker, or NULL if no match found

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) (for assertion checking)
  - isParallelApplyWorker (to exclude parallel workers)
- Called from:
  - [logicalrep_worker_stop](logicalrep_worker_stop.md)
  - [logicalrep_worker_wakeup](logicalrep_worker_wakeup.md)
  - [ApplyLauncherMain](../A/ApplyLauncherMain.md)
  - [wait_for_relation_state_change](../w/wait_for_relation_state_change.md)
  - [wait_for_worker_state_change](../w/wait_for_worker_state_change.md)
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)

## Notes and Other Information
- Must be called while holding LogicalRepWorkerLock (enforced by assertion)
- Deliberately skips parallel apply workers using isParallelApplyWorker check
- Returns the first matching worker found - does not handle multiple matches
- Used extensively by both launcher and tablesync subsystems for worker management
- The only_running parameter allows callers to distinguish between allocated workers and actually running processes
- Critical for coordinating worker lifecycle management across the logical replication system

## Simplified Source

```c
LogicalRepWorker *logicalrep_worker_find(Oid subid, Oid relid, bool only_running) {
    Assert(LWLockHeldByMe(LogicalRepWorkerLock));

    // Search through all worker slots
    for (int i = 0; i < max_logical_replication_workers; i++) {
        LogicalRepWorker *w = &LogicalRepCtx->workers[i];

        // Skip parallel apply workers - we only want leader/sync workers
        if (isParallelApplyWorker(w))
            continue;

        // Check if worker matches criteria
        if (w->in_use && w->subid == subid && w->relid == relid &&
            (!only_running || w->proc)) {
            return w;  // Found matching worker
        }
    }

    return NULL;  // No matching worker found
}
```