# logicalrep_worker_stop

## Location
[src/backend/replication/logical/launcher.c:622-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L622-L645)

## Overview
Public function that stops a logical replication worker for a specific subscription and relation pair.

## Definition

```c
void
logicalrep_worker_stop(Oid subid, Oid relid)
```
## Detailed Description
This function provides the public interface for stopping logical replication workers associated with a particular subscription and relation. It first attempts to locate the worker using the subscription ID and relation ID, then delegates the actual termination logic to logicalrep_worker_stop_internal(). The function includes a safety check to ensure it's not being called on parallel apply workers, which have their own dedicated stop function.

The function operates under LogicalRepWorkerLock protection to ensure thread-safe access to the worker pool and prevent race conditions during worker lookup and termination.

## Parameters / Member Variables
- : Object ID of the subscription whose worker should be stopped
- : Object ID of the relation being replicated (InvalidOid for subscription-level workers)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Acquires LogicalRepWorkerLock for thread-safe operations
  - [logicalrep_worker_find](logicalrep_worker_find.md): Locates the worker matching the subscription/relation pair
  - isParallelApplyWorker: Validates that this is not a parallel apply worker
  - [logicalrep_worker_stop_internal](logicalrep_worker_stop_internal.md): Performs the actual worker termination

- Called from:
  - [DropSubscription](../D/DropSubscription.md): Used during subscription cleanup to stop associated workers

## Notes and Other Information
- Only handles regular subscription workers, not parallel apply workers
- Uses SIGTERM for graceful worker termination
- Acquires LogicalRepWorkerLock in shared mode during the entire operation
- Includes assertion to prevent misuse with parallel apply workers
- Safe to call even if no worker exists for the given subscription/relation pair