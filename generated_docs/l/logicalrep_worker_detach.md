# logicalrep_worker_detach

## Location
[src/backend/replication/logical/launcher.c:757-798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L757-L798)

## Overview
Detaches the current logical replication worker process from its slot, stopping any associated parallel apply workers and cleaning up the worker state information.

## Definition

```c
static void
logicalrep_worker_detach(void)
```
## Detailed Description
This static function handles the orderly shutdown and detachment of a logical replication worker process. It performs a comprehensive cleanup that includes stopping parallel apply workers (if the current worker is a leader) and cleaning up the worker's shared memory state.

The detachment process involves two main phases:
1. **Parallel Worker Cleanup**: If the current process is a leader apply worker, it stops all associated parallel apply workers by:
   - Detaching from error message queues to prevent duplicate logging
   - Finding all parallel workers for the same subscription
   - Sending SIGTERM signals to terminate parallel workers
2. **Worker State Cleanup**: Cleans up the current worker's shared memory slot and state

The function ensures proper synchronization using different lock levels: shared locks for reading worker information and exclusive locks for modifying worker state.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [am_leader_apply_worker](../a/am_leader_apply_worker.md)
  - [pa_detach_all_error_mq](../p/pa_detach_all_error_mq.md)
  - [LWLockAcquire](../L/LWLockAcquire.md) (LogicalRepWorkerLock, LW_SHARED/LW_EXCLUSIVE)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [logicalrep_workers_find](logicalrep_workers_find.md)
  - isParallelApplyWorker
  - [logicalrep_worker_stop_internal](logicalrep_worker_stop_internal.md)
  - [logicalrep_worker_cleanup](logicalrep_worker_cleanup.md)
- Called from (representative examples):
  - [logicalrep_worker_onexit](logicalrep_worker_onexit.md) (src/backend/replication/logical/launcher.c:837)

## Notes and Other Information
- This is a static function, only accessible within the launcher.c file
- The function uses different locking strategies: shared locks for reading worker lists and exclusive locks for cleanup operations
- Error message queue detachment is performed before terminating parallel workers to prevent duplicate error reporting
- The function is typically called during worker process termination as part of the exit handler chain
- Proper cleanup prevents resource leaks and ensures that parallel workers are terminated gracefully