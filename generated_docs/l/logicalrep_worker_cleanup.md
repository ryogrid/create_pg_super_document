# logicalrep_worker_cleanup

## Location
[src/backend/replication/logical/launcher.c:799-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L799-L819)

## Overview
Cleans up and resets all fields of a logical replication worker structure to their default/invalid states, making the worker slot available for reuse.

## Definition

```c
static void
logicalrep_worker_cleanup(LogicalRepWorker *worker)
```
## Detailed Description
This static function performs a complete cleanup of a LogicalRepWorker structure by resetting all its fields to default or invalid values. The function is designed to be called when a worker is being terminated or when a worker slot needs to be made available for reuse.

The cleanup process resets:
- Worker type to unknown state
- Usage flag to indicate the slot is free
- Process pointer to NULL (detaching from any process)
- All OID fields (database, user, subscription, relation) to invalid values
- Leader process ID to invalid PID
- Parallel apply flag to false

The function includes an assertion to ensure it's called while holding the appropriate exclusive lock, which prevents concurrent modifications to worker state.

## Parameters / Member Variables
- `*worker`: Pointer to the LogicalRepWorker structure to be cleaned up
## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - WORKERTYPE_UNKNOWN
  - InvalidOid
  - InvalidPid
- Called from (representative examples):
  - [WaitForReplicationWorkerAttach](../W/WaitForReplicationWorkerAttach.md) (src/backend/replication/logical/launcher.c:218)
  - [logicalrep_worker_launch](logicalrep_worker_launch.md) (src/backend/replication/logical/launcher.c:398, 521)
  - [logicalrep_worker_detach](logicalrep_worker_detach.md) (src/backend/replication/logical/launcher.c:790)

## Notes and Other Information
- This is a static function, only accessible within the launcher.c file
- The function requires the caller to hold LogicalRepWorkerLock in exclusive mode (enforced by assertion)
- All identity and state information is reset to ensure no stale data remains in the worker slot
- The function is commonly used in error handling paths and normal worker termination scenarios
- After calling this function, the worker slot becomes available for allocation to new workers

## Simplified Source

```c
static void logicalrep_worker_cleanup(LogicalRepWorker *worker)
{
    // Ensure we have exclusive lock (required for worker modifications)
    Assert(LWLockHeldByMeInMode(LogicalRepWorkerLock, LW_EXCLUSIVE));

    // Reset worker state to defaults
    worker->type = WORKERTYPE_UNKNOWN;
    worker->in_use = false;
    worker->proc = NULL;

    // Clear all identity information
    worker->dbid = InvalidOid;
    worker->userid = InvalidOid;
    worker->subid = InvalidOid;
    worker->relid = InvalidOid;

    // Clear parallel worker information
    worker->leader_pid = InvalidPid;
    worker->parallel_apply = false;
}
```