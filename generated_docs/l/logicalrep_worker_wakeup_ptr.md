# logicalrep_worker_wakeup_ptr

## Location
[src/backend/replication/logical/launcher.c:709-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L709-L719)

## Overview
Low-level function that directly wakes up a logical replication worker using its process latch.

## Definition

```c
void
logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker)
```
## Detailed Description
This function provides the core latch-based wakeup mechanism for logical replication workers. It directly sets the worker process's latch to signal that the worker should resume processing. This is the fundamental building block used by higher-level wakeup functions and requires the caller to already hold the appropriate lock to ensure the worker process pointer remains valid during the operation.

The function performs a simple but critical operation: setting the worker's process latch, which causes the worker to exit any WaitLatch() calls and resume execution. This enables responsive communication between different parts of the logical replication system.

## Parameters / Member Variables
- `*worker`: Pointer to the LogicalRepWorker structure whose process should be awakened
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md): Asserts that the caller holds LogicalRepWorkerLock to prevent race conditions
  - [SetLatch](../S/SetLatch.md): Sets the worker process's latch to wake it up

- Called from:
  - [logicalrep_worker_wakeup](logicalrep_worker_wakeup.md): Higher-level function that finds and wakes workers by subscription/relation
  - [wait_for_worker_state_change](../w/wait_for_worker_state_change.md): Used during table synchronization state transitions
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md): Coordinates timing during table synchronization startup
  - [AtEOXact_LogicalRepWorkers](../A/AtEOXact_LogicalRepWorkers.md): Wakes workers during transaction end processing

## Notes and Other Information
- Requires LogicalRepWorkerLock to be held by caller (asserted at runtime)
- Assumes worker->proc is valid and non-NULL (caller's responsibility)  
- Provides the fundamental inter-process communication primitive for logical replication
- Essential for minimizing worker response latency in logical replication systems
- Used extensively throughout the logical replication subsystem for worker coordination

## Simplified Source

```c
void
logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker)
{
    Assert(LWLockHeldByMe(LogicalRepWorkerLock));

    SetLatch(&worker->proc->procLatch);
}
```