# RegisterDynamicBackgroundWorker

## Location
[src/backend/postmaster/bgworker.c:970-1081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L970-L1081)

## Overview
Registers a new dynamic background worker from a regular backend process that can be started on-demand during normal database operation.

## Definition

```c
structure.
	 */
	if (!IsUnderPostmaster)
		return false;
```
## Detailed Description
This function allows regular backend processes to register background workers dynamically during runtime, unlike static workers which must be registered during server startup. The function searches for an available slot in the shared memory worker pool, validates the worker configuration, and if successful, registers the worker and optionally provides a handle for tracking its status. Dynamic workers are particularly useful for parallel operations and on-demand background tasks.

The function implements proper concurrency control using lightweight locks and memory barriers to ensure the postmaster sees consistent state when checking for new workers. It also enforces limits on parallel workers to prevent resource exhaustion and provides immediate feedback on registration success or failure.

## Parameters / Member Variables
- : Pointer to a BackgroundWorker structure containing the worker configuration
- : Optional output parameter that receives a handle for tracking the worker's status (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [SanityCheckBackgroundWorker](../S/SanityCheckBackgroundWorker.md) (worker validation)
  - LWLockAcquire, LWLockRelease (locking)
  - memcpy (memory copy)
  - pg_write_barrier (memory barrier)
  - SendPostmasterSignal (postmaster notification)
  - [palloc](../p/palloc.md) (memory allocation)
- Constants referenced:
  - ERROR (error level)
  - BGWORKER_CLASS_PARALLEL (worker class flag)
  - LW_EXCLUSIVE (lock mode)
  - InvalidPid (invalid process ID)
  - MAX_PARALLEL_WORKER_LIMIT (parallel worker limit)
  - PMSIGNAL_BACKGROUND_WORKER_CHANGE (signal type)
- Global variables accessed:
  - IsUnderPostmaster (process context check)
  - BackgroundWorkerLock (shared lock)
  - BackgroundWorkerData (shared memory structure)
  - max_parallel_workers (configuration limit)
- Data structures used:
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md) (worker slot in shared memory)
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md) (worker tracking handle)
- Called from:
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md) (parallel query execution)
  - logicalrep_worker_launch (logical replication)
  - [setup_background_workers](../s/setup_background_workers.md), worker_spi_launch (test modules)

## Notes and Other Information
- Returns true on success, false on failure (typically due to no available slots)
- Can only be called from regular backend processes, not from the postmaster
- Enforces the max_parallel_workers limit for parallel workers to prevent resource exhaustion
- Uses memory barriers to ensure proper ordering of shared memory updates
- Signals the postmaster via PMSIGNAL_BACKGROUND_WORKER_CHANGE when a worker is registered
- The handle can be used with GetBackgroundWorkerPid() to track worker status
- Handles can be freed with pfree() when no longer needed
- Parallel workers are subject to additional accounting to track registration and termination counts
- The function performs immediate validation and slot allocation under exclusive lock