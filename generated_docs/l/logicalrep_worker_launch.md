# logicalrep_worker_launch

## Location
[src/backend/replication/logical/launcher.c:313-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L313-L539)

## Overview
Launches a new logical replication background worker of the specified type, handling slot allocation, worker configuration, and process startup with proper cleanup on failures.

## Definition

```c
bool
logicalrep_worker_launch(LogicalRepWorkerType wtype,
						 Oid dbid, Oid subid, const char *subname, Oid userid,
						 Oid relid, dsm_handle subworker_dsm)
```
## Detailed Description
logicalrep_worker_launch is the central function responsible for creating and starting logical replication workers. It handles three types of workers: apply workers (main subscription workers), parallel apply workers (for parallel processing), and table synchronization workers (for initial data sync). The function manages the complete worker lifecycle from slot allocation through process startup.

The function operates in several phases: first, it validates parameters and acquires locks; then it searches for an available worker slot, performing garbage collection if needed; next, it enforces worker limits per subscription type; finally, it configures the worker slot, registers a background worker, and waits for successful attachment.

Key features include automatic cleanup of stale worker slots, enforcement of subscription-specific worker limits (max_sync_workers_per_subscription, max_parallel_apply_workers_per_subscription), and comprehensive error handling with appropriate user feedback. The function uses generation counters to prevent race conditions during worker lifecycle management.

## Parameters / Member Variables
- : Type of worker to launch (WORKERTYPE_APPLY, WORKERTYPE_TABLESYNC, or WORKERTYPE_PARALLEL_APPLY)
- : Database OID where the subscription resides
- : Subscription OID that the worker will service
- : Subscription name (used for logging and worker naming)
- : User OID who owns the subscription
- : Relation OID (valid only for WORKERTYPE_TABLESYNC workers)
- : Dynamic shared memory handle (valid only for WORKERTYPE_PARALLEL_APPLY workers)
- Returns:  - true if worker was successfully launched and attached, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [logicalrep_sync_worker_count](logicalrep_sync_worker_count.md)
  - [logicalrep_pa_worker_count](logicalrep_pa_worker_count.md)
  - [logicalrep_worker_cleanup](logicalrep_worker_cleanup.md)
  - [TimestampDifferenceExceeds](../T/TimestampDifferenceExceeds.md)
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md)
  - [WaitForReplicationWorkerAttach](../W/WaitForReplicationWorkerAttach.md)
  - ereport/elog
- Called from:
  - [pa_launch_parallel_worker](../p/pa_launch_parallel_worker.md)
  - [ApplyLauncherMain](../A/ApplyLauncherMain.md)
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)

## Notes and Other Information
- Enforces max_replication_slots > 0 requirement for logical replication
- Implements automatic garbage collection of stale worker slots using wal_receiver_timeout
- Uses generation counters to prevent race conditions between slot cleanup and worker attachment
- Provides detailed error messages with configuration hints when limits are exceeded
- Sets up different background worker entry points based on worker type (ApplyWorkerMain, ParallelApplyWorkerMain, TablesyncWorkerMain)
- Critical for the scalability and reliability of PostgreSQL's logical replication system
- The function's return value indicates successful worker attachment, not just successful launch
- Worker slots are pre-allocated in shared memory, making this function's slot management crucial for system stability