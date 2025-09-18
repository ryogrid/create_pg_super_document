# GetLeaderApplyWorkerPid

## Location
src/backend/replication/logical/launcher.c: 1277 - 1303

## Overview
Retrieves the process ID of the leader apply worker when given the PID of a parallel apply worker in logical replication.

## Definition


## Detailed Description
This function searches through the logical replication worker array to find a parallel apply worker with the specified process ID. If found, it returns the PID of that worker's leader apply worker. The function is designed to support parallel logical replication where multiple workers can be coordinated under a single leader worker. It acquires a shared lock on the LogicalRepWorkerLock to safely iterate through the worker array, checking each worker to see if it's a parallel apply worker and if its PID matches the provided parameter. If no matching parallel worker is found, it returns InvalidPid.

## Parameters / Member Variables
- : The process ID to look up, expected to be a parallel apply worker PID

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire, LWLockRelease
  - isParallelApplyWorker
  - InvalidPid, LW_SHARED
  - [LogicalRepWorker](../L/LogicalRepWorker.md) (struct type)
  - LogicalRepCtx->workers (shared memory array)
  - max_logical_replication_workers (configuration variable)
- Called from (representative examples):
  - PG_STAT_GET_ACTIVITY_COLS (for pg_stat_activity views)

## Notes and Other Information
- Returns InvalidPid if the given PID is not a parallel apply worker or if no worker is found
- The function is thread-safe due to proper locking with LogicalRepWorkerLock
- Used primarily for monitoring and administrative functions like pg_stat_activity
- The leader_pid field in the LogicalRepWorker structure maintains the relationship between parallel and leader workers
- This function supports PostgreSQL's parallel logical replication feature introduced in later versions