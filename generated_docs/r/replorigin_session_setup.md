# replorigin_session_setup

## Location
[src/backend/replication/logical/origin.c:1097-1189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1097-L1189)

## Overview
Sets up a replication origin in shared memory for tracking replication progress and caches access to the specific ReplicationSlot to avoid array searches during subsequent operations.

## Definition

```c
void
replorigin_session_setup(RepOriginId node, int acquired_by)
```
## Detailed Description
This function establishes a replication origin session by either finding an existing slot for the specified origin or creating a new one in the shared memory replication state array. It performs exclusive locking to ensure thread safety during the setup process.

The function supports two usage patterns:
1. **Normal case**: Single process usage where , ensuring the slot is not already acquired by another process
2. **Shared case**: Multiple processes safely sharing the same origin slot (e.g., parallel apply workers), where the first process uses  and subsequent processes pass the PID of the first process

The function also registers a cleanup handler on first use to ensure proper resource cleanup on process exit.

## Parameters / Member Variables
- : RepOriginId identifying the replication origin to set up
- : Process ID for shared usage (0 for exclusive single-process usage)

## Dependencies
- Functions called/Symbols referenced:
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [ReplicationOriginExitCleanup](../R/ReplicationOriginExitCleanup.md) 
  - LWLockAcquire/LWLockRelease
  - ConditionVariableBroadcast
  - ereport/elog
- Called from (representative examples):
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md)
  - [pg_replication_origin_session_setup](../p/pg_replication_origin_session_setup.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)
  - [run_apply_worker](run_apply_worker.md)

## Notes and Other Information
- Only one cached origin can exist per process in normal usage
- Requires  configuration
- Throws error if attempting to setup when another origin is already active
- Automatically registers cleanup handler for proper resource management
- Uses exclusive locking on ReplicationOriginLock during setup
- Supports parallel apply worker scenarios through the acquired_by mechanism