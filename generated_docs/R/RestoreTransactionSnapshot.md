# RestoreTransactionSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1840-1855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1840-L1855)

## Overview
Installs a restored snapshot as the current transaction snapshot, typically used when setting up parallel worker processes.

## Definition

```c
void
RestoreTransactionSnapshot(Snapshot snapshot, void *source_pgproc)
```
## Detailed Description
RestoreTransactionSnapshot is a convenience wrapper function that installs a previously restored snapshot as the active transaction snapshot. This function is primarily used in parallel query execution when a worker process needs to adopt the same MVCC visibility rules that were active in the main process. The function delegates to SetTransactionSnapshot with appropriate parameters for restored snapshots.

The function uses a void pointer for the source_pgproc parameter to avoid including PGPROC declarations in snapmgr.h, maintaining clean header dependencies.

## Parameters / Member Variables
- `snapshot`: The snapshot structure to install as the transaction snapshot (typically from RestoreSnapshot)
- `*source_pgproc`: Pointer to the PGPROC structure of the source process (cast to void* for header independence)
## Dependencies
- Functions called/Symbols referenced:
  - [SetTransactionSnapshot](../S/SetTransactionSnapshot.md) (core function for installing transaction snapshots)
  - InvalidPid (constant indicating no specific process ID)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel worker process initialization)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (replication slot creation)

## Notes and Other Information
- This is essentially a thin wrapper around SetTransactionSnapshot with preset parameters
- The void* parameter type for source_pgproc maintains header file independence
- Used primarily in parallel execution contexts where workers need to adopt the main process's snapshot
- The InvalidPid parameter indicates that no specific process ID tracking is needed for this snapshot installation

## Simplified Source

```c
// Simplified version of RestoreTransactionSnapshot
void RestoreTransactionSnapshot(Snapshot snapshot, void *source_pgproc) {
    // Install the restored snapshot as the transaction snapshot
    SetTransactionSnapshot(snapshot, NULL, InvalidPid, source_pgproc);
}
```

Key simplifications made:
- Simple wrapper function with clear purpose
- Maintained void* typing for header independence
- Preserved all essential parameters
- Added descriptive comment