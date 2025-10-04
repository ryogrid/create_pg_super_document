# slotsync_worker_onexit

## Location
[src/backend/replication/logical/slotsync.c:1190-1235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1190-L1235)

## Overview
slotsync_worker_onexit is a comprehensive cleanup function that handles replication slot cleanup and shared memory state management when the slot synchronization worker exits.

## Definition
```c
static void slotsync_worker_onexit(int code, Datum arg)
```

## Detailed Description
This function serves as the primary cleanup handler for the slot synchronization worker process. It performs critical cleanup operations similar to WalSndErrorCleanup() to ensure proper resource management during worker termination. The function handles both normal shutdown and error scenarios.

Key operations include:
1. Releasing any active replication slots that the worker may be holding
2. Cleaning up temporary replication slots to prevent resource leaks
3. Updating shared memory state by clearing the worker's PID
4. Resetting synchronization flags if the worker terminated unexpectedly during sync operations

The function ensures that the startup process can properly detect when slot synchronization has finished by managing the 'syncing' flag in shared memory, which is crucial during database promotion scenarios.

## Parameters / Member Variables
- `code`: Exit code of the terminating process (not used in current implementation)
- `arg`: Datum argument (not used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md)
  - [ReplicationSlotCleanup](../R/ReplicationSlotCleanup.md)
  - SpinLockAcquire
  - SpinLockRelease
  - InvalidPid (constant)
- Global variables accessed:
  - MyReplicationSlot
  - SlotSyncCtx
  - syncing_slots
- Called from (representative examples):
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (registered as exit callback at src/backend/replication/logical/slotsync.c:1406)

## Notes and Other Information
- This is a static function, meaning it's only visible within the slotsync.c compilation unit
- Critical for preventing resource leaks and maintaining consistent shared memory state
- Handles both graceful shutdown and error recovery scenarios
- The cleanup logic mirrors WalSndErrorCleanup() for consistency with other replication components
- Essential for proper coordination with the startup process during database promotion
- Uses spinlock protection when modifying shared memory state to ensure thread safety

## Simplified Source

```c
static void slotsync_worker_onexit(int code, Datum arg)
{
    // Release any active replication slots
    if (MyReplicationSlot != NULL)
        ReplicationSlotRelease();

    // Clean up temporary slots to prevent resource leaks
    ReplicationSlotCleanup(false);

    // Update shared memory state with spinlock protection
    SpinLockAcquire(&SlotSyncCtx->mutex);

    // Clear worker PID to indicate process is terminating
    SlotSyncCtx->pid = InvalidPid;

    // Reset syncing flag if process errored out during sync
    if (syncing_slots)
    {
        SlotSyncCtx->syncing = false;
        syncing_slots = false;
    }

    SpinLockRelease(&SlotSyncCtx->mutex);
}
```