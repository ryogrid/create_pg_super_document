# ReplicationSlotCleanup

## Location
[src/backend/replication/slot.c:745-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L745-L783)

## Overview
Cleans up temporary replication slots created in the current session, with an option to clean up only synced temporary slots or all temporary slots.

## Definition
void ReplicationSlotCleanup(bool synced_only)

## Detailed Description
ReplicationSlotCleanup iterates through all replication slots in the system and removes temporary slots that were created by the current process. The function provides flexibility through the synced_only parameter:

- When synced_only is true, it only removes temporary slots that are marked as synced
- When synced_only is false, it removes all temporary slots owned by the current process

The function uses a restart mechanism to handle the case where slots are dropped during iteration, which requires reacquiring locks and starting the search over. This ensures safe cleanup even when the slot array structure changes during processing.

## Parameters / Member Variables
- : Boolean flag that determines the scope of cleanup - if true, only synced temporary slots are cleaned up; if false, all temporary slots owned by the process are cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotDropPtr](ReplicationSlotDropPtr.md)
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)
  - LW_SHARED (lock mode)
  - RS_TEMPORARY (slot persistency type)
- Called from (representative examples):
  - [slotsync_worker_onexit](../s/slotsync_worker_onexit.md)
  - [ReplicationSlotShmemExit](ReplicationSlotShmemExit.md)
  - [WalSndErrorCleanup](../W/WalSndErrorCleanup.md)
  - [PostgresMain](../P/PostgresMain.md)
  - [SyncReplicationSlots](../S/SyncReplicationSlots.md)

## Notes and Other Information
- Only operates on temporary slots (RS_TEMPORARY persistency)
- Uses a restart loop to handle concurrent modifications to the slot array
- Requires that MyReplicationSlot be NULL before calling
- Properly manages locking to avoid deadlocks during slot cleanup
- Broadcasts condition variables to wake up processes waiting on cleaned-up slots
- Primarily used for session cleanup and error recovery scenarios

## Simplified Source

```c
// Simplified version of ReplicationSlotCleanup
void ReplicationSlotCleanup(bool synced_only) {
    int i;

    // Ensure we're not currently using any replication slot
    Assert(MyReplicationSlot == NULL);

restart:
    // Acquire shared lock on replication slot control structure
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    // Iterate through all replication slots
    for (i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

        // Skip unused slots
        if (!slot->in_use)
            continue;

        // Check if this slot belongs to current process and matches cleanup criteria
        SpinLockAcquire(&slot->mutex);
        if (slot->active_pid == MyProcPid &&
            (!synced_only || slot->data.synced)) {

            // Ensure it's a temporary slot (should always be true)
            Assert(slot->data.persistency == RS_TEMPORARY);

            // Release locks to avoid deadlock during slot drop
            SpinLockRelease(&slot->mutex);
            LWLockRelease(ReplicationSlotControlLock);

            // Drop the slot
            ReplicationSlotDropPtr(slot);

            // Wake up any processes waiting on this slot
            ConditionVariableBroadcast(&slot->active_cv);

            // Restart iteration since slot array may have changed
            goto restart;
        } else {
            SpinLockRelease(&slot->mutex);
        }
    }

    // Release the control lock
    LWLockRelease(ReplicationSlotControlLock);
}
```

Key simplifications made:
- Added clear comments explaining each major step
- Simplified variable names (s → slot) for better readability
- Consolidated the main logic flow with descriptive comments
- Explained the restart mechanism and why locks are released
- Focused on the core algorithm: iterate, check ownership, drop if matches criteria
- Preserved the essential locking protocol and error handling structure