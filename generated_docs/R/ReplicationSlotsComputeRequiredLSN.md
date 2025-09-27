# ReplicationSlotsComputeRequiredLSN

## Location
[src/backend/replication/slot.c:1105-1153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1105-L1153)

## Overview
Computes the oldest restart LSN across all active replication slots and informs the WAL module of the minimum LSN that must be retained for replication purposes.

## Definition

```c
void
ReplicationSlotsComputeRequiredLSN(void)
```
## Detailed Description
This function iterates through all replication slots to find the oldest (minimum) restart LSN among all active, non-invalidated slots. The computed minimum LSN is then passed to the WAL module via XLogSetReplicationSlotMinimumLSN to ensure that WAL segments at or after this position are retained and not deleted prematurely.

The function acquires a shared lock on ReplicationSlotControlLock to safely read slot data, then examines each slot's restart_lsn while holding the slot's mutex for thread safety. Invalidated slots are skipped as they don't require WAL retention.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with LW_SHARED mode)
  - [LWLockRelease](../L/LWLockRelease.md)
  - SpinLockAcquire
  - SpinLockRelease
  - [XLogSetReplicationSlotMinimumLSN](../X/XLogSetReplicationSlotMinimumLSN.md)
  - [ReplicationSlot](ReplicationSlot.md) (struct access)
  - RS_INVAL_NONE (invalidation state constant)

- Called from (representative examples):
  - [LogicalConfirmReceivedLocation](../L/LogicalConfirmReceivedLocation.md)
  - [ReplicationSlotDropPtr](ReplicationSlotDropPtr.md)
  - [ReplicationSlotReserveWal](ReplicationSlotReserveWal.md)
  - [InvalidateObsoleteReplicationSlots](../I/InvalidateObsoleteReplicationSlots.md)
  - [StartupReplicationSlots](../S/StartupReplicationSlots.md)
  - [PhysicalConfirmReceivedLocation](../P/PhysicalConfirmReceivedLocation.md)

## Notes and Other Information
- The function specifically notes that max_slot_wal_keep_size is theoretically relevant but not accounted for because the module doesn't know what to compare against
- Invalidated slots are deliberately excluded from the computation as they no longer need WAL retention
- The function uses proper locking hierarchy: first acquiring the control lock, then individual slot mutexes
- This is a critical function for WAL retention management in PostgreSQL replication

## Simplified Source

```c
// Simplified version of ReplicationSlotsComputeRequiredLSN
void ReplicationSlotsComputeRequiredLSN(void) {
    int i;
    XLogRecPtr min_required = InvalidXLogRecPtr;

    Assert(ReplicationSlotCtl != NULL);

    // Step 1: Acquire shared lock to safely read all slot data
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    // Step 2: Iterate through all possible replication slots
    for (i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
        XLogRecPtr restart_lsn;
        bool invalidated;

        // Skip unused slots
        if (!s->in_use)
            continue;

        // Step 3: Safely read slot data under its mutex
        SpinLockAcquire(&s->mutex);
        restart_lsn = s->data.restart_lsn;
        invalidated = s->data.invalidated != RS_INVAL_NONE;
        SpinLockRelease(&s->mutex);

        // Skip invalidated slots - they don't need WAL retention
        if (invalidated)
            continue;

        // Step 4: Track the minimum (oldest) restart LSN across all valid slots
        if (restart_lsn != InvalidXLogRecPtr &&
            (min_required == InvalidXLogRecPtr || restart_lsn < min_required)) {
            min_required = restart_lsn;
        }
    }

    LWLockRelease(ReplicationSlotControlLock);

    // Step 5: Inform WAL module of the minimum LSN that must be retained
    XLogSetReplicationSlotMinimumLSN(min_required);
}
```

Key simplifications made:
- Organized into clear sequential steps with descriptive comments
- Preserved the essential locking hierarchy for safe concurrent access
- Maintained the logic for skipping unused and invalidated slots
- Simplified the minimum LSN tracking algorithm while preserving correctness
- Kept the critical communication with the WAL module
- Focused on the core purpose: find minimum restart LSN and inform WAL system
- Explained why invalidated slots are excluded from consideration