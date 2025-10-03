# ReplicationSlotMarkDirty

## Location
[src/backend/replication/slot.c:1010-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1010-L1026)

## Overview
Marks the currently acquired replication slot as dirty, signaling that it should be flushed to disk when convenient.

## Definition

```c
void
ReplicationSlotMarkDirty(void)
```
## Detailed Description
This function provides a lightweight mechanism to indicate that a replication slot's state has changed and should eventually be persisted to disk. It sets both the 'dirty' and 'just_dirtied' flags on the current slot to signal that the slot contains unpersisted changes. The function is designed to be fast and non-blocking, as the actual disk flush is deferred and can be performed later when appropriate.

The 'dirty' flag indicates the slot has changes that need to be saved, while 'just_dirtied' is typically used to track recent modifications for optimization purposes. This lazy approach to disk persistence allows for better performance by batching disk writes and avoiding frequent I/O operations during normal replication activity.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [ReplicationSlot](ReplicationSlot.md) (struct type)
- Called from (representative examples):
  - [CreateInitDecodingContext](../C/CreateInitDecodingContext.md)
  - [CreateDecodingContext](../C/CreateDecodingContext.md)
  - [LogicalConfirmReceivedLocation](../L/LogicalConfirmReceivedLocation.md)
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [ReplicationSlotAlter](ReplicationSlotAlter.md)
  - [PhysicalConfirmReceivedLocation](../P/PhysicalConfirmReceivedLocation.md)
  - [PhysicalReplicationSlotNewXmin](../P/PhysicalReplicationSlotNewXmin.md)

## Notes and Other Information
- Requires that MyReplicationSlot is not NULL (verified by Assert)
- Uses spinlock for thread-safe modification of slot flags
- Does not immediately flush to disk - use ReplicationSlotSave() for immediate persistence
- Called frequently during replication operations to track state changes
- The 'just_dirtied' flag allows background processes to identify recently modified slots
- Part of PostgreSQL's lazy evaluation strategy for slot persistence
- Essential for tracking when slots need to be checkpointed or saved during shutdown

## Simplified Source

```c
// Simplified version of ReplicationSlotMarkDirty
void ReplicationSlotMarkDirty(void) {
    ReplicationSlot *slot = MyReplicationSlot;

    Assert(MyReplicationSlot != NULL);

    // Atomically mark the slot as dirty
    SpinLockAcquire(&slot->mutex);
    MyReplicationSlot->just_dirtied = true;
    MyReplicationSlot->dirty = true;
    SpinLockRelease(&slot->mutex);
}
```

Key simplifications made:
- Added clear comment for the atomic operation
- Maintained essential thread safety with spinlock
- Preserved both dirty flags for proper tracking
- Simple and focused on core functionality