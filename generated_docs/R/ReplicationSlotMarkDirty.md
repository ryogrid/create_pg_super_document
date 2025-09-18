# ReplicationSlotMarkDirty

## Location
src/backend/replication/slot.c: 1010 - 1026

## Overview
Marks the currently acquired replication slot as dirty, signaling that it should be flushed to disk when convenient.

## Definition


## Detailed Description
This function provides a lightweight mechanism to indicate that a replication slot's state has changed and should eventually be persisted to disk. It sets both the 'dirty' and 'just_dirtied' flags on the current slot to signal that the slot contains unpersisted changes. The function is designed to be fast and non-blocking, as the actual disk flush is deferred and can be performed later when appropriate.

The 'dirty' flag indicates the slot has changes that need to be saved, while 'just_dirtied' is typically used to track recent modifications for optimization purposes. This lazy approach to disk persistence allows for better performance by batching disk writes and avoiding frequent I/O operations during normal replication activity.

## Parameters / Member Variables
- No parameters (operates on the global MyReplicationSlot variable)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [ReplicationSlot](ReplicationSlot.md) (struct type)
- Called from (representative examples):
  - CreateInitDecodingContext
  - CreateDecodingContext
  - LogicalConfirmReceivedLocation
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