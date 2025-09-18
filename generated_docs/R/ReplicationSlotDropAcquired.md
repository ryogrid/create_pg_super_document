# ReplicationSlotDropAcquired

## Location
src/backend/replication/slot.c: 868 - 884

## Overview
Permanently drops the currently acquired replication slot, serving as a low-level function that assumes the slot is already held by the calling backend.

## Definition
void ReplicationSlotDropAcquired(void)

## Detailed Description
ReplicationSlotDropAcquired is a lower-level function that permanently removes the replication slot that is currently held by the calling backend (stored in MyReplicationSlot). Unlike ReplicationSlotDrop which acquires a slot by name, this function operates on the assumption that the slot is already acquired.

The function performs minimal validation - it only ensures that MyReplicationSlot is not NULL - and then:
1. Captures a reference to the current slot
2. Clears the MyReplicationSlot global variable to indicate the slot is no longer acquired
3. Delegates the actual dropping logic to ReplicationSlotDropPtr

This function is typically used in scenarios where the slot is already held and needs to be dropped as part of a larger operation.

## Parameters / Member Variables
This function takes no parameters and operates on the global MyReplicationSlot variable.

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotDropPtr](ReplicationSlotDropPtr.md)
  - [ReplicationSlot](ReplicationSlot.md) (data type)
- Called from (representative examples):
  - [ReplicationSlotRelease](ReplicationSlotRelease.md)
  - [ReplicationSlotDrop](ReplicationSlotDrop.md)
  - [drop_local_obsolete_slots](../d/drop_local_obsolete_slots.md)
  - [ReplicationSlotsDropDBSlots](ReplicationSlotsDropDBSlots.md)

## Notes and Other Information
- Requires that MyReplicationSlot be non-NULL (a slot must be currently acquired)
- Does not perform slot acquisition - assumes the caller has already acquired the target slot
- Clears MyReplicationSlot before delegating to ReplicationSlotDropPtr for the actual cleanup
- Used internally by higher-level slot management functions
- Provides the core dropping functionality for both ephemeral slot cleanup and explicit slot deletion
- Does not include the validation checks present in ReplicationSlotDrop (such as synced slot protection)