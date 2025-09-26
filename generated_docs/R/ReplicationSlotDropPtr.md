# ReplicationSlotDropPtr

## Location
[src/backend/replication/slot.c:885-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L885-L991)

## Overview
Permanently drops a replication slot, removing all associated files and metadata from disk and memory.

## Definition

```c
static void
ReplicationSlotDropPtr(ReplicationSlot *slot)
```
## Detailed Description
This function performs the complete and permanent removal of a replication slot. It handles both the logical cleanup (marking the slot as inactive, removing it from memory) and physical cleanup (removing directory and files from disk). The function is designed to be crash-safe and handles concurrent operations through proper locking mechanisms.

The function performs these key operations:
1. Acquires ReplicationSlotAllocationLock to prevent concurrent slot creation/deletion
2. Renames the slot directory to a temporary name to invalidate it immediately
3. Performs crash-safe filesystem operations with fsync
4. Updates slot metadata to mark it as inactive
5. Recomputes replication limits since the slot no longer constrains resource cleanup
6. Removes the temporary directory and associated statistics
7. Handles both persistent and ephemeral slots with appropriate error handling

## Parameters / Member Variables
- : Pointer to the ReplicationSlot structure to be dropped permanently

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (ReplicationSlotAllocationLock, ReplicationSlotControlLock)
  - [LWLockRelease](../L/LWLockRelease.md)
  - rename
  - [fsync_fname](../f/fsync_fname.md)
  - START_CRIT_SECTION/END_CRIT_SECTION
  - SpinLockAcquire/SpinLockRelease
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)
  - [ReplicationSlotsComputeRequiredXmin](ReplicationSlotsComputeRequiredXmin.md)
  - [ReplicationSlotsComputeRequiredLSN](ReplicationSlotsComputeRequiredLSN.md)
  - [rmtree](../r/rmtree.md)
  - SlotIsLogical
  - [pgstat_drop_replslot](../p/pgstat_drop_replslot.md)
- Called from (representative examples):
  - [ReplicationSlotCleanup](ReplicationSlotCleanup.md)
  - [ReplicationSlotDropAcquired](ReplicationSlotDropAcquired.md)

## Notes and Other Information
- The function uses a two-phase approach: first rename to invalidate, then remove completely
- Critical sections ensure crash-safety for filesystem operations
- Different error handling for persistent vs ephemeral/temporary slots (hard error vs warning)
- Recomputes global replication limits after slot removal to allow resource cleanup
- Statistics are only dropped for logical replication slots
- Uses comprehensive locking strategy to handle concurrent operations safely
- Directory removal failure is non-fatal and only generates a warning