# pgstat_drop_replslot

## Location
[src/backend/utils/activity/pgstat_replslot.c:156-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L156-L169)

## Overview
Reports the dropping of a replication slot and removes its associated statistics entry from the shared statistics hash table.

## Definition
```c
void pgstat_drop_replslot(ReplicationSlot *slot)
```

## Detailed Description
This function is called when a replication slot is being dropped to clean up its statistics entry. It operates under the assumption that the ReplicationSlotAllocationLock is already held exclusively. The function attempts to drop the statistics entry for the slot from the shared statistics hash table. If the drop operation fails (indicating the entry couldn't be immediately removed), it requests garbage collection of entry references to ensure eventual cleanup. This maintains the integrity of the statistics system by preventing accumulation of stale entries.

## Parameters / Member Variables
- `slot`: Pointer to the ReplicationSlot structure that is being dropped

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode (with ReplicationSlotAllocationLock, LW_EXCLUSIVE)
  - pgstat_drop_entry (with PGSTAT_KIND_REPLSLOT)
  - ReplicationSlotIndex
  - pgstat_request_entry_refs_gc (if drop fails)
- Called from (representative examples):
  - ReplicationSlotDropPtr

## Notes and Other Information
- Must be called with ReplicationSlotAllocationLock already held exclusively
- Ensures proper cleanup of statistics when slots are removed
- Requests garbage collection if immediate entry removal fails
- Part of the replication slot lifecycle management in the statistics system
- Works in conjunction with pgstat_acquire_replslot() for complete slot statistics lifecycle
- Helps prevent memory leaks in the statistics hash table