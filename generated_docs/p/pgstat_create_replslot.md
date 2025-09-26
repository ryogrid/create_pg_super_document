# pgstat_create_replslot

## Location
[src/backend/utils/activity/pgstat_replslot.c:111-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L111-L145)

## Overview
Reports the creation of a replication slot by initializing its statistics entry in the shared statistics hash table.

## Definition
```c
void pgstat_create_replslot(ReplicationSlot *slot)
```

## Detailed Description
This function is called when a new replication slot is created to set up its statistics tracking. It operates under the assumption that the ReplicationSlotAllocationLock is already held exclusively. The function retrieves or creates a statistics entry for the slot in the shared statistics hash table, then initializes the statistics structure to zero. This ensures that any previous statistics from an older slot with the same index (which could happen after a crash and recovery) are cleared.

## Parameters / Member Variables
- `slot`: Pointer to the ReplicationSlot structure for which statistics should be initialized

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode (with ReplicationSlotAllocationLock, LW_EXCLUSIVE)
  - pgstat_get_entry_ref_locked (with PGSTAT_KIND_REPLSLOT)
  - ReplicationSlotIndex
  - memset (to clear statistics)
  - pgstat_unlock_entry
- Called from (representative examples):
  - ReplicationSlotCreate

## Notes and Other Information
- Must be called with ReplicationSlotAllocationLock already held exclusively
- Careful consideration needed when calling back into slot.c due to locking constraints
- Handles the case where statistics might exist from a previously dropped slot with the same index
- Part of PostgreSQL's statistics infrastructure for monitoring replication slot usage
- The function clears any existing statistics to ensure a clean state for the new slot