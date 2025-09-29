# pgstat_acquire_replslot

## Location
[src/backend/utils/activity/pgstat_replslot.c:146-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L146-L155)

## Overview
Reports that a replication slot has been acquired and ensures a statistics entry exists for subsequent statistics reporting calls.

## Definition
```c
void pgstat_acquire_replslot(ReplicationSlot *slot)
```

## Detailed Description
This function is called when a replication slot is acquired (activated for use). Its primary purpose is to guarantee that a statistics entry exists in the shared statistics hash table, which is necessary for later pgstat_report_replslot() calls to function properly. The function handles crash recovery scenarios intelligently - if PostgreSQL previously crashed, no stats data exists and a new entry is created. If there was no crash, existing statistics are preserved because they legitimately belong to this slot (since pgstat_drop_replslot() would have been called if the slot was dropped, and shutdown cleanup would have removed stats for slots that were removed while shut down).

## Parameters / Member Variables
- `slot`: Pointer to the ReplicationSlot structure that is being acquired

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md) (with PGSTAT_KIND_REPLSLOT, create=true)
  - [ReplicationSlotIndex](../R/ReplicationSlotIndex.md)
- Called from (representative examples):
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)

## Notes and Other Information
- Ensures statistics entry exists before slot usage begins
- Handles crash recovery gracefully by creating new entries when needed
- Preserves existing statistics when no crash occurred
- Works in conjunction with pgstat_drop_replslot() for proper lifecycle management
- The create=true parameter ensures an entry is created if it doesn't exist
- Part of the replication slot statistics infrastructure for monitoring slot usage

## Simplified Source

```c
// Simplified version of pgstat_acquire_replslot
void pgstat_acquire_replslot(ReplicationSlot *slot) {
    // Ensure stats entry exists for this replication slot
    // This guarantees later pgstat_report_replslot() calls will work
    pgstat_get_entry_ref(PGSTAT_KIND_REPLSLOT, InvalidOid,
                         ReplicationSlotIndex(slot), true, NULL);
}
```

Key simplifications made:
- Preserved the core logic: ensuring a statistics entry exists for the slot
- Maintained the essential function call to pgstat_get_entry_ref with correct parameters
- Added clear comment explaining the purpose and guarantee provided
- Removed detailed crash recovery explanation from code (kept in documentation above)
- Function is already quite simple, so minimal changes were needed