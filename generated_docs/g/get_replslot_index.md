# get_replslot_index

## Location
[src/backend/utils/activity/pgstat_replslot.c:224-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L224-L236)

## Overview
A static helper function that retrieves the array index of a replication slot given its name, used internally by the replication slot statistics subsystem.

## Definition
```c
static int get_replslot_index(const char *name, bool need_lock)
```

## Detailed Description
This function serves as a utility for the replication slot statistics system to convert a replication slot name into its corresponding array index. It searches for a named replication slot using the PostgreSQL replication slot management system and returns the slot's index position in the replication slot array. The function provides an option to control whether locking is needed during the search operation, making it flexible for different calling contexts.

If the named slot is not found, the function returns -1 to indicate failure, allowing callers to handle missing slots appropriately.

## Parameters / Member Variables
- `name`: C string containing the name of the replication slot to look up (must not be NULL)
- `need_lock`: Boolean flag indicating whether the search operation should acquire locks for thread safety

## Dependencies
- Functions called/Symbols referenced:
  - [SearchNamedReplicationSlot](../S/SearchNamedReplicationSlot.md)
  - [ReplicationSlotIndex](../R/ReplicationSlotIndex.md)
  - Assert (macro)
- Types referenced:
  - [ReplicationSlot](../R/ReplicationSlot.md)
- Called from (representative examples):
  - pgstat_fetch_replslot (src/backend/utils/activity/pgstat_replslot.c:177)
  - [pgstat_replslot_from_serialized_name_cb](../p/pgstat_replslot_from_serialized_name_cb.md) (src/backend/utils/activity/pgstat_replslot.c:204)

## Notes and Other Information
- Static function - only visible within the pgstat_replslot.c source file
- Returns -1 if the named replication slot cannot be found
- Includes assertion to ensure name parameter is not NULL
- The need_lock parameter allows for optimized usage in contexts where locks are already held
- Used as a bridge between slot names and array indices for statistics operations
- Located in src/backend/utils/activity/pgstat_replslot.c:224-236