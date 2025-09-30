# ReplicationSlotsCountDBSlots

## Location
[src/backend/replication/slot.c:1212-1269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1212-L1269)

## Overview
Counts the number of logical replication slots that refer to a specific database, providing both total slot count and count of currently active slots.

## Definition
```c
bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive)
```

## Detailed Description
This function iterates through all replication slots to count how many logical slots are associated with a specific database identified by its OID. It provides separate counts for total slots and currently active slots (those with active_pid != 0).

The function only considers logical replication slots since physical replication slots are not database-specific. It intentionally counts invalidated slots as well, since they still represent a reference to the database even if they're not currently functional.

Returns true if any slots reference the specified database, false otherwise. The counts are returned through the provided output parameters.

## Parameters / Member Variables
- `dboid`: Database OID to search for in slot references
- `nslots`: Output parameter - total number of logical slots referencing the database (including invalidated ones)
- `nactive`: Output parameter - number of currently active logical slots referencing the database
- **Return value**: `true` if any slots reference the database, `false` otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with LW_SHARED mode)
  - [LWLockRelease](../L/LWLockRelease.md)
  - SpinLockAcquire
  - SpinLockRelease
  - SlotIsLogical
  - [ReplicationSlot](ReplicationSlot.md) (struct access)

- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (database drop command)

## Notes and Other Information
- Only considers logical replication slots since physical slots are not database-specific
- Intentionally counts invalidated slots as they still represent database references
- Uses proper locking: shared control lock followed by individual slot mutexes
- Early return with false if max_replication_slots <= 0 (replication slots disabled)
- Active slots are identified by having a non-zero active_pid
- This function is commonly used in database management operations to check if a database can be safely dropped

## Simplified Source

```c
bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive) {
    int i;

    // Initialize counters
    *nslots = *nactive = 0;

    // Early return if replication slots disabled
    if (max_replication_slots <= 0)
        return false;

    // Iterate through all slots with shared lock
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

        // Skip unused slots
        if (!s->in_use)
            continue;

        // Only count logical slots (physical slots aren't database-specific)
        if (!SlotIsLogical(s))
            continue;

        // Skip slots for other databases
        if (s->data.database != dboid)
            continue;

        // Count this slot (including invalidated ones)
        SpinLockAcquire(&s->mutex);
        (*nslots)++;
        if (s->active_pid != 0)
            (*nactive)++;
        SpinLockRelease(&s->mutex);
    }
    LWLockRelease(ReplicationSlotControlLock);

    return (*nslots > 0);
}
```