# get_local_synced_slots

## Location
[src/backend/replication/logical/slotsync.c:333-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L333-L363)

## Overview
Retrieves a list of all local logical replication slots that are synchronized from the primary server.

## Definition
```c
static List *get_local_synced_slots(void)
```

## Detailed Description
This function iterates through all replication slots in the system and collects those that are marked as synchronized slots. It acquires a shared lock on the ReplicationSlotControlLock to safely access the slot control structure and examine each slot's properties. Only slots that are currently in use and have the 'synced' flag set are included in the returned list.

The function performs validation by asserting that all synchronized slots must be logical slots, as physical slots cannot be synchronized in this context.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - SlotIsLogical
  - [lappend](../l/lappend.md)
- Called from:
  - [drop_local_obsolete_slots](../d/drop_local_obsolete_slots.md)

## Notes and Other Information
- Returns a List pointer containing ReplicationSlot pointers for all synchronized slots
- Uses shared locking to allow concurrent reads while preventing modifications during iteration
- Includes an assertion to verify that all synced slots are logical slots, helping catch programming errors
- The returned list should be freed by the caller when no longer needed
- An empty list (NIL) is returned if no synchronized slots exist