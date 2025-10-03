# ReplicationSlotName

## Location
[src/backend/replication/slot.c:513-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L513-L539)

## Overview
Safely retrieves the name of a replication slot at a given index, returning whether the slot is in use.

## Definition

```c
bool
ReplicationSlotName(int index, Name name)
```
## Detailed Description
ReplicationSlotName provides a thread-safe way to retrieve the name of a replication slot at a specific index in the replication slots array. The function uses shared locking to ensure the slot cannot be dropped while copying the name, though it notes that the name of an existing slot cannot change so a spinlock is not needed. The function returns a boolean indicating whether the slot at the specified index is actually in use, making it safe to call with any valid index.

The function includes a warning about Time-of-Check-Time-of-Use (TOCTOU) issues, noting that it's primarily intended for use by pgstat_replslot.c during shutdown when such race conditions are less of a concern.

## Parameters / Member Variables
- `index`: The zero-based index of the slot in the replication slots array
- `name`: Output parameter - a Name structure that will receive the slot name if the slot is in use
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease with LW_SHARED
  - [namestrcpy](../n/namestrcpy.md) for copying the slot name
  - NameStr macro for accessing slot names
- Called from (representative examples):
  - [pgstat_replslot_to_serialized_name_cb](../p/pgstat_replslot_to_serialized_name_cb.md)

## Notes and Other Information
- Returns false if the slot at the given index is not in use, true otherwise
- Uses shared locking to prevent slot deletion during name copying
- Primarily designed for statistics subsystem use during shutdown
- Has inherent TOCTOU issues that limit its usefulness in most contexts
- Does not validate the index parameter - caller must ensure it's within valid range
- The slot name cannot change once set, so no additional synchronization is needed beyond preventing slot deletion