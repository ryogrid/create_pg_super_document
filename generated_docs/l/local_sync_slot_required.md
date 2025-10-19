# local_sync_slot_required

## Location
[src/backend/replication/logical/slotsync.c:364-416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L364-L416)

## Overview
Helper function that determines whether a local synchronized slot should be retained by checking if it exists in the remote slots list and validating its invalidation status.

## Definition
```c
static bool local_sync_slot_required(ReplicationSlot *local_slot, List *remote_slots)
```

## Detailed Description
This function implements the logic to determine if a local synchronized replication slot is still required and should be kept. It performs two key checks:

1. **Existence check**: Verifies if the local slot has a corresponding remote slot with the same name
2. **Invalidation status check**: Ensures that if the remote slot is valid, the local slot should also be valid

The function returns false in two scenarios:
- The local slot does not exist in the remote slots list (indicating it was dropped on the primary)
- The local slot is invalidated while the corresponding remote slot is still valid (indicating a synchronization issue)

In all other cases, it returns true, meaning the local slot should be retained.

## Parameters / Member Variables
- `*local_slot`: Pointer to the local ReplicationSlot to be checked
- `*remote_slots`: List of RemoteSlot structures representing slots from the primary server
## Dependencies
- Functions called/Symbols referenced:
  - foreach_ptr (macro for iterating over list)
  - strcmp
  - NameStr (macro to extract string from Name structure)
  - SpinLockAcquire
  - SpinLockRelease
  - RS_INVAL_NONE (invalidation status constant)
- Called from:
  - [drop_local_obsolete_slots](../d/drop_local_obsolete_slots.md)

## Notes and Other Information
- Uses spinlock protection when accessing the local slot's invalidation status to ensure thread safety
- The function name matching is case-sensitive using strcmp
- Returns true if the slot should be kept, false if it should be dropped
- This is a key component in the slot cleanup logic that removes obsolete synchronized slots

## Simplified Source

```c
/*
 * Helper function to check if local_slot is required to be retained.
 *
 * Return false if local_slot does not exist in remote_slots or is
 * invalidated while the corresponding remote slot is still valid.
 */
static bool
local_sync_slot_required(ReplicationSlot *local_slot, List *remote_slots)
{
    bool remote_exists = false;
    bool locally_invalidated = false;

    // Search for matching remote slot by name
    foreach_ptr(RemoteSlot, remote_slot, remote_slots)
    {
        if (strcmp(remote_slot->name, NameStr(local_slot->data.name)) == 0)
        {
            remote_exists = true;

            // Check if local slot is invalidated while remote is valid
            SpinLockAcquire(&local_slot->mutex);
            locally_invalidated =
                (remote_slot->invalidated == RS_INVAL_NONE) &&
                (local_slot->data.invalidated != RS_INVAL_NONE);
            SpinLockRelease(&local_slot->mutex);

            break;
        }
    }

    // Keep slot only if remote exists and local is not invalidated
    return (remote_exists && !locally_invalidated);
}
```