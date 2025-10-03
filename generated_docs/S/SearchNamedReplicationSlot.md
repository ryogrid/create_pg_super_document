# SearchNamedReplicationSlot

## Location
[src/backend/replication/slot.c:464-496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L464-L496)

## Overview
Searches for a replication slot by name in the shared memory array and returns a pointer to it if found.

## Definition

```c
ReplicationSlot *
SearchNamedReplicationSlot(const char *name, bool need_lock)
```
## Detailed Description
SearchNamedReplicationSlot performs a linear search through the max_replication_slots array in shared memory to find a replication slot with the specified name. The function provides flexible locking behavior based on the need_lock parameter, allowing callers to control whether they need the function to acquire the ReplicationSlotControlLock or if they already hold appropriate locks. The search compares slot names using string comparison and only considers slots that are marked as in_use.

## Parameters / Member Variables
- `*name`: The name of the replication slot to search for
- `need_lock`: If true, the function acquires and releases ReplicationSlotControlLock; if false, assumes caller already holds appropriate locks
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (when need_lock is true)
  - strcmp for name comparison
  - NameStr macro for accessing slot names
- Called from (representative examples):
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)
  - [validate_sync_standby_slots](../v/validate_sync_standby_slots.md)
  - [StandbySlotsHaveCaughtup](StandbySlotsHaveCaughtup.md)

## Notes and Other Information
- Returns NULL if no slot with the specified name is found
- Uses linear search through the replication slots array, which is acceptable given the typically small number of slots
- The need_lock parameter provides flexibility for different calling contexts where locks may already be held
- Only searches slots that are marked as in_use, ignoring freed slots
- Thread-safe when used with appropriate locking (either via need_lock=true or caller-managed locks)

## Simplified Source

```c
// Simplified version of SearchNamedReplicationSlot
ReplicationSlot *
SearchNamedReplicationSlot(const char *name, bool need_lock)
{
    ReplicationSlot *found_slot = NULL;

    // Acquire lock if requested by caller
    if (need_lock)
        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    // Linear search through all replication slots
    for (int i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

        // Check if slot is active and name matches
        if (slot->in_use && strcmp(name, NameStr(slot->data.name)) == 0) {
            found_slot = slot;
            break;  // Found it, stop searching
        }
    }

    // Release lock if we acquired it
    if (need_lock)
        LWLockRelease(ReplicationSlotControlLock);

    return found_slot;  // NULL if not found
}
```

Key simplifications made:
- Used more descriptive variable names (`found_slot` instead of `slot`)
- Added clear comments explaining each major step
- Simplified the loop variable declaration to modern C style
- Made the logic flow more explicit with comments
- Clarified the return behavior in comments