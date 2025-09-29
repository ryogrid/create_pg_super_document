# LockReassignOwner

## Location
[src/backend/storage/lmgr/lock.c:2599-2644](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2599-L2644)

## Overview
LockReassignOwner is an internal subroutine that transfers ownership of a specific lock from the current resource owner to its parent resource owner, handling both simple reassignment and count merging scenarios.

## Definition
```c
static void LockReassignOwner(LOCALLOCK *locallock, ResourceOwner parent)
```

## Detailed Description
This internal static function performs the core logic of lock ownership reassignment by scanning the lock owners array to locate entries for both the current resource owner and the parent resource owner. It handles two scenarios: if the parent doesn't already own the lock, it simply transfers ownership of the current owner's slot; if the parent already owns the lock, it merges the lock counts and compacts the owners array by removing the current owner's slot. The function maintains proper resource owner tracking by calling ResourceOwnerRememberLock for the parent and ResourceOwnerForgetLock for the current owner.

## Parameters / Member Variables
- `locallock`: Pointer to the LOCALLOCK structure representing the lock whose ownership should be reassigned.
- `parent`: ResourceOwner that should become the new owner of the lock (typically the parent of CurrentResourceOwner).

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRememberLock](../R/ResourceOwnerRememberLock.md): Registers the lock with the new resource owner
  - [ResourceOwnerForgetLock](../R/ResourceOwnerForgetLock.md): Removes the lock from the current resource owner's tracking
- Called from (representative examples):
  - [LockReassignCurrentOwner](LockReassignCurrentOwner.md): Primary caller for bulk lock reassignment operations
  - PROCLOCK_PRINT: Used in debug/logging contexts

## Notes and Other Information
- This is an internal static function not exposed in the public API
- Uses backward iteration (i = numLockOwners - 1; i >= 0; i--) to safely handle array modifications during scanning
- Handles the case where the current resource owner has no locks on the given locallock by returning early
- When merging lock counts (parent already has locks), performs array compaction to avoid fragmentation
- The function maintains the critical invariant that resource owners must be properly tracked for all lock ownership
- [Variables](../V/Variables.md) ic and ip track indices of current owner and parent owner respectively (-1 indicates not found)
- Array compaction involves moving the last element to fill the gap left by the removed current owner entry

## Simplified Source

```c
// Simplified version of LockReassignOwner
static void
LockReassignOwner(LOCALLOCK *locallock, ResourceOwner parent)
{
    LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
    int current_index = -1;
    int parent_index = -1;

    // Step 1: Find current owner and parent owner in the lock owners array
    for (int i = locallock->numLockOwners - 1; i >= 0; i--) {
        if (lockOwners[i].owner == CurrentResourceOwner)
            current_index = i;
        else if (lockOwners[i].owner == parent)
            parent_index = i;
    }

    // Step 2: Exit early if current owner has no locks
    if (current_index < 0)
        return;

    // Step 3: Handle ownership transfer
    if (parent_index < 0) {
        // Parent has no existing locks - simple transfer
        lockOwners[current_index].owner = parent;
        ResourceOwnerRememberLock(parent, locallock);
    } else {
        // Parent already has locks - merge counts and compact array
        lockOwners[parent_index].nLocks += lockOwners[current_index].nLocks;

        // Remove current owner's slot by compacting array
        locallock->numLockOwners--;
        if (current_index < locallock->numLockOwners)
            lockOwners[current_index] = lockOwners[locallock->numLockOwners];
    }

    // Step 4: Update resource owner tracking
    ResourceOwnerForgetLock(CurrentResourceOwner, locallock);
}
```

Key simplifications made:
- Simplified variable declarations and initialization
- Added descriptive comments for each major logic step
- Renamed variables for clarity (ic → current_index, ip → parent_index)
- Grouped related operations together logically
- Reduced complex conditional nesting
- Made the two-phase logic (simple transfer vs merge) more explicit
- Maintained all essential functionality and error handling