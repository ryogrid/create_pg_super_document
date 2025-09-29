# GrantLockLocal

## Location
[src/backend/storage/lmgr/lock.c:1692-1723](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1692-L1723)

## Overview
GrantLockLocal updates the local lock data structures to record that a lock request has been granted, managing both total lock counts and per-owner reference counts.

## Definition

```c
struction here, on architectures where that is supported.
	 */

	SpinLockAcquire(&FastPathStrongRelationLocks->mutex);
```
## Detailed Description
GrantLockLocal is a static function that updates the local lock tracking structures (LOCALLOCK) when a lock has been successfully granted. It maintains two levels of reference counting: a total count of how many times this lock has been acquired by the current backend (nLocks), and per-owner counts that track which resource owners hold references to the lock.

The function first increments the total lock count, then searches through existing lock owners to find a matching ResourceOwner. If found, it increments that owner's count; otherwise, it adds a new owner entry. Finally, it registers the lock with the ResourceOwner for cleanup purposes and marks the lock as held for certain lock types.

## Parameters / Member Variables
- : Pointer to the LOCALLOCK structure representing the backend's local view of the lock
- : The ResourceOwner that should be recorded as holding this lock reference

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRememberLock](../R/ResourceOwnerRememberLock.md)
  - [CheckAndSetLockHeld](../C/CheckAndSetLockHeld.md)
- Data structures used:
  - [LOCALLOCK](../L/LOCALLOCK.md)
  - [LOCALLOCKOWNER](../L/LOCALLOCKOWNER.md)
  - [ResourceOwner](../R/ResourceOwner.md)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - [GrantAwaitedLock](GrantAwaitedLock.md)

## Notes and Other Information
- This is a static function only accessible within lock.c
- The function assumes that LockAcquire has already ensured there is room for a new ResourceOwner entry
- It maintains the invariant that locallock->numLockOwners < locallock->maxLockOwners
- The function handles both NULL and non-NULL ResourceOwner values appropriately
- [CheckAndSetLockHeld](../C/CheckAndSetLockHeld.md) is called to update lock status for specific lock types that require tracking

## Simplified Source

```c
// Simplified version of GrantLockLocal
static void GrantLockLocal(LOCALLOCK *locallock, ResourceOwner owner) {
    LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
    int i;

    // Increment total lock count for this backend
    locallock->nLocks++;

    // Find existing owner or add new one
    for (i = 0; i < locallock->numLockOwners; i++) {
        if (lockOwners[i].owner == owner) {
            // Found existing owner, increment their count
            lockOwners[i].nLocks++;
            return;
        }
    }

    // Owner not found, add as new entry
    lockOwners[i].owner = owner;
    lockOwners[i].nLocks = 1;
    locallock->numLockOwners++;

    // Register lock with resource owner for cleanup
    if (owner != NULL) {
        ResourceOwnerRememberLock(owner, locallock);
    }

    // Mark lock as held for tracking purposes
    CheckAndSetLockHeld(locallock, true);
}
```

Key simplifications made:
- Added descriptive comments for each logical section
- Simplified variable declarations for clarity
- Made the two-phase logic (search then add) more explicit
- Condensed the owner search loop with clearer comments
- Emphasized the core purpose: increment counts and track ownership