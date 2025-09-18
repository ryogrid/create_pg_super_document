# GrantLockLocal

## Location
src/backend/storage/lmgr/lock.c: 1692 - 1723

## Overview
GrantLockLocal updates the local lock data structures to record that a lock request has been granted, managing both total lock counts and per-owner reference counts.

## Definition


## Detailed Description
GrantLockLocal is a static function that updates the local lock tracking structures (LOCALLOCK) when a lock has been successfully granted. It maintains two levels of reference counting: a total count of how many times this lock has been acquired by the current backend (nLocks), and per-owner counts that track which resource owners hold references to the lock.

The function first increments the total lock count, then searches through existing lock owners to find a matching ResourceOwner. If found, it increments that owner's count; otherwise, it adds a new owner entry. Finally, it registers the lock with the ResourceOwner for cleanup purposes and marks the lock as held for certain lock types.

## Parameters / Member Variables
- : Pointer to the LOCALLOCK structure representing the backend's local view of the lock
- : The ResourceOwner that should be recorded as holding this lock reference

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRememberLock
  - CheckAndSetLockHeld
- Data structures used:
  - LOCALLOCK
  - LOCALLOCKOWNER
  - ResourceOwner
- Called from (representative examples):
  - LockAcquireExtended
  - GrantAwaitedLock

## Notes and Other Information
- This is a static function only accessible within lock.c
- The function assumes that LockAcquire has already ensured there is room for a new ResourceOwner entry
- It maintains the invariant that locallock->numLockOwners < locallock->maxLockOwners
- The function handles both NULL and non-NULL ResourceOwner values appropriately
- CheckAndSetLockHeld is called to update lock status for specific lock types that require tracking