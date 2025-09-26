# LockReassignOwner

## Location
src/backend/storage/lmgr/lock.c: 2599 - 2644

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
  - ResourceOwnerRememberLock: Registers the lock with the new resource owner
  - ResourceOwnerForgetLock: Removes the lock from the current resource owner's tracking
- Called from (representative examples):
  - LockReassignCurrentOwner: Primary caller for bulk lock reassignment operations
  - PROCLOCK_PRINT: Used in debug/logging contexts

## Notes and Other Information
- This is an internal static function not exposed in the public API
- Uses backward iteration (i = numLockOwners - 1; i >= 0; i--) to safely handle array modifications during scanning
- Handles the case where the current resource owner has no locks on the given locallock by returning early
- When merging lock counts (parent already has locks), performs array compaction to avoid fragmentation
- The function maintains the critical invariant that resource owners must be properly tracked for all lock ownership
- Variables ic and ip track indices of current owner and parent owner respectively (-1 indicates not found)
- Array compaction involves moving the last element to fill the gap left by the removed current owner entry