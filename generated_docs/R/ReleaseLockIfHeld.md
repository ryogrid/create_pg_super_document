# ReleaseLockIfHeld

## Location
[src/backend/storage/lmgr/lock.c:2509-2568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2509-L2568)

## Overview
ReleaseLockIfHeld releases locks held by a specific resource owner (session-level or current resource owner) for a given LOCALLOCK, handling partial and complete lock releases.

## Definition
```c
static void ReleaseLockIfHeld(LOCALLOCK *locallock, bool sessionLock)
```

## Detailed Description
This internal function handles the conditional release of locks based on ownership. It determines the target owner (NULL for session locks, CurrentResourceOwner otherwise) and scans the lock owners array to find matching ownership entries. For partial releases (where the owner holds fewer locks than the total), it decrements the lock count and compacts the owners array. For complete releases (where the owner holds all the locks), it calls LockRelease to perform the actual lock release operation. The function includes careful reference counting and memory management to maintain the integrity of the lock ownership structure.

## Parameters / Member Variables
- `locallock`: Pointer to the LOCALLOCK structure representing the local view of the lock to potentially release.
- `sessionLock`: Boolean flag indicating whether to release session-level locks (true) or locks owned by the current resource owner (false).

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetLock](ResourceOwnerForgetLock.md): Removes lock reference from resource owner's tracking
  - [LockRelease](../L/LockRelease.md): Performs the actual lock release operation
  - Assert: Debug assertion macro
  - elog: Error logging function
- Called from (representative examples):
  - [LockReleaseSession](../L/LockReleaseSession.md): To release session-level locks
  - [LockReleaseCurrentOwner](../L/LockReleaseCurrentOwner.md): To release transaction-level locks
  - PROCLOCK_PRINT: Used in debug/logging contexts

## Notes and Other Information
- This is an internal static function not exposed in the public API
- Handles complex ownership scenarios where multiple resource owners may hold the same lock
- Performs careful array compaction when removing lock ownership entries to avoid memory fragmentation
- The function includes a warning log if LockRelease unexpectedly fails
- Comments indicate potential for future refactoring to avoid hashtable lookups and support arbitrary resource owners
- Lock owners array is processed in reverse order (from end to beginning) to safely handle array compaction during iteration
- The function maintains the invariant that total lock count equals the sum of all owner lock counts