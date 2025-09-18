# CheckAndSetLockHeld

## Location
[src/backend/storage/lmgr/lock.c:1364-1375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1364-L1375)

## Overview
CheckAndSetLockHeld is a debug utility function that tracks whether the current process holds a relation extension lock, used for assertion checking to prevent improper lock nesting.

## Definition


## Detailed Description
CheckAndSetLockHeld is a specialized tracking function that maintains the global flag IsRelationExtensionLockHeld to indicate whether the current backend holds a relation extension lock. This function is only active when assertion checking is enabled (USE_ASSERT_CHECKING), serving as a debugging aid to detect improper lock acquisition patterns.

Relation extension locks are special locks used when extending relations (adding new blocks), and PostgreSQL enforces that no other heavyweight locks should be acquired while holding a relation extension lock. This function helps enforce that invariant by tracking the state and allowing other parts of the system to assert against improper nesting.

## Parameters / Member Variables
- : Pointer to LOCALLOCK structure representing the lock being acquired or released
- : Boolean indicating whether the lock was acquired (true) or released (false)

## Dependencies
- Functions called/Symbols referenced:
  - LOCALLOCK_LOCKTAG (macro to extract lock tag from LOCALLOCK)
  - LOCKTAG_RELATION_EXTEND (constant identifying relation extension locks)
- Data structures used:
  - [LOCALLOCK](../L/LOCALLOCK.md) (local lock table entry)
  - IsRelationExtensionLockHeld (global state flag)
- Called from (representative examples):
  - [GrantLockLocal](../G/GrantLockLocal.md) (when acquiring locks locally)
  - [RemoveLocalLock](../R/RemoveLocalLock.md) (when releasing locks locally)

## Notes and Other Information
- Only compiled and active when USE_ASSERT_CHECKING is defined
- Specifically tracks LOCKTAG_RELATION_EXTEND locks, ignoring all other lock types
- Sets IsRelationExtensionLockHeld global variable to track state for assertion checking
- Part of PostgreSQL's debugging infrastructure to catch lock ordering violations
- Helps enforce the constraint that relation extension locks should not be held simultaneously with other heavyweight locks
- The function is inline for performance in debug builds since it's called frequently