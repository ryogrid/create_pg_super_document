# LockAcquire

## Location
[src/backend/storage/lmgr/lock.c:756-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L756-L779)

## Overview
LockAcquire is the primary entry point for acquiring locks in PostgreSQL, providing a simplified interface that delegates to LockAcquireExtended for the actual lock acquisition logic.

## Definition

```c
*/
LockAcquireResult
LockAcquire(const LOCKTAG *locktag,
			LOCKMODE lockmode,
			bool sessionLock,
			bool dontWait)
```
## Detailed Description
LockAcquire serves as the main public API for lock acquisition in PostgreSQL's lock manager. It acts as a wrapper around LockAcquireExtended, providing a simplified interface with commonly used default parameters. The function handles conflict detection and either waits for lock availability or returns immediately based on the dontWait parameter.

The function can return different results indicating whether the lock was freshly acquired, already held by the current transaction, or unavailable. This information is crucial for optimizing lock management and understanding transaction behavior.

## Parameters / Member Variables
- `*locktag`: Pointer to LOCKTAG structure that uniquely identifies the lockable object (relation, tuple, page, etc.)
- `lockmode`: The specific lock mode to acquire (e.g., AccessShareLock, RowExclusiveLock, AccessExclusiveLock)
- `sessionLock`: If true, acquire lock for the entire session rather than just the current transaction
- `dontWait`: If true, return immediately if lock cannot be acquired without waiting
## Dependencies
- Functions called/Symbols referenced:
  - [LockAcquireExtended](LockAcquireExtended.md) (the actual implementation with extended parameters)
- Data structures used:
  - [LOCKTAG](LOCKTAG.md) (lock identifier structure)
  - LockAcquireResult (return value enum)
- Called from (representative examples):
  - [LockRelationIdForSession](LockRelationIdForSession.md) (relation locking)
  - [LockPage](LockPage.md), LockTuple (granular locking)
  - [XactLockTableWait](../X/XactLockTableWait.md) (transaction waiting)
  - pg_advisory_lock_* functions (advisory locking API)
  - Various lmgr.c wrapper functions

## Notes and Other Information
- Returns LOCKACQUIRE_OK for successful acquisition, LOCKACQUIRE_NOT_AVAIL when dontWait=true and lock unavailable
- LOCKACQUIRE_ALREADY_HELD indicates the lock count was incremented for an already-held lock
- The function is transaction-safe and integrates with PostgreSQL's deadlock detection
- Used extensively throughout the system for relation, page, tuple, and advisory locking
- Provides the foundation for all PostgreSQL locking operations

## Simplified Source

```c
// Simplified version of LockAcquire
LockAcquireResult LockAcquire(const LOCKTAG *locktag, LOCKMODE lockmode,
                              bool sessionLock, bool dontWait) {
    // Delegate to extended version with default parameters
    return LockAcquireExtended(locktag, lockmode, sessionLock, dontWait,
                               true, NULL);
}
```

Key simplifications made:
- Focused on the delegation pattern to LockAcquireExtended
- Showed the simplified interface compared to the extended version
- Emphasized the wrapper nature of this function
- Preserved the key parameters that callers need to understand