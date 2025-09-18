# UnGrantLock

## Location
[src/backend/storage/lmgr/lock.c:1581-1637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1581-L1637)

## Overview
UnGrantLock is the opposite of GrantLock, updating lock and proclock data structures to show that a lock is no longer held and determining if any waiting processes should be awakened.

## Definition
```c
static bool UnGrantLock(LOCK *lock, LOCKMODE lockmode,
                       PROCLOCK *proclock, LockMethod lockMethodTable)
```

## Detailed Description
This static function performs the inverse operation of GrantLock when a lock is being released. It handles several key responsibilities:

1. **Counter Updates**: Decrements both the general request count (`nRequested`) and granted count (`nGranted`), as well as the specific mode counters
2. **Grant Mask Management**: If no more locks of this mode are granted, clears the corresponding bit in the `grantMask`
3. **Wakeup Decision**: Determines whether waiting processes should be awakened by checking if the released lock mode conflicts with any requested modes in the wait mask
4. **Proclock Updates**: Clears the appropriate bit in the proclock's `holdMask` to reflect that this process no longer holds the lock
5. **Assertions**: Validates that lock counts are consistent before making changes

The function returns a boolean indicating whether `ProcLockWakeup` should be called to awaken waiting processes.

## Parameters / Member Variables
- `lock`: Pointer to the LOCK structure representing the locked resource
- `lockmode`: The specific lock mode being released
- `proclock`: Pointer to the PROCLOCK structure representing the process-lock relationship
- `lockMethodTable`: Lock method table containing conflict information

## Dependencies
- Functions called/Symbols referenced:
  - LOCKBIT_OFF (macro for clearing lock mode bits)
  - LOCK_PRINT (debugging macro)
  - PROCLOCK_PRINT (debugging macro)
- Called from (representative examples):
  - [LockRelease](../L/LockRelease.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - [LockRefindAndRelease](../L/LockRefindAndRelease.md)

## Notes and Other Information
- This is a static function internal to lock.c, not exposed externally
- The wakeup logic accounts for MVCC semantics where remaining granted locks might belong to waiters themselves
- Before MVCC, wakeup could be skipped if any locks of the same mode remained, but this optimization is no longer safe
- The function includes comprehensive assertions to verify lock count consistency
- Proper grant mask clearing prevents unnecessary conflict checks when no locks of a mode remain
- The return value guides the caller on whether to perform expensive wakeup operations
- Used primarily during normal lock release but also in cleanup and error recovery scenarios