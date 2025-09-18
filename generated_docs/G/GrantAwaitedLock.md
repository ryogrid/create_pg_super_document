# GrantAwaitedLock

## Location
[src/backend/storage/lmgr/lock.c:1789-1801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1789-L1801)

## Overview
GrantAwaitedLock is a wrapper function that calls GrantLockLocal for the specific lock that the current process is waiting on, used primarily when a lock is granted during timeout scenarios.

## Definition
```c
void GrantAwaitedLock(void)
```

## Detailed Description
GrantAwaitedLock serves as a public interface to GrantLockLocal for a very specific scenario: when a process discovers that it has been granted a lock even after timing out or being interrupted during the wait. This can happen when the lock is granted just as the timeout occurs, creating a race condition.

The function simply calls GrantLockLocal with the global variables awaitedLock and awaitedOwner, which are set up when a process begins waiting for a lock. This design avoids the need to export GrantLockLocal directly, which would require including resowner.h in lock.h and create circular dependencies between header files.

## Parameters / Member Variables
- None (void function, operates on global state)

## Dependencies
- Functions called/Symbols referenced:
  - [GrantLockLocal](GrantLockLocal.md)
- Global variables used:
  - awaitedLock (LOCALLOCK being waited on)
  - awaitedOwner (ResourceOwner for the awaited lock)
- Called from (representative examples):
  - ProcSleep (in timeout/interrupt scenarios)
  - [LockErrorCleanup](../L/LockErrorCleanup.md)

## Notes and Other Information
- This is a non-static function accessible from other modules, particularly proc.c
- Designed to handle race conditions between lock timeouts and lock grants
- Exists primarily to avoid circular header dependencies
- The global variables awaitedLock and awaitedOwner must be properly set before calling this function
- Used specifically in scenarios where a process was waiting on a lock but needs to handle late-arriving grants
- Part of the lock wait and timeout handling mechanism in PostgreSQL's lock manager