# pg_try_advisory_lock_int4

## Location
[src/backend/utils/adt/lockfuncs.c:880-900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L880-L900)

## Overview
Attempts to acquire a session-scoped exclusive advisory lock using two 32-bit integer keys without blocking, returning success status.

## Definition
```c
Datum pg_try_advisory_lock_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function attempts to acquire an exclusive advisory lock that is scoped to the current session. The lock is identified by a combination of two 32-bit integer keys (key1, key2). Unlike the blocking version, this function returns immediately whether the lock was successfully acquired or not. It returns true if the lock was obtained, false if the lock was not available. The lock persists until explicitly released or the session ends.

## Parameters / Member Variables
- `key1`: First 32-bit integer component of the lock identifier (PG_GETARG_INT32(0))
- `key2`: Second 32-bit integer component of the lock identifier (PG_GETARG_INT32(1))

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_INT32: Macro to initialize the lock tag with two 32-bit integers
  - [LockAcquire](../L/LockAcquire.md): Core lock acquisition function with ExclusiveLock mode, session scope (true), and no-wait (true)
  - [LOCKTAG](../L/LOCKTAG.md): Lock identifier structure
  - LockAcquireResult: Enumeration type for lock acquisition results
  - ExclusiveLock: Lock mode constant for exclusive access
  - LOCKACQUIRE_NOT_AVAIL: Constant indicating lock was not available
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- [Session](../S/Session.md)-scoped locks must be explicitly released or will persist until session end
- Uses exclusive lock mode, preventing other processes from acquiring conflicting locks
- Non-blocking operation returns immediately with success/failure status
- Returns boolean: true if lock acquired, false if not available
- Part of PostgreSQL's advisory locking system for application-level coordination
- Accessible via SQL as pg_try_advisory_lock(int4, int4)