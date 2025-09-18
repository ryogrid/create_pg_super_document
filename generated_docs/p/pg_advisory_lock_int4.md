# pg_advisory_lock_int4

## Location
[src/backend/utils/adt/lockfuncs.c:808-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L808-L825)

## Overview
This function acquires an exclusive advisory lock using two 32-bit integers as a composite key, blocking until the lock is available.

## Definition
```c
Datum pg_advisory_lock_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_advisory_lock_int4 function provides a blocking mechanism to acquire exclusive advisory locks in PostgreSQL using a composite key made of two 32-bit integers. This variant allows for more granular locking by using two integer parameters instead of a single 64-bit key. The function will block (wait) until the lock becomes available, making it different from the non-blocking 'try' variants. Once acquired, the exclusive lock prevents any other session from acquiring either exclusive or shared locks on the same key combination until it is explicitly released or the session ends.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `key1` (int32): The first 32-bit integer component of the composite lock key
  - `key2` (int32): The second 32-bit integer component of the composite lock key

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Extracts the 32-bit integer arguments (called twice for key1 and key2)
  - SET_LOCKTAG_INT32: Sets up the lock tag structure with the composite key
  - [LockAcquire](../L/LockAcquire.md): Core lock acquisition function (called with sessionLock=true, dontWait=false)
  - ExclusiveLock: Lock mode constant for exclusive locks
  - PG_RETURN_VOID: Returns void (no return value)
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL interface)

## Notes and Other Information
- Returns void (no return value) - always succeeds by waiting if necessary
- Blocking operation - will wait indefinitely until lock becomes available
- Uses exclusive lock mode, preventing any other locks on the same key
- [Session](../S/Session.md)-scoped: persists until session end or explicit unlock
- Uses composite key approach with two 32-bit integers instead of single 64-bit key
- Equivalent to using pg_advisory_lock with a single bigint derived from the two int4 values
- Part of PostgreSQL's advisory locking system for application-level coordination
- Commonly used through SQL interface as pg_advisory_lock(integer, integer)
- Should be paired with corresponding pg_advisory_unlock calls using the same key pair