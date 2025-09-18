# pg_advisory_unlock_int4

## Location
src/backend/utils/adt/lockfuncs.c: 962 - 981

## Overview
Releases an exclusive advisory lock on two int4 keys that was previously acquired by the current session.

## Definition
```c
Datum pg_advisory_unlock_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function releases a session-scoped exclusive advisory lock identified by a pair of 32-bit integer keys. It is the counterpart to the pg_advisory_lock functions and is used to explicitly release locks before the session ends. The function only succeeds if the current session actually holds the specified exclusive lock. This is part of PostgreSQL's advisory locking system which allows applications to coordinate access to shared resources using application-defined lock identifiers.

## Parameters / Member Variables
- `key1`: First 32-bit integer key component of the lock identifier to release
- `key2`: Second 32-bit integer key component of the lock identifier to release

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (to extract function arguments)
  - SET_LOCKTAG_INT32 (to construct lock tag from the two keys)
  - LockRelease (to release the exclusive lock)
  - PG_RETURN_BOOL (to return boolean result)
- Types used:
  - LOCKTAG (lock identifier structure)
  - ExclusiveLock (lock mode constant)
- Called from (representative examples):
  - SQL function calls via pg_proc catalog entry

## Notes and Other Information
- Returns true if the lock was successfully released, false if the lock was not held by the current session
- Only works with session-scoped exclusive locks (not transaction-scoped or shared locks)
- The lock is identified by the exact combination of both key1 and key2 values
- Attempting to unlock a lock not held by the current session returns false
- Part of PostgreSQL's advisory locking system for application-level coordination
- The true parameter in LockRelease indicates session scope