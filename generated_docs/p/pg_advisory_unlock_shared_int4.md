# pg_advisory_unlock_shared_int4

## Location
src/backend/utils/adt/lockfuncs.c: 982 - 999

## Overview
Releases a shared advisory lock on two int4 keys that was previously acquired by the current session.

## Definition
```c
Datum pg_advisory_unlock_shared_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function releases a session-scoped shared advisory lock identified by a pair of 32-bit integer keys. It is the counterpart to the pg_advisory_lock_shared functions and is used to explicitly release shared locks before the session ends. The function only succeeds if the current session actually holds the specified shared lock. Unlike exclusive locks, shared locks can be held by multiple sessions simultaneously, but each session must individually release its own hold on the lock.

## Parameters / Member Variables
- `key1`: First 32-bit integer key component of the lock identifier to release
- `key2`: Second 32-bit integer key component of the lock identifier to release

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (to extract function arguments)
  - SET_LOCKTAG_INT32 (to construct lock tag from the two keys)
  - LockRelease (to release the shared lock)
  - PG_RETURN_BOOL (to return boolean result)
- Types used:
  - LOCKTAG (lock identifier structure)
  - ShareLock (lock mode constant)
- Called from (representative examples):
  - SQL function calls via pg_proc catalog entry

## Notes and Other Information
- Returns true if the lock was successfully released, false if the lock was not held by the current session
- Only works with session-scoped shared locks (not transaction-scoped or exclusive locks)
- The lock is identified by the exact combination of both key1 and key2 values
- Multiple sessions can hold shared locks on the same key pair; each must unlock individually
- Attempting to unlock a shared lock not held by the current session returns false
- Part of PostgreSQL's advisory locking system for application-level coordination
- The true parameter in LockRelease indicates session scope