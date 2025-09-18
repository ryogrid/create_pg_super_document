# pg_advisory_unlock_shared_int8

## Location
src/backend/utils/adt/lockfuncs.c: 791 - 807

## Overview
This function releases a shared advisory lock on a 64-bit integer key, returning whether the unlock operation was successful.

## Definition
```c
Datum pg_advisory_unlock_shared_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_advisory_unlock_shared_int8 function provides a mechanism to release shared advisory locks in PostgreSQL. It takes a 64-bit integer as a lock key and attempts to release a shared lock that was previously acquired on that key. The function will only succeed if the current session actually holds a shared lock on the specified key. Since shared locks can be held by multiple sessions simultaneously, this function only releases the current session's hold on the shared lock. Other sessions may continue to hold shared locks on the same key.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `key` (int64): The 64-bit integer value identifying the shared lock to release

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts the 64-bit integer argument
  - SET_LOCKTAG_INT64: Sets up the lock tag structure with the key
  - LockRelease: Core lock release function
  - ShareLock: Lock mode constant for shared locks
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL interface)

## Notes and Other Information
- Returns boolean: true if lock was successfully released, false if no shared lock was held
- Only releases shared locks (not exclusive locks - use pg_advisory_unlock_int8 for those)
- Session-scoped: only the session that acquired the lock can release it
- Will fail if the session doesn't hold a shared lock on the specified key
- Multiple sessions can hold shared locks on the same key simultaneously
- Releasing a shared lock doesn't affect other sessions' shared locks on the same key
- Part of PostgreSQL's advisory locking system for application-level coordination
- Commonly used through SQL interface as pg_advisory_unlock_shared(bigint)
- Should be paired with previous calls to pg_advisory_lock_shared or pg_try_advisory_lock_shared