# pg_advisory_unlock_int8

## Location
src/backend/utils/adt/lockfuncs.c: 772 - 790

## Overview
This function releases an exclusive advisory lock on a 64-bit integer key, returning whether the unlock operation was successful.

## Definition
```c
Datum pg_advisory_unlock_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_advisory_unlock_int8 function provides a mechanism to release exclusive advisory locks in PostgreSQL. It takes a 64-bit integer as a lock key and attempts to release an exclusive lock that was previously acquired on that key. The function will only succeed if the current session actually holds an exclusive lock on the specified key. Advisory locks are session-scoped by default and persist until explicitly unlocked or the session ends.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `key` (int64): The 64-bit integer value identifying the lock to release

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts the 64-bit integer argument
  - SET_LOCKTAG_INT64: Sets up the lock tag structure with the key
  - LockRelease: Core lock release function
  - ExclusiveLock: Lock mode constant for exclusive locks
- Called from (representative examples):
  - delay_execution_planner: Used in test module for synchronization

## Notes and Other Information
- Returns boolean: true if lock was successfully released, false if no lock was held
- Only releases exclusive locks (not shared locks - use pg_advisory_unlock_shared_int8 for those)
- Session-scoped: only the session that acquired the lock can release it
- Will fail if the session doesn't hold an exclusive lock on the specified key
- Part of PostgreSQL's advisory locking system for application-level coordination
- Commonly used through SQL interface as pg_advisory_unlock(bigint)
- Should be paired with previous calls to pg_advisory_lock or pg_try_advisory_lock