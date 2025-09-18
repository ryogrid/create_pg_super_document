# pg_try_advisory_lock_int8

## Location
src/backend/utils/adt/lockfuncs.c: 694 - 713

## Overview
Attempts to acquire a session-scoped exclusive advisory lock on a 64-bit integer key without blocking, returning a boolean indicating success or failure.

## Definition
```c
Datum pg_try_advisory_lock_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a non-blocking variant of advisory lock acquisition for session-scoped exclusive locks using a 64-bit integer key. Unlike the regular pg_advisory_lock functions that block indefinitely until the lock is available, this function immediately returns a boolean result indicating whether the lock was successfully acquired. If the lock is already held by another session, the function returns false instead of waiting.

The function converts the input 64-bit integer into a LOCKTAG structure and attempts to acquire an exclusive lock using PostgreSQL's internal lock manager. The fourth parameter to LockAcquire is set to 'true', indicating this is a non-blocking (try) operation. The lock is session-scoped, meaning it persists until explicitly unlocked or the session terminates.

## Parameters / Member Variables
- `key`: A 64-bit integer that serves as the unique identifier for the advisory lock

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 argument)
  - SET_LOCKTAG_INT64 (macro for setting up lock tag with int64 key)
  - [LockAcquire](../L/LockAcquire.md) (core lock acquisition function)
  - LockAcquireResult (enum type for lock acquisition results)
  - ExclusiveLock (lock mode constant)
  - LOCKACQUIRE_NOT_AVAIL (result constant indicating lock unavailable)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- This is a non-blocking operation - the function returns immediately regardless of lock availability
- Returns true if the lock was successfully acquired, false if the lock is already held by another session
- The lock is session-scoped and must be explicitly unlocked using pg_advisory_unlock functions
- The lock mode is ExclusiveLock, meaning it conflicts with all other lock modes on the same key
- Useful for implementing timeout-based locking strategies or polling mechanisms
- The function is exposed as a SQL function pg_try_advisory_lock(bigint)
- Located in src/backend/utils/adt/lockfuncs.c:694-713