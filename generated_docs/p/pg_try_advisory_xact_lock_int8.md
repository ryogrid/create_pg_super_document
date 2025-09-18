# pg_try_advisory_xact_lock_int8

## Location
[src/backend/utils/adt/lockfuncs.c:714-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L714-L732)

## Overview
Attempts to acquire a transaction-scoped exclusive advisory lock on a 64-bit integer key without blocking, returning a boolean indicating success or failure and automatically releasing the lock at transaction end.

## Definition
```c
Datum pg_try_advisory_xact_lock_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function combines the non-blocking behavior of the 'try' advisory lock functions with the automatic cleanup benefits of transaction-scoped locks. It attempts to acquire an exclusive advisory lock using a 64-bit integer key without blocking, returning immediately with a boolean result indicating success or failure. If successful, the lock is automatically scoped to the current transaction and will be released when the transaction commits or aborts.

The function converts the input 64-bit integer into a LOCKTAG structure and attempts to acquire an exclusive lock using PostgreSQL's internal lock manager. The 'false' value for the session-level parameter indicates this is a transaction-scoped lock, while the 'true' value for the dontWait parameter makes this a non-blocking operation.

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
- The lock is automatically released when the current transaction ends (commit or abort)
- The lock mode is ExclusiveLock, meaning it conflicts with all other lock modes on the same key
- Unlike session-scoped advisory locks, these transaction-scoped locks cannot be explicitly unlocked
- Combines the immediate feedback of try-locks with the automatic cleanup of transaction scoping
- Useful for implementing conditional locking within transactional contexts
- The function is exposed as a SQL function pg_try_advisory_xact_lock(bigint)
- Located in src/backend/utils/adt/lockfuncs.c:714-732