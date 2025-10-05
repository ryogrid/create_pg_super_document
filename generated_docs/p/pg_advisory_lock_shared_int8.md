# pg_advisory_lock_shared_int8

## Location
[src/backend/utils/adt/lockfuncs.c:659-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L659-L675)

## Overview
Acquires a session-scoped shared advisory lock on a 64-bit integer key, allowing multiple processes to hold the same lock simultaneously for read-like operations.

## Definition
```c
Datum pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's advisory locking mechanism for session-scoped shared locks using a 64-bit integer key. Unlike exclusive locks, shared advisory locks can be held by multiple sessions simultaneously, making them suitable for implementing reader/writer synchronization patterns or coordinating access to shared resources where multiple readers are acceptable but exclusive access is needed for writers.

The function converts the input 64-bit integer into a LOCKTAG structure and acquires a shared lock using PostgreSQL's internal lock manager. The lock is session-scoped, meaning it persists until explicitly unlocked or the session terminates. The third parameter to LockAcquire is set to 'true', indicating this is a session-level lock rather than a transaction-level lock.

## Parameters / Member Variables
- `key`: A 64-bit integer that serves as the unique identifier for the advisory lock

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 argument)
  - SET_LOCKTAG_INT64 (macro for setting up lock tag with int64 key)
  - [LockAcquire](../L/LockAcquire.md) (core lock acquisition function)
  - ShareLock (lock mode constant)
  - PG_RETURN_VOID (macro for returning void result)
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- The lock is session-scoped and must be explicitly unlocked using pg_advisory_unlock functions
- This is a blocking operation - the function will wait indefinitely until the lock can be acquired
- The lock mode is ShareLock, which is compatible with other ShareLocks but conflicts with ExclusiveLocks
- Multiple sessions can hold shared locks on the same key simultaneously
- Shared locks are commonly used in reader/writer scenarios where concurrent reads are acceptable
- The function is exposed as a SQL function pg_advisory_lock_shared(bigint)
- Located in src/backend/utils/adt/lockfuncs.c:659-675

## Simplified Source

```c
Datum
pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;

    // Create lock tag from the 64-bit integer key
    SET_LOCKTAG_INT64(tag, key);

    // Acquire shared advisory lock (multiple sessions can hold simultaneously)
    LockAcquire(&tag, ShareLock, true, false);

    PG_RETURN_VOID();
}
```