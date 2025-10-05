# pg_advisory_xact_lock_shared_int8

## Location
[src/backend/utils/adt/lockfuncs.c:676-693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L676-L693)

## Overview
Acquires a transaction-scoped shared advisory lock on a 64-bit integer key, allowing multiple processes to hold the same lock simultaneously while being automatically released at transaction end.

## Definition
```c
Datum pg_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function combines the characteristics of both transaction-scoped and shared advisory locks. It acquires a shared advisory lock using a 64-bit integer key, where the lock is automatically scoped to the current transaction and will be released when the transaction commits or aborts. Like other shared locks, multiple sessions can hold this lock simultaneously, making it suitable for coordinating access to shared resources within transaction boundaries.

The function converts the input 64-bit integer into a LOCKTAG structure and acquires a shared lock using PostgreSQL's internal lock manager. The lock is non-interruptible and will block until acquired. The 'false' value for the session-level parameter indicates this is a transaction-scoped lock rather than a session-scoped lock.

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
- The lock is automatically released when the current transaction ends (commit or abort)
- This is a blocking operation - the function will wait indefinitely until the lock can be acquired
- The lock mode is ShareLock, which is compatible with other ShareLocks but conflicts with ExclusiveLocks
- Multiple sessions can hold shared locks on the same key simultaneously
- Unlike session-scoped advisory locks, these transaction-scoped locks cannot be explicitly unlocked
- Combines the automatic cleanup benefits of transaction scope with the concurrency benefits of shared locking
- The function is exposed as a SQL function pg_advisory_xact_lock_shared(bigint)
- Located in src/backend/utils/adt/lockfuncs.c:676-693

## Simplified Source

```c
Datum
pg_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;

    // Create lock tag from the 64-bit integer key
    SET_LOCKTAG_INT64(tag, key);

    // Acquire shared advisory lock (transaction-scoped, auto-released)
    LockAcquire(&tag, ShareLock, false, false);

    PG_RETURN_VOID();
}
```