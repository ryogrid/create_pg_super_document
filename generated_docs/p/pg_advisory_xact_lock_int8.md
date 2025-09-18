# pg_advisory_xact_lock_int8

## Location
src/backend/utils/adt/lockfuncs.c: 643 - 658

## Overview
Acquires a transaction-scoped exclusive advisory lock on a 64-bit integer key, providing application-level locking functionality that is automatically released at transaction end.

## Definition
```c
Datum pg_advisory_xact_lock_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's advisory locking mechanism for transaction-scoped exclusive locks using a 64-bit integer key. Advisory locks are user-defined locks that are not tied to any particular database object but rather serve as application-level coordination mechanisms. The "xact" in the name indicates that the lock is scoped to the current transaction and will be automatically released when the transaction commits or aborts, eliminating the need for explicit unlock operations.

The function converts the input 64-bit integer into a LOCKTAG structure and acquires an exclusive lock using PostgreSQL's internal lock manager. The lock is non-interruptible and will block until acquired, making it suitable for critical sections that must complete atomically.

## Parameters / Member Variables
- `key`: A 64-bit integer that serves as the unique identifier for the advisory lock

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 argument)
  - SET_LOCKTAG_INT64 (macro for setting up lock tag with int64 key)
  - LockAcquire (core lock acquisition function)
  - ExclusiveLock (lock mode constant)
  - PG_RETURN_VOID (macro for returning void result)
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- The lock is automatically released when the current transaction ends (commit or abort)
- This is a blocking operation - the function will wait indefinitely until the lock can be acquired
- The lock mode is ExclusiveLock, meaning it conflicts with all other lock modes on the same key
- Unlike session-scoped advisory locks, these transaction-scoped locks cannot be explicitly unlocked
- The function is exposed as a SQL function pg_advisory_xact_lock(bigint)
- Located in src/backend/utils/adt/lockfuncs.c:643-658