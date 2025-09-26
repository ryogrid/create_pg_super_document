# pg_advisory_lock_int8

## Location
[src/backend/utils/adt/lockfuncs.c:626-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L626-L642)

## Overview
pg_advisory_lock_int8 acquires an exclusive advisory lock on a 64-bit integer key, providing application-level locking mechanisms independent of database objects.

## Definition
Datum pg_advisory_lock_int8(PG_FUNCTION_ARGS)

## Detailed Description
pg_advisory_lock_int8 is a PostgreSQL system function that implements advisory locking using a 64-bit integer as the lock key. Advisory locks are application-level locks that do not lock any database objects but instead provide a mechanism for applications to coordinate access to shared resources. The function creates a lock tag using the provided integer key and attempts to acquire an exclusive lock on it. If the lock is already held by another session, the function will block until the lock becomes available. Advisory locks are session-scoped and are automatically released when the session ends or when explicitly unlocked. This function is part of PostgreSQL's advisory locking API that allows applications to implement custom locking schemes for application-specific resources.

## Parameters / Member Variables
- `key`: INT64 - A 64-bit integer that serves as the unique identifier for the advisory lock

The function returns VOID after successfully acquiring the lock.

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts 64-bit integer argument from function call)
  - SET_LOCKTAG_INT64 (creates a lock tag for 64-bit integer advisory locks)
  - [LockAcquire](../L/LockAcquire.md) (core lock acquisition function)
  - PG_RETURN_VOID (returns void result)
- Referenced types:
  - [LOCKTAG](../L/LOCKTAG.md) (lock identifier structure)
  - ExclusiveLock (lock mode constant)
- Called from:
  - [delay_execution_planner](../d/delay_execution_planner.md) (in test modules for execution control)
  - Application code requiring custom locking mechanisms
  - SQL queries using pg_advisory_lock(bigint) function

## Notes and Other Information
- Advisory locks are completely separate from regular database locks on tables, rows, or other database objects
- The lock will block until available - for non-blocking behavior, use pg_try_advisory_lock_int8 instead
- Advisory locks are automatically released when the session ends, even if not explicitly unlocked
- Multiple advisory lock functions exist for different key types (int4, int8, and two-int4 variants)
- The lock is acquired in ExclusiveLock mode, meaning only one session can hold the lock at a time
- Advisory locks are useful for coordinating application-level operations that span multiple transactions
- The function uses PostgreSQL's standard lock manager infrastructure, making advisory locks visible in lock monitoring views
- Lock keys should be chosen carefully to avoid conflicts between different applications or modules
- Unlike database object locks, advisory locks do not participate in deadlock detection across different lock types