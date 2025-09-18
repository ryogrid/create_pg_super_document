# pg_try_advisory_xact_lock_int4

## Location
src/backend/utils/adt/lockfuncs.c: 901 - 920

## Overview
Attempts to acquire a transaction-scoped exclusive advisory lock using two 32-bit integer keys without blocking, returning success status.

## Definition
```c
Datum pg_try_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function attempts to acquire an exclusive advisory lock that is automatically scoped to the current transaction. The lock is identified by a combination of two 32-bit integer keys (key1, key2). Unlike the blocking version, this function returns immediately whether the lock was successfully acquired or not. It returns true if the lock was obtained, false if the lock was not available. Unlike session-scoped advisory locks, transaction-scoped locks are automatically released when the transaction ends (either by commit or rollback).

## Parameters / Member Variables
- `key1`: First 32-bit integer component of the lock identifier (PG_GETARG_INT32(0))
- `key2`: Second 32-bit integer component of the lock identifier (PG_GETARG_INT32(1))

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_INT32: Macro to initialize the lock tag with two 32-bit integers
  - LockAcquire: Core lock acquisition function with ExclusiveLock mode, transaction scope (false), and no-wait (true)
  - LOCKTAG: Lock identifier structure
  - LockAcquireResult: Enumeration type for lock acquisition results
  - ExclusiveLock: Lock mode constant for exclusive access
  - LOCKACQUIRE_NOT_AVAIL: Constant indicating lock was not available
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Transaction-scoped locks are automatically released at transaction end
- Uses exclusive lock mode, preventing other processes from acquiring conflicting locks
- Non-blocking operation returns immediately with success/failure status
- Returns boolean: true if lock acquired, false if not available
- Part of PostgreSQL's advisory locking system for application-level coordination
- Accessible via SQL as pg_try_advisory_xact_lock(int4, int4)