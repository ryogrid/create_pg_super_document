# pg_try_advisory_xact_lock_shared_int8

## Location
src/backend/utils/adt/lockfuncs.c: 753 - 771

## Overview
This function attempts to acquire a transaction-scoped shared advisory lock on a 64-bit integer key without waiting, returning immediately with success or failure status.

## Definition
```c
Datum pg_try_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_try_advisory_xact_lock_shared_int8 function provides a non-blocking mechanism to acquire transaction-scoped shared advisory locks in PostgreSQL. Unlike session-scoped locks, transaction-scoped locks are automatically released when the current transaction ends (either commits or aborts). It takes a 64-bit integer as a lock key and attempts to acquire a shared lock on that key without waiting. The shared lock mode allows multiple sessions to hold the same lock simultaneously but prevents exclusive locks from being acquired on the same key.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `key` (int64): The 64-bit integer value to use as the lock key

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts the 64-bit integer argument
  - SET_LOCKTAG_INT64: Sets up the lock tag structure with the key
  - LockAcquire: Core lock acquisition function (called with sessionLock=false for transaction scope)
  - ShareLock: Lock mode constant for shared locks
  - LOCKACQUIRE_NOT_AVAIL: Result constant indicating lock unavailability
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL interface)

## Notes and Other Information
- Returns boolean: true if lock acquired successfully, false if not available
- Non-blocking operation - does not wait if lock is unavailable
- Uses shared lock mode, allowing multiple concurrent holders
- Transaction-scoped: automatically released at transaction end
- Difference from session-scoped version: passes false as sessionLock parameter to LockAcquire
- Part of PostgreSQL's advisory locking system for application-level coordination
- Commonly used through SQL interface as pg_try_advisory_xact_lock_shared(bigint)