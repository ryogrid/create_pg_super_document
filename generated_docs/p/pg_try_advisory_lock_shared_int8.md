# pg_try_advisory_lock_shared_int8

## Location
[src/backend/utils/adt/lockfuncs.c:733-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L733-L752)

## Overview
This function attempts to acquire a shared advisory lock on a 64-bit integer key without waiting, returning immediately with success or failure status.

## Definition


## Detailed Description
The pg_try_advisory_lock_shared_int8 function provides a non-blocking mechanism to acquire shared advisory locks in PostgreSQL. It takes a 64-bit integer as a lock key and attempts to acquire a shared lock on that key. Unlike regular advisory locks that block until available, this function returns immediately, indicating whether the lock was successfully acquired or not. Shared locks allow multiple sessions to hold the same lock simultaneously, but prevent exclusive locks from being acquired on the same key.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (int64): The 64-bit integer value to use as the lock key

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts the 64-bit integer argument
  - SET_LOCKTAG_INT64: Sets up the lock tag structure with the key
  - [LockAcquire](../L/LockAcquire.md): Core lock acquisition function
  - ShareLock: Lock mode constant for shared locks
  - LOCKACQUIRE_NOT_AVAIL: Result constant indicating lock unavailability
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL interface)

## Notes and Other Information
- Returns boolean: true if lock acquired successfully, false if not available
- Non-blocking operation - does not wait if lock is unavailable
- Uses shared lock mode, allowing multiple concurrent holders
- Part of PostgreSQL's advisory locking system for application-level coordination
- Lock persists until session end or explicit unlock
- Commonly used through SQL interface as pg_try_advisory_lock_shared(bigint)