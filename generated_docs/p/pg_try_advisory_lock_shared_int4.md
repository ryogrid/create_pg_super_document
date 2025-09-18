# pg_try_advisory_lock_shared_int4

## Location
src/backend/utils/adt/lockfuncs.c: 921 - 941

## Overview
Attempts to acquire a shared advisory lock on two int4 keys without waiting, returning immediately whether successful or not.

## Definition


## Detailed Description
This function implements the PostgreSQL advisory locking mechanism for shared locks on a pair of 32-bit integer keys. Unlike exclusive locks, shared locks allow multiple processes to hold the same lock simultaneously, making them useful for coordinating read access to shared resources. The "try" variant of this function is non-blocking - it returns immediately with success or failure status rather than waiting for the lock to become available. The lock is session-scoped, meaning it will be automatically released when the session ends or when explicitly unlocked.

## Parameters / Member Variables
- : First 32-bit integer key component of the lock identifier
- : Second 32-bit integer key component of the lock identifier

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (to extract function arguments)
  - SET_LOCKTAG_INT32 (to construct lock tag from the two keys)
  - LockAcquire (to attempt lock acquisition with ShareLock mode)
  - PG_RETURN_BOOL (to return boolean result)
- Types used:
  - LOCKTAG (lock identifier structure)
  - LockAcquireResult (result status enumeration)
  - ShareLock (lock mode constant)
  - LOCKACQUIRE_NOT_AVAIL (failure status constant)
- Called from (representative examples):
  - SQL function calls via pg_proc catalog entry

## Notes and Other Information
- Returns true if the shared lock was successfully acquired, false if not available
- Uses session-scoped locking (not transaction-scoped like the xact variants)
- The lock is identified by the combination of both key1 and key2 values
- Multiple processes can hold shared locks on the same key pair simultaneously
- Lock will persist until explicitly released or session ends
- Part of PostgreSQL's advisory locking system for application-level coordination