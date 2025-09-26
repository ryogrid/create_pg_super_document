# pg_try_advisory_xact_lock_shared_int4

## Location
[src/backend/utils/adt/lockfuncs.c:942-961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L942-L961)

## Overview
Attempts to acquire a transaction-scoped shared advisory lock on two int4 keys without waiting, returning immediately whether successful or not.

## Definition
```c
Datum pg_try_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL advisory locking mechanism for transaction-scoped shared locks on a pair of 32-bit integer keys. The key difference from the session-scoped variant is that this lock is automatically released at the end of the current transaction, whether it commits or aborts. Like other shared locks, multiple processes can hold the same lock simultaneously. The "try" variant is non-blocking and returns immediately with success or failure status rather than waiting for lock availability.

## Parameters / Member Variables
- `key1`: First 32-bit integer key component of the lock identifier  
- `key2`: Second 32-bit integer key component of the lock identifier

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (to extract function arguments)
  - SET_LOCKTAG_INT32 (to construct lock tag from the two keys)
  - [LockAcquire](../L/LockAcquire.md) (to attempt lock acquisition with ShareLock mode, transaction-scoped)
  - PG_RETURN_BOOL (to return boolean result)
- Types used:
  - [LOCKTAG](../L/LOCKTAG.md) (lock identifier structure)
  - LockAcquireResult (result status enumeration)  
  - ShareLock (lock mode constant)
  - LOCKACQUIRE_NOT_AVAIL (failure status constant)
- Called from (representative examples):
  - SQL function calls via pg_proc catalog entry

## Notes and Other Information
- Returns true if the shared lock was successfully acquired, false if not available
- Uses transaction-scoped locking (automatically released at transaction end)
- The lock is identified by the combination of both key1 and key2 values
- Multiple processes can hold shared locks on the same key pair simultaneously
- Lock will be automatically released when the transaction commits or aborts
- Part of PostgreSQL's advisory locking system for application-level coordination
- The false parameter in LockAcquire indicates transaction scope (vs session scope)