# pg_advisory_xact_lock_shared_int4

## Location
[src/backend/utils/adt/lockfuncs.c:861-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L861-L879)

## Overview
Acquires a transaction-scoped shared advisory lock using two 32-bit integer keys, allowing multiple concurrent holders within the transaction scope.

## Definition
```c
Datum pg_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function acquires a shared advisory lock that is automatically scoped to the current transaction. The lock is identified by a combination of two 32-bit integer keys (key1, key2). Shared locks allow multiple processes to hold the same lock simultaneously, but prevent exclusive locks from being acquired. Unlike session-scoped advisory locks, transaction-scoped locks are automatically released when the transaction ends (either by commit or rollback). The function blocks indefinitely until the lock can be acquired.

## Parameters / Member Variables
- `key1`: First 32-bit integer component of the lock identifier (PG_GETARG_INT32(0))
- `key2`: Second 32-bit integer component of the lock identifier (PG_GETARG_INT32(1))

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_INT32: Macro to initialize the lock tag with two 32-bit integers
  - [LockAcquire](../L/LockAcquire.md): Core lock acquisition function with ShareLock mode and transaction scope (false)
  - [LOCKTAG](../L/LOCKTAG.md): Lock identifier structure
  - ShareLock: Lock mode constant for shared access
  - PG_RETURN_VOID: Macro to return void from a PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Transaction-scoped locks are automatically released at transaction end
- Uses shared lock mode, allowing multiple concurrent shared lock holders
- Blocks indefinitely until lock acquisition succeeds
- Compatible with other shared locks but conflicts with exclusive locks
- Part of PostgreSQL's advisory locking system for application-level coordination
- Accessible via SQL as pg_advisory_xact_lock_shared(int4, int4)

## Simplified Source

```c
Datum pg_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
{
    // Extract the two 32-bit keys from arguments
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;

    // Set up lock tag for the composite key
    SET_LOCKTAG_INT32(tag, key1, key2);

    // Acquire shared transaction-scoped lock (blocking until available)
    LockAcquire(&tag, ShareLock, false, false);

    PG_RETURN_VOID();
}
```