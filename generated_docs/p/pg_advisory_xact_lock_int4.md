# pg_advisory_xact_lock_int4

## Location
[src/backend/utils/adt/lockfuncs.c:826-842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L826-L842)

## Overview
Acquires a transaction-scoped exclusive advisory lock using two 32-bit integer keys.

## Definition

```c
Datum
pg_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
```
## Detailed Description
This function acquires an exclusive advisory lock that is automatically scoped to the current transaction. The lock is identified by a combination of two 32-bit integer keys (key1, key2). Unlike session-scoped advisory locks, transaction-scoped locks are automatically released when the transaction ends (either by commit or rollback), eliminating the need for explicit unlocking. The function blocks indefinitely until the lock can be acquired.

## Parameters / Member Variables
- : First 32-bit integer component of the lock identifier (PG_GETARG_INT32(0))
- : Second 32-bit integer component of the lock identifier (PG_GETARG_INT32(1))

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_INT32: Macro to initialize the lock tag with two 32-bit integers
  - [LockAcquire](../L/LockAcquire.md): Core lock acquisition function with ExclusiveLock mode
  - [LOCKTAG](../L/LOCKTAG.md): Lock identifier structure
  - ExclusiveLock: Lock mode constant for exclusive access
  - PG_RETURN_VOID: Macro to return void from a PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Transaction-scoped locks are automatically released at transaction end
- Uses exclusive lock mode, preventing other processes from acquiring conflicting locks
- Blocks indefinitely until lock acquisition succeeds
- Part of PostgreSQL's advisory locking system for application-level coordination
- Accessible via SQL as pg_advisory_xact_lock(int4, int4)

## Simplified Source

```c
Datum pg_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
{
    // Extract the two 32-bit keys from arguments
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;

    // Set up lock tag for the composite key
    SET_LOCKTAG_INT32(tag, key1, key2);

    // Acquire exclusive transaction-scoped lock (blocking until available)
    LockAcquire(&tag, ExclusiveLock, false, false);

    PG_RETURN_VOID();
}
```