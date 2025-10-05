# pg_advisory_unlock_all

## Location
[src/backend/utils/adt/lockfuncs.c:1000-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L1000-L1005)

## Overview
Releases all advisory locks currently held by the current session.

## Definition
```c
Datum pg_advisory_unlock_all(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a convenient way to release all session-scoped advisory locks held by the current session in a single operation. It is equivalent to calling the individual unlock functions for every advisory lock currently held by the session. This is particularly useful for cleanup operations or when a session needs to release all its locks without knowing exactly which locks it holds. The function operates only on session-scoped advisory locks and does not affect transaction-scoped locks or regular table locks.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [LockReleaseSession](../L/LockReleaseSession.md) (to release all locks for the current session)
  - PG_RETURN_VOID (to return void result)
- Constants used:
  - USER_LOCKMETHOD (lock method identifier for advisory locks)
- Called from (representative examples):
  - SQL function calls via pg_proc catalog entry

## Notes and Other Information
- Returns void (no return value)
- Only affects session-scoped advisory locks, not transaction-scoped locks
- Releases both exclusive and shared advisory locks held by the current session
- Works on all advisory locks regardless of their key values or types (int4, int8, etc.)
- Part of PostgreSQL's advisory locking system for application-level coordination
- Useful for cleanup and error recovery scenarios
- Does not affect locks held by other sessions

## Simplified Source

```c
Datum
pg_advisory_unlock_all(PG_FUNCTION_ARGS)
{
    // Release all advisory locks held by current session
    LockReleaseSession(USER_LOCKMETHOD);

    PG_RETURN_VOID();
}
```