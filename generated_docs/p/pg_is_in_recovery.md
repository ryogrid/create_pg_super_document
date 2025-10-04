# pg_is_in_recovery

## Location
[src/backend/access/transam/xlogfuncs.c:642-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L642-L650)

## Overview
Returns a boolean value indicating whether the PostgreSQL server is currently in recovery mode (replaying WAL records).

## Definition
```c
Datum pg_is_in_recovery(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a simple way to determine if the PostgreSQL server is currently performing recovery operations. Recovery mode occurs in several scenarios:
- During startup recovery after an unclean shutdown
- On standby servers continuously replaying WAL from a primary server
- During point-in-time recovery from a backup

The function returns true if the server is in any form of recovery mode, and false if it's running normally as a primary server accepting read-write operations. This is a global state check that reflects the overall server status.

The function directly wraps the internal RecoveryInProgress() function, making this recovery state information available through the SQL interface.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (checks the global recovery state)
  - PG_RETURN_BOOL (macro for returning boolean value)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Essential for applications that need to determine server role (primary vs standby)
- Useful for conditional logic in applications that behave differently on primary vs standby
- The recovery state is a global server property, not connection-specific
- Commonly used in monitoring and high-availability setups
- Returns false on a fully operational primary server
- Defined in src/backend/access/transam/xlogfuncs.c:642-650

## Simplified Source

```c
Datum
pg_is_in_recovery(PG_FUNCTION_ARGS)
{
    // Return current recovery mode state
    PG_RETURN_BOOL(RecoveryInProgress());
}
```