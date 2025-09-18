# pg_replication_origin_session_reset

## Location
src/backend/replication/logical/origin.c: 1372 - 1388

## Overview
SQL-callable function that resets and tears down a previously established replication origin session, releasing resources and clearing session-specific state.

## Definition
```c
Datum pg_replication_origin_session_reset(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function wrapper for resetting replication origin sessions. It performs cleanup of session state that was previously established by `pg_replication_origin_session_setup()`. The function releases the shared memory slot associated with the current session's origin, clears global session variables, and notifies other processes that the slot is now available. After calling this function, a new origin can be set up for the session.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (no arguments required for this function)

## Dependencies
- Functions called/Symbols referenced:
  - `replorigin_check_prerequisites` - Validates prerequisites with `check_slots=true` and `recoveryOK=false`
  - `replorigin_session_reset` - Performs the actual session cleanup and resource release
  - `InvalidRepOriginId` - Constant used to clear the session origin ID
  - `InvalidXLogRecPtr` - Constant used to clear the session LSN tracking
  - `replorigin_session_origin` - Global variable cleared to invalid state
  - `replorigin_session_origin_lsn` - Global variable cleared to invalid LSN
  - `replorigin_session_origin_timestamp` - Global variable cleared to zero
  - `PG_RETURN_VOID` - Returns void result to PostgreSQL function call framework
- Called from (representative examples):
  - SQL interface (no direct C callers found)

## Notes and Other Information
- Requires that a replication origin session was previously set up (enforced by internal function)
- Requires `max_replication_slots > 0` (checked by prerequisites function)
- Cannot be called during recovery (enforced by prerequisites check)
- Acquires exclusive lock on ReplicationOriginLock during cleanup to ensure thread safety
- Releases the shared memory slot by setting `acquired_by=0`, making it available for other processes
- Broadcasts condition variable to wake up processes waiting for the slot
- Clears all session-specific global state variables to prevent accidental reuse
- After reset, a new origin can be set up for the session using `pg_replication_origin_session_setup()`
- Part of PostgreSQL's logical replication origin session management system
- Located in `src/backend/replication/logical/origin.c:1372-1388`