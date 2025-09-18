# pg_replication_origin_session_is_setup

## Location
src/backend/replication/logical/origin.c: 1389 - 1404

## Overview
SQL-callable function that checks whether a replication origin has been set up for the current session, returning a boolean indicating the session's origin setup status.

## Definition
```c
Datum pg_replication_origin_session_is_setup(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function wrapper for checking replication origin session status. It provides a simple way to determine from SQL whether the current session has an active replication origin configured. The function performs basic prerequisite checks and then examines the global `replorigin_session_origin` variable to determine if it contains a valid origin ID. This is useful for conditional logic in replication management scripts and for debugging replication origin state.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (no arguments required for this function)

## Dependencies
- Functions called/Symbols referenced:
  - `replorigin_check_prerequisites` - Validates prerequisites with `check_slots=false` and `recoveryOK=false`
  - `InvalidRepOriginId` - Constant used to compare against the current session origin
  - `replorigin_session_origin` - Global variable that stores the current session's origin ID
  - `PG_RETURN_BOOL` - Returns boolean result to PostgreSQL function call framework
- Called from (representative examples):
  - SQL interface (no direct C callers found)

## Notes and Other Information
- Does not require `max_replication_slots > 0` (uses `check_slots=false` in prerequisites)
- Cannot be called during recovery (enforced by prerequisites check with `recoveryOK=false`)
- Returns `true` if a replication origin session is currently active, `false` otherwise
- Very lightweight operation - only checks a global variable after basic validation
- Useful for conditional logic in replication management procedures
- Can be called safely even when no origin is set up (returns false rather than erroring)
- Part of PostgreSQL's logical replication origin session management system
- The returned status reflects whether `pg_replication_origin_session_setup()` has been called without a corresponding `pg_replication_origin_session_reset()`
- Located in `src/backend/replication/logical/origin.c:1389-1404`