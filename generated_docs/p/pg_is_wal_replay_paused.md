# pg_is_wal_replay_paused

## Location
[src/backend/access/transam/xlogfuncs.c:571-591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L571-L591)

## Overview
Checks whether WAL replay is currently paused during recovery and returns a boolean indicating the pause state.

## Definition
```c
Datum pg_is_wal_replay_paused(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides a way to query the current pause state of WAL (Write-Ahead Log) replay during database recovery. The function performs a simple check:

1. Verifies that the database is currently in recovery mode (required for this operation)
2. Calls GetRecoveryPauseState() to retrieve the current recovery pause state
3. Returns true if the recovery state is anything other than RECOVERY_NOT_PAUSED, false otherwise

This function is useful for monitoring scripts, administrative tools, or applications that need to determine whether WAL replay has been paused (via pg_wal_replay_pause()) before performing operations that depend on the recovery state. It provides a programmatic way to check the pause status without requiring external state tracking.

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS convention but takes no arguments)
- Returns boolean: true if WAL replay is paused, false if not paused

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if database is in recovery mode
  - [GetRecoveryPauseState](../G/GetRecoveryPauseState.md): Retrieves the current recovery pause state
  - RECOVERY_NOT_PAUSED: Constant representing the non-paused state
  - PG_RETURN_BOOL: Returns boolean value from SQL function

## Notes and Other Information
- Can only be executed during recovery (standby server or point-in-time recovery)
- Returns true for any paused state, not just user-requested pauses
- Useful for scripting and monitoring applications that need to check recovery state
- Complements the pg_wal_replay_pause() and pg_wal_replay_resume() functions
- The function checks against RECOVERY_NOT_PAUSED constant, meaning any other pause state returns true
- Located in src/backend/access/transam/xlogfuncs.c:571-591