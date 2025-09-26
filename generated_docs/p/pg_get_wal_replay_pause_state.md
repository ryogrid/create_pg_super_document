# pg_get_wal_replay_pause_state

## Location
[src/backend/access/transam/xlogfuncs.c:592-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L592-L626)

## Overview
Returns the current recovery pause state as a human-readable text value, providing detailed information about whether WAL replay is paused, requested to pause, or running normally.

## Definition
```c
Datum pg_get_wal_replay_pause_state(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides detailed information about the current state of WAL (Write-Ahead Log) replay pause functionality during database recovery. Unlike pg_is_wal_replay_paused() which returns a simple boolean, this function returns one of three descriptive text states:

1. **"not paused"** - Recovery is running normally, no pause is requested or active
2. **"pause requested"** - A pause has been requested (via pg_wal_replay_pause()) but recovery has not yet reached the paused state
3. **"paused"** - Recovery is currently paused and not processing WAL records

The function first validates that the database is in recovery mode, then uses GetRecoveryPauseState() to determine the exact pause state and maps it to the appropriate human-readable string. This detailed state information is particularly useful for monitoring and debugging recovery operations.

## Parameters / Member Variables
- Returns text: One of "not paused", "pause requested", or "paused"

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if database is in recovery mode
  - [GetRecoveryPauseState](../G/GetRecoveryPauseState.md): Retrieves the current recovery pause state
  - RECOVERY_NOT_PAUSED: Constant for normal (not paused) state
  - RECOVERY_PAUSE_REQUESTED: Constant for pause requested but not yet active
  - RECOVERY_PAUSED: Constant for active pause state
  - [cstring_to_text](../c/cstring_to_text.md): Converts C string to PostgreSQL text type
  - PG_RETURN_TEXT_P: Returns PostgreSQL text value

## Notes and Other Information
- Can only be executed during recovery (standby server or point-in-time recovery)
- Provides more detailed state information than pg_is_wal_replay_paused()
- The "pause requested" state indicates a transition period where a pause has been requested but not yet taken effect
- Useful for detailed monitoring and troubleshooting of recovery pause operations
- The function includes an Assert to ensure a valid state string is always returned
- Complements the other recovery pause control functions (pause, resume, is_paused)
- Located in src/backend/access/transam/xlogfuncs.c:592-626