# pg_wal_replay_pause

## Location
src/backend/access/transam/xlogfuncs.c: 517 - 546

## Overview
Requests to pause WAL replay during recovery, allowing administrators to temporarily halt the recovery process on a standby server.

## Definition
```c
Datum pg_wal_replay_pause(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides a way to pause WAL (Write-Ahead Log) replay during database recovery. The function performs several validation checks and then sets the recovery pause state:

1. Verifies that the database is currently in recovery mode (required for this operation)
2. Ensures that a standby promotion is not currently in progress (which would conflict with pausing)
3. Sets the recovery pause flag to true via SetRecoveryPause()
4. Wakes up the recovery process so it can acknowledge and process the pause request

This functionality is crucial for maintenance operations, debugging recovery issues, or creating consistent points for backup operations on standby servers. The pause remains in effect until explicitly resumed via pg_wal_replay_resume().

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS convention but takes no arguments)
- Returns void

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress: Checks if database is in recovery mode
  - PromoteIsTriggered: Checks if standby promotion is ongoing
  - SetRecoveryPause: Sets the recovery pause state to true
  - WakeupRecovery: Signals the recovery process to acknowledge the pause
  - PG_RETURN_VOID: Returns void from SQL function

## Notes and Other Information
- Can only be executed during recovery (standby server or point-in-time recovery)
- Cannot be used during an ongoing standby promotion
- Permission checking is managed through the normal PostgreSQL GRANT system
- The function returns immediately after setting the pause state; actual pausing happens asynchronously
- Recovery process must be awakened to process the pause request promptly
- Commonly used for maintenance operations on standby servers
- Located in src/backend/access/transam/xlogfuncs.c:517-546