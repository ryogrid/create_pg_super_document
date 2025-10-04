# pg_wal_replay_pause

## Location
[src/backend/access/transam/xlogfuncs.c:517-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L517-L546)

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


## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if database is in recovery mode
  - [PromoteIsTriggered](../P/PromoteIsTriggered.md): Checks if standby promotion is ongoing
  - [SetRecoveryPause](../S/SetRecoveryPause.md): Sets the recovery pause state to true
  - [WakeupRecovery](../W/WakeupRecovery.md): Signals the recovery process to acknowledge the pause
  - PG_RETURN_VOID: Returns void from SQL function

## Notes and Other Information
- Can only be executed during recovery (standby server or point-in-time recovery)
- Cannot be used during an ongoing standby promotion
- Permission checking is managed through the normal PostgreSQL GRANT system
- The function returns immediately after setting the pause state; actual pausing happens asynchronously
- Recovery process must be awakened to process the pause request promptly
- Commonly used for maintenance operations on standby servers
- Located in src/backend/access/transam/xlogfuncs.c:517-546

## Simplified Source

```c
Datum
pg_wal_replay_pause(PG_FUNCTION_ARGS)
{
    // Must be in recovery mode to pause replay
    if (!RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("recovery is not in progress")));

    // Cannot pause during ongoing promotion
    if (PromoteIsTriggered())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("standby promotion is ongoing")));

    // Set recovery pause flag and wake up recovery process
    SetRecoveryPause(true);
    WakeupRecovery();

    PG_RETURN_VOID();
}
```