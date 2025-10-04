# pg_wal_replay_resume

## Location
[src/backend/access/transam/xlogfuncs.c:547-570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L547-L570)

## Overview
Resumes WAL replay during recovery after it has been previously paused, allowing the recovery process to continue processing WAL records.

## Definition
```c
Datum pg_wal_replay_resume(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function resumes WAL (Write-Ahead Log) replay that has been previously paused via pg_wal_replay_pause(). The function performs validation checks similar to its pause counterpart:

1. Verifies that the database is currently in recovery mode (required for this operation)
2. Ensures that a standby promotion is not currently in progress
3. Sets the recovery pause flag to false via SetRecoveryPause(), which allows the recovery process to continue

Unlike pg_wal_replay_pause(), this function does not need to wake up the recovery process since the recovery process will naturally continue once the pause flag is cleared. This function is the complementary operation to pg_wal_replay_pause() and is essential for resuming normal recovery operations after maintenance or debugging activities.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if database is in recovery mode
  - [PromoteIsTriggered](../P/PromoteIsTriggered.md): Checks if standby promotion is ongoing
  - [SetRecoveryPause](../S/SetRecoveryPause.md): Sets the recovery pause state to false (resume)
  - PG_RETURN_VOID: Returns void from SQL function

## Notes and Other Information
- Can only be executed during recovery (standby server or point-in-time recovery)
- Cannot be used during an ongoing standby promotion
- Permission checking is managed through the normal PostgreSQL GRANT system
- Complementary function to pg_wal_replay_pause()
- Does not require waking up the recovery process (unlike the pause function) since recovery naturally continues when unpaused
- The function returns immediately after clearing the pause state
- Commonly used to resume operations after maintenance on standby servers
- Located in src/backend/access/transam/xlogfuncs.c:547-570

## Simplified Source

```c
Datum
pg_wal_replay_resume(PG_FUNCTION_ARGS)
{
    // Must be in recovery mode to control WAL replay
    if (!RecoveryInProgress())
        ereport(ERROR, "recovery is not in progress");

    // Cannot resume during promotion
    if (PromoteIsTriggered())
        ereport(ERROR, "standby promotion is ongoing");

    // Clear the pause flag to resume recovery
    SetRecoveryPause(false);

    PG_RETURN_VOID();
}
```