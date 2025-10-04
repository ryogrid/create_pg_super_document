# pg_last_xact_replay_timestamp

## Location
[src/backend/access/transam/xlogfuncs.c:627-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L627-L641)

## Overview
Returns the timestamp of the latest processed commit/abort record during WAL replay, or NULL if the server was started normally without recovery.

## Definition
```c
Datum pg_last_xact_replay_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the timestamp of the most recent commit or abort transaction record that has been replayed from the WAL (Write-Ahead Log) during recovery. The function is specifically designed for standby servers or servers recovering from a backup.

When called on a server that has started normally (without going through recovery), the function returns NULL since no WAL replay has occurred. This makes it useful for determining the currency of data on a standby server - the timestamp indicates how far behind the standby is in processing transactions from the primary.

The function internally calls GetLatestXTime() to retrieve the timestamp of the latest replayed transaction, and returns it as a PostgreSQL TIMESTAMPTZ value.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetLatestXTime](../G/GetLatestXTime.md) (retrieves the actual timestamp from recovery state)
  - PG_RETURN_TIMESTAMPTZ (macro for returning timestamp value)
  - PG_RETURN_NULL (macro for returning NULL value)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's replication and recovery monitoring capabilities
- Useful for monitoring the lag between primary and standby servers
- Returns NULL on servers that haven't undergone WAL replay
- The timestamp represents transaction commit/abort time, not the time when replay occurred
- Defined in src/backend/access/transam/xlogfuncs.c:627-641

## Simplified Source

```c
Datum
pg_last_xact_replay_timestamp(PG_FUNCTION_ARGS)
{
    TimestampTz xtime;

    // Get timestamp of latest replayed commit/abort
    xtime = GetLatestXTime();

    // Return NULL if no replay has occurred
    if (xtime == 0)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(xtime);
}
```