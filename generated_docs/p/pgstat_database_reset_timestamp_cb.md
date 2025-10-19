# pgstat_database_reset_timestamp_cb

## Location
[src/backend/utils/activity/pgstat_database.c:438-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L438-L441)

## Overview
A callback function that updates the statistics reset timestamp for database-level statistics in PostgreSQL's statistics collection system.

## Definition
```c
void pgstat_database_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
```

## Detailed Description
This function serves as a callback within PostgreSQL's statistics collection framework to update the reset timestamp for database statistics. It is specifically designed to work with the shared statistics infrastructure and is called when database statistics are reset. The function casts the generic `PgStatShared_Common` header to a `PgStatShared_Database` structure and updates its `stat_reset_timestamp` field with the provided timestamp.

The function is part of the pluggable callback system used by the statistics collector to handle different types of statistics objects (databases, relations, etc.) in a uniform way. Each statistics kind has its own set of callbacks, including this reset timestamp callback for database statistics.

## Parameters / Member Variables
- `header`: A pointer to the shared statistics header structure, which will be cast to `PgStatShared_Database` to access database-specific statistics
- `ts`: The timestamp value to set as the new reset timestamp for the database statistics

## Dependencies
- Functions called/Symbols referenced:
  - [PgStatShared_Common](../P/PgStatShared_Common.md) (parameter type)
  - [PgStatShared_Database](../P/PgStatShared_Database.md) (cast target type)
  - TimestampTz (parameter type)
- Called from (representative examples):
  - Statistics collector framework via callback mechanism (registered in pgstat.c:274)

## Notes and Other Information
- This function is registered as the `reset_timestamp_cb` callback for `PGSTAT_KIND_DATABASE` statistics in the statistics collector configuration
- The function assumes that the provided `header` parameter is actually pointing to a `PgStatShared_Database` structure, which is guaranteed by the statistics framework
- The reset timestamp is used to track when database statistics were last reset, which is important for monitoring and administrative purposes
- Located in src/backend/utils/activity/pgstat_database.c:438-441

## Simplified Source

```c
void
pgstat_database_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
{
    // Cast generic header to database-specific structure and update timestamp
    ((PgStatShared_Database *) header)->stats.stat_reset_timestamp = ts;
}
```