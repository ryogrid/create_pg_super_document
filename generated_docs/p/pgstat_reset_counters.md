# pgstat_reset_counters

## Location
[src/backend/utils/activity/pgstat.c:714-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L714-L732)

## Overview
Resets all statistics counters for the current database, providing a clean slate for statistics collection within the calling backend's database context.

## Definition

```c
void
pgstat_reset_counters(void)
```
## Detailed Description
This function performs a comprehensive reset of all statistics counters that belong to the current database. It operates by using a filtering mechanism to identify and reset only the statistics entries that match the current database OID, ensuring that statistics for other databases remain unaffected.

The function works by obtaining the current timestamp and then delegating the actual reset operation to pgstat_reset_matching_entries(), which applies the match_db_entries filter function to identify database-specific statistics entries. This approach ensures that the reset operation is both efficient and precise, affecting only the statistics that belong to the current database context.

The timestamp is recorded with each reset operation, allowing the statistics system to track when counters were last reset and to properly handle cumulative vs. incremental statistics calculations.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (to record the reset time)
  - [match_db_entries](../m/match_db_entries.md) (filter function to identify current database entries)
  - [pgstat_reset_matching_entries](pgstat_reset_matching_entries.md) (performs the actual reset operation)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (converts database OID to Datum)
  - MyDatabaseId (global variable for current database OID)
- Called from (representative examples):
  - [pg_stat_reset](pg_stat_reset.md) (SQL-callable function that provides user access to this functionality)

## Notes and Other Information
- Permission checking is handled through the normal PostgreSQL GRANT system rather than within this function
- The function only affects statistics for the current database, making it safe for use in multi-database environments
- Timestamps are recorded for each reset to enable proper statistics calculations post-reset
- This is the primary mechanism for administrative reset of database-level statistics in PostgreSQL
- The function operates on shared memory statistics structures, making the reset immediately visible to all backend processes

## Simplified Source

```c
void
pgstat_reset_counters(void)
{
    TimestampTz ts = GetCurrentTimestamp();

    // Reset all entries matching current database
    pgstat_reset_matching_entries(match_db_entries,
                                  ObjectIdGetDatum(MyDatabaseId),
                                  ts);
}
```