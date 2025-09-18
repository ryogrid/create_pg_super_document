# pg_stat_get_db_conflict_all

## Location
src/backend/utils/adt/pgstatfuncs.c: 1092 - 1111

## Overview
Returns the total number of all types of conflicts that have occurred in a specific database, primarily relevant for Hot Standby scenarios.

## Definition
```c
Datum pg_stat_get_db_conflict_all(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the cumulative count of all conflict types that have occurred in a specified database. Conflicts typically occur in Hot Standby scenarios where read-only queries on a standby server conflict with WAL replay operations. The function sums up all individual conflict counters stored in the database statistics entry:

1. Tablespace conflicts (conflict_tablespace)
2. Lock conflicts (conflict_lock)
3. Snapshot conflicts (conflict_snapshot)
4. Logical replication slot conflicts (conflict_logicalslot)
5. Buffer pin conflicts (conflict_bufferpin)
6. Startup deadlock conflicts (conflict_startup_deadlock)

The function returns 0 if the database statistics entry doesn't exist, otherwise it returns the sum of all conflict types as a 64-bit integer.

## Parameters / Member Variables
- `dbid` (Oid): The database OID to retrieve conflict statistics for

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_fetch_stat_dbentry
- Data types used:
  - PgStat_StatDBEntry

## Notes and Other Information
- This function aggregates all conflict types into a single total count
- Conflicts are primarily relevant in Hot Standby configurations where standby servers replay WAL
- Returns 0 if no database entry exists in the statistics collector
- Each conflict type represents a different category of conflicts between read operations and WAL replay
- Used by monitoring tools and system views to track overall database conflict activity
- The returned value is cumulative since database startup or statistics reset
- Individual conflict types can be queried separately using specific functions for detailed analysis