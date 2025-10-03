# pg_stat_clear_snapshot

## Location
[src/backend/utils/adt/pgstatfuncs.c:1668-1677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1668-L1677)

## Overview
A PostgreSQL function that discards the current active statistics snapshot, forcing the statistics system to fetch fresh data on the next access.

## Definition

```c
Datum
pg_stat_clear_snapshot(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a SQL-callable interface to clear PostgreSQL's statistics snapshot. The statistics system maintains cached snapshots of statistics data to provide consistent views during a transaction or query execution. By calling this function, users can force the statistics system to discard the current snapshot and fetch fresh statistics data from the statistics collector on the next access. This is useful when you need to see the most up-to-date statistics within a session, particularly after performing operations that would significantly change the statistics.

The function is a simple wrapper around the internal pgstat_clear_snapshot() function, making it accessible from SQL as a system administration function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure (no actual parameters used)
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_clear_snapshot](pgstat_clear_snapshot.md) (clears the internal statistics snapshot)
  - PG_RETURN_VOID (PostgreSQL macro to return void from a function)
- Called from (representative examples):
  - SQL queries using pg_stat_clear_snapshot() function

## Notes and Other Information
- This function returns void as it performs a side effect rather than returning data
- Useful for database administrators who need fresh statistics data within a session
- The snapshot clearing affects only the current backend session, not system-wide statistics
- Commonly used in monitoring scripts or after bulk operations when fresh statistics are needed immediately
- No special privileges are required to call this function beyond normal database access