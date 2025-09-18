# pg_stat_reset

## Location
[src/backend/utils/adt/pgstatfuncs.c:1688-1701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1688-L1701)

## Overview
A PostgreSQL function that resets all statistics counters for the current database, providing a way to start fresh statistics collection from a known baseline.

## Definition


## Detailed Description
This function provides a SQL-callable interface to reset all statistics counters for the current database. When called, it clears accumulated statistics data including table access statistics, index usage statistics, function call statistics, and other database-level metrics. This is particularly useful for establishing a baseline for statistics collection, such as after system maintenance, performance tuning, or when starting a new measurement period.

The function affects only the current database's statistics, not system-wide or other databases' statistics. After reset, all counters start accumulating from zero again, and the reset timestamp in various pg_stat_* views is updated to reflect when the reset occurred.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure (no actual parameters used)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_reset_counters](pgstat_reset_counters.md) (resets the internal statistics counters)
  - PG_RETURN_VOID (PostgreSQL macro to return void from a function)
- Called from (representative examples):
  - SQL queries using pg_stat_reset() function

## Notes and Other Information
- This function returns void as it performs a side effect rather than returning data
- Resets only the current database's statistics, not global or other databases' statistics
- Updates the reset timestamp visible in pg_stat_* system views
- Requires appropriate privileges (typically superuser or database owner)
- Commonly used by database administrators for performance monitoring and analysis
- Should be used carefully as it permanently destroys historical statistics data
- Often used in conjunction with monitoring tools that need clean baseline measurements
- The reset is immediate and irreversible within the scope of the current database