# pgstat_fetch_stat_dbentry

## Location
[src/backend/utils/activity/pgstat_database.c:242-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L242-L248)

## Overview
Retrieves the collected database statistics for a specified database OID, returning the statistics entry or NULL if no statistics exist.

## Definition
```c
PgStat_StatDBEntry *pgstat_fetch_stat_dbentry(Oid dboid)
```

## Detailed Description
This function serves as a support function for SQL-callable pgstat* functions that need to access database-level statistics. It acts as a wrapper around the more general pgstat_fetch_entry() function, specifically configured to fetch database-kind statistics.

The function returns a pointer to a PgStat_StatDBEntry structure containing all collected statistics for the specified database, or NULL if no statistics have been collected for that database. It's important to note that a NULL return value doesn't necessarily mean the database doesn't exist - it simply means no statistics have been recorded for it yet.

The underlying pgstat_fetch_entry() function handles various consistency modes, caching mechanisms, and snapshot management to provide efficient access to the statistics data.

## Parameters / Member Variables
- `dboid`: The OID of the database for which to retrieve statistics

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_entry](pgstat_fetch_entry.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (data structure)
  - PGSTAT_KIND_DATABASE (constant)
  - InvalidOid (constant)
- Called from (representative examples):
  - [rebuild_database_list](../r/rebuild_database_list.md) (in src/backend/postmaster/autovacuum.c:928, 952, 976)
  - [do_start_worker](../d/do_start_worker.md) (in src/backend/postmaster/autovacuum.c:1181)
  - PG_STAT_GET_DBENTRY_INT64 (in src/backend/utils/adt/pgstatfuncs.c:998)
  - [pg_stat_get_db_conflict_all](pg_stat_get_db_conflict_all.md) (in src/backend/utils/adt/pgstatfuncs.c:1098)
  - [pg_stat_get_db_checksum_failures](pg_stat_get_db_checksum_failures.md) (in src/backend/utils/adt/pgstatfuncs.c:1121)
  - [pg_stat_get_db_checksum_last_failure](pg_stat_get_db_checksum_last_failure.md) (in src/backend/utils/adt/pgstatfuncs.c:1139)

## Notes and Other Information
- Returns NULL when no statistics exist for the database (not when database doesn't exist)
- Callers should report ZERO values rather than treating NULL as an error
- Used extensively by SQL statistics functions and autovacuum processes
- Part of PostgreSQL's statistics framework for monitoring database activity
- Handles memory allocation and snapshot consistency internally through pgstat_fetch_entry
- The returned data includes counters for sessions, transactions, temporary files, checksum failures, and other database-level metrics