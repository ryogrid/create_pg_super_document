# pgstat_fetch_stat_funcentry

## Location
[src/backend/utils/activity/pgstat_function.c:239-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L239-L243)

## Overview
Retrieves the collected statistics for a specific function, serving as a support function for SQL-callable pgstat* functions.

## Definition
```c
PgStat_StatFuncEntry *pgstat_fetch_stat_funcentry(Oid func_id)
```

## Detailed Description
This function serves as an interface for retrieving committed function statistics from PostgreSQL's statistics collection system. It is specifically designed to support the SQL-callable pgstat* functions that provide access to function performance metrics from SQL queries. Unlike `find_funcstat_entry` which searches for pending statistics entries, this function retrieves finalized, committed statistics data.

The function acts as a thin wrapper around the generic `pgstat_fetch_entry` function, specifically configured to fetch function statistics by casting the result to the appropriate PgStat_StatFuncEntry type. This provides type safety and a clean interface for function-specific statistics retrieval.

## Parameters / Member Variables
- `func_id`: The OID (Object Identifier) of the function for which to retrieve committed statistics

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_entry](pgstat_fetch_entry.md)
  - PGSTAT_KIND_FUNCTION
  - [PgStat_StatFuncEntry](../P/PgStat_StatFuncEntry.md)
  - MyDatabaseId
- Called from (representative examples):
  - PG_STAT_GET_FUNCENTRY_FLOAT8_MS
  - pgstat_count_conn_txn_idle_time

## Notes and Other Information
- This function is part of PostgreSQL's function statistics tracking system located in src/backend/utils/activity/pgstat_function.c:239-243
- Returns NULL if no statistics entry exists for the specified function
- Provides access to committed/finalized statistics data, as opposed to pending statistics
- Used primarily by SQL-callable functions that expose function performance metrics to users
- The returned statistics include metrics like call count, total time, self time, etc. for the specified function