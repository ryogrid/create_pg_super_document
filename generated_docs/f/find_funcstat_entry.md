# find_funcstat_entry

## Location
[src/backend/utils/activity/pgstat_function.c:223-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L223-L238)

## Overview
Searches for an existing PgStat_FunctionCounts entry for a specified function without creating a new entry if one doesn't exist.

## Definition

```c
PgStat_FunctionCounts *
find_funcstat_entry(Oid func_id)
```
## Detailed Description
This function is a utility for looking up function statistics entries in PostgreSQL's statistics collection system. It attempts to find an existing pending statistics entry for a given function ID. The function is designed to be non-intrusive - it will only return an existing entry and will not create a new one if none is found. This makes it useful for checking whether statistics are already being tracked for a particular function without affecting the statistics collection state.

The function operates by calling  with the function kind, current database ID, and the target function ID. If a pending entry exists, it returns the pending statistics data; otherwise, it returns NULL.

## Parameters / Member Variables
- : The OID (Object Identifier) of the function for which to search for statistics entry

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_pending_entry](../p/pgstat_fetch_pending_entry.md)
  - PGSTAT_KIND_FUNCTION
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md)
  - MyDatabaseId
- Called from (representative examples):
  - PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS
  - pgstat_count_conn_txn_idle_time

## Notes and Other Information
- This function is part of PostgreSQL's function statistics tracking system located in src/backend/utils/activity/pgstat_function.c:223-238
- Returns NULL if no existing entry is found, making it safe to use for conditional statistics access
- The function is read-only and does not modify the statistics collection state
- It specifically looks for pending entries, which are statistics that have been collected but not yet committed to the main statistics tables