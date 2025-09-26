# pgstat_fetch_pending_entry

## Location
[src/backend/utils/activity/pgstat.c:1145-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1145-L1157)

## Overview
Fetches an existing pending statistics entry without creating a new one, primarily used as a helper function for pgstatfuncs.c to retrieve statistics entries that are already in the pending state.

## Definition
PgStat_EntryRef *pgstat_fetch_pending_entry(PgStat_Kind kind, Oid dboid, Oid objoid)

## Detailed Description
This function serves as a read-only accessor for existing pending statistics entries in the PostgreSQL statistics collection system. It wraps pgstat_get_entry_ref with specific parameters to only retrieve entries that already exist and have pending data, without creating new entries. The function is designed specifically for use by pgstatfuncs.c helper functions and should not be used elsewhere in the codebase. It provides a safe way to check for and access pending statistics without modifying the statistics state.

## Parameters / Member Variables
- `kind`: The type of statistics object being queried (database, relation, function, etc.)
- `dboid`: The object identifier of the database containing the statistics object
- `objoid`: The object identifier of the specific statistics object being queried

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_entry_ref
  - PgStat_Kind
  - PgStat_EntryRef
- Called from (representative examples):
  - find_funcstat_entry (src/backend/utils/activity/pgstat_function.c:227)
  - find_tabstat_entry (src/backend/utils/activity/pgstat_relation.c:494, 497)

## Notes and Other Information
- Returns NULL if no entry exists or if the entry has no pending data
- Should only be used by helper functions in pgstatfuncs.c as noted in the source comments
- Does not create new entries - purely a read-only operation
- Part of PostgreSQL's statistics collection infrastructure for monitoring database activity