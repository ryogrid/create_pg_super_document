# pgstat_fetch_stat_tabentry

## Location
[src/backend/utils/activity/pgstat_relation.c:456-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L456-L465)

## Overview
Retrieves collected statistics for a specific table relation, serving as a convenience wrapper for SQL-callable statistics functions.

## Definition
```c
PgStat_StatTabEntry *pgstat_fetch_stat_tabentry(Oid relid)
```

## Detailed Description
This function serves as a simplified interface for retrieving table statistics in PostgreSQL. It acts as a wrapper around `pgstat_fetch_stat_tabentry_ext`, automatically determining whether the relation is shared (system catalog) or not by calling `IsSharedRelation()`. 

The function is designed to support SQL-callable pgstat functions that need to access table statistics. It returns a pointer to the statistics entry for the specified table, or NULL if no statistics are available. Importantly, a NULL return value doesn't indicate that the table doesn't exist, but rather that no statistics have been collected for it yet. Callers should interpret NULL as meaning the statistics values should be treated as zero.

This design pattern allows the statistics system to be lazy - tables don't need to have statistics entries created until they actually have activity to report.

## Parameters / Member Variables
- `relid`: An `Oid` representing the object identifier of the relation for which statistics are being requested

## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md) - Determines if the relation is a shared system catalog
  - [pgstat_fetch_stat_tabentry_ext](pgstat_fetch_stat_tabentry_ext.md) - Extended version that handles both shared and regular relations
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md) - Structure type for table statistics entries

- Called from (representative examples):
  - `PG_STAT_GET_RELENTRY_INT64` - SQL function for retrieving 64-bit integer statistics
  - `PG_STAT_GET_RELENTRY_TIMESTAMPTZ` - SQL function for retrieving timestamp statistics

## Notes and Other Information
- This function is a convenience wrapper that automatically handles the shared/non-shared relation distinction
- NULL return values should be interpreted as "no statistics available" rather than "table doesn't exist"
- The function is specifically designed to support the SQL-callable pgstat* functions exposed to users
- All actual statistics retrieval logic is delegated to `pgstat_fetch_stat_tabentry_ext`
- The distinction between shared and non-shared relations is important because they are tracked in different statistics databases

## Simplified Source

```c
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
    // Convenience wrapper that automatically determines if relation is shared
    // and delegates to the extended version for actual statistics retrieval
    return pgstat_fetch_stat_tabentry_ext(IsSharedRelation(relid), relid);
}
```