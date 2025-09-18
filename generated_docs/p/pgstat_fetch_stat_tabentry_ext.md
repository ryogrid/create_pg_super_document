# pgstat_fetch_stat_tabentry_ext

## Location
[src/backend/utils/activity/pgstat_relation.c:466-486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L466-L486)

## Overview
Extended and more efficient version of table statistics retrieval that allows explicit specification of whether the target relation is shared, avoiding the need to determine this at runtime.

## Definition
```c
PgStat_StatTabEntry *pgstat_fetch_stat_tabentry_ext(bool shared, Oid reloid)
```

## Detailed Description
This function provides the core implementation for retrieving table statistics in PostgreSQL. It is the "extended" version that allows callers to explicitly specify whether the target relation is a shared relation (system catalog) or not, which improves efficiency by avoiding the runtime call to `IsSharedRelation()`.

The function determines the appropriate database OID based on the shared parameter: for shared relations, it uses `InvalidOid` to indicate they belong to the shared statistics database, while for regular relations it uses `MyDatabaseId` to specify the current database. It then delegates to the generic `pgstat_fetch_entry()` function with the `PGSTAT_KIND_RELATION` type to retrieve the actual statistics entry.

This function is used both as a direct interface for callers who already know the shared status of a relation (like autovacuum) and as the implementation backend for the simpler `pgstat_fetch_stat_tabentry()` wrapper.

## Parameters / Member Variables
- `shared`: A boolean indicating whether the relation is a shared relation (system catalog). If true, the relation's statistics are stored in the shared statistics database
- `reloid`: An `Oid` representing the object identifier of the relation for which statistics are being requested

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md) - Structure type for table statistics entries (return type)
  - [pgstat_fetch_entry](pgstat_fetch_entry.md) - Generic statistics entry retrieval function
  - `PGSTAT_KIND_RELATION` - Constant identifying the statistics entry type as relation statistics
  - `PgStat_TableStatus` - Structure type referenced in the broader context

- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) - Multiple calls during autovacuum processing to get relation statistics
  - [recheck_relation_needs_vacanalyze](../r/recheck_relation_needs_vacanalyze.md) - When determining if a relation needs vacuum/analyze
  - `pgstat_copy_relation_stats` - When copying relation statistics
  - [pgstat_fetch_stat_tabentry](pgstat_fetch_stat_tabentry.md) - The simpler wrapper function

## Notes and Other Information
- This is the more efficient version compared to `pgstat_fetch_stat_tabentry()` because it avoids the runtime `IsSharedRelation()` check
- Shared relations use `InvalidOid` as their database identifier in the statistics system
- Regular relations use `MyDatabaseId` to identify their database context
- The function serves as a thin wrapper around the generic `pgstat_fetch_entry()` mechanism
- Used extensively by autovacuum to make decisions about when tables need maintenance
- Returns NULL if no statistics entry exists for the specified relation