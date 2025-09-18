# RemoveStatisticsById

## Location
[src/backend/commands/statscmds.c:747-808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/statscmds.c#L747-L808)

## Overview
Completely removes an extended statistics object by deleting its metadata from pg_statistic_ext and all associated statistical data from pg_statistic_ext_data.

## Definition


## Detailed Description
This function implements the core deletion logic for extended statistics objects, typically called as part of DROP STATISTICS commands or cascading deletions through the dependency system. It performs a complete cleanup by removing both the statistics object metadata and all computed statistical data.

The function follows a careful locking protocol to prevent race conditions: it first acquires ShareUpdateExclusiveLock on the target table to prevent concurrent operations like ANALYZE or other DROP STATISTICS commands, then removes both inherited and non-inherited statistical data, invalidates relevant plan caches, and finally deletes the main catalog entry.

Key operations performed:
- Retrieves the statistics object metadata from pg_statistic_ext
- Locks the target table to prevent concurrent modifications
- Removes all statistical data entries (both inherited and direct)
- Invalidates relation cache to trigger query plan rebuilds
- Deletes the main statistics object tuple

## Parameters / Member Variables
- : OID of the extended statistics object to be completely removed from the system

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens catalogs with appropriate locks)
  - [SearchSysCache1](../S/SearchSysCache1.md) (retrieves statistics object metadata by OID)
  - [RemoveStatisticsDataById](RemoveStatisticsDataById.md) (removes statistical data for both inheritance modes)
  - [CacheInvalidateRelcacheByRelid](../C/CacheInvalidateRelcacheByRelid.md) (invalidates cached plans for affected table)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (removes the main catalog entry)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (src/backend/catalog/dependency.c:1420)

## Notes and Other Information
- Called through PostgreSQL's dependency deletion mechanism, ensuring proper cascade handling
- Acquires ShareUpdateExclusiveLock on the target table to prevent concurrent statistics operations
- Removes both inherited (inh=true) and direct (inh=false) statistical data entries
- Cache invalidation ensures that query plans using the deleted statistics are rebuilt
- Holds table lock until transaction end to maintain consistency during the deletion process
- Forms part of the cleanup process when tables are dropped or when statistics are explicitly dropped