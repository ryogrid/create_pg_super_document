# RelationGetStatExtList

## Location
src/backend/utils/cache/relcache.c: 4927 - 4996

## Overview
RelationGetStatExtList returns a list of statistics extension object OIDs associated with a relation by scanning pg_statistic_ext.

## Definition
```c
List *RelationGetStatExtList(Relation relation)
```

## Detailed Description
RelationGetStatExtList retrieves all extended statistics objects associated with a given relation by scanning pg_statistic_ext for entries where stxrelid matches the target relation OID. The function follows a similar caching pattern to RelationGetIndexList - if the statistics list has already been computed and cached (rd_statvalid is non-zero), it returns a copy of the cached list immediately.

The function scans pg_statistic_ext using the StatisticExtRelidIndexId index for efficient lookup. For each matching statistics extension object found, it extracts the OID and adds it to the result list. After completing the scan, the function sorts the result list by OID to ensure consistent ordering, though this ordering is not currently required by callers.

Like other similar relcache functions, RelationGetStatExtList carefully manages memory contexts to prevent leaks. It builds the result list in the caller's context during the scan, then copies it to CacheMemoryContext for caching. The function always returns a copy of the list to protect against cache invalidation that might occur during subsequent syscache lookups by the caller.

## Parameters / Member Variables
- `relation`: The Relation structure for which statistics extension objects should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy](../l/list_copy.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - table_open
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - lappend_oid
  - [list_sort](../l/list_sort.md)
  - [list_oid_cmp](../l/list_oid_cmp.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [list_free](../l/list_free.md)
  - Form_pg_statistic_ext (struct type)
- Called from (representative examples):
  - [get_relation_statistics](../g/get_relation_statistics.md)
  - [expandTableLikeClause](../e/expandTableLikeClause.md)

## Notes and Other Information
- Implements caching via rd_statlist and rd_statvalid fields in the relation structure
- Returns statistics object OIDs sorted by OID for consistency
- Returns a copy of the list to protect against cache invalidation during syscache lookups
- Memory management prevents leaks by using appropriate memory contexts for building and caching
- Handles creation or deletion of statistics objects through shared cache invalidation mechanism
- Used primarily by the query optimizer to gather extended statistics for planning purposes