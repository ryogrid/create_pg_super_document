# pgoutput_column_list_init

## Location
[src/backend/replication/pgoutput/pgoutput.c:1041-1155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1041-L1155)

## Overview
Initializes column list filtering for a relation in the pgoutput logical replication plugin by building a bitmap of published columns from multiple publications.

## Definition

```c
static void
pgoutput_column_list_init(PGOutputData *data, List *publications,
						  RelationSyncEntry *entry)
```
## Detailed Description
This function processes column list definitions from multiple publications for a specific relation and creates a unified column bitmap for logical replication filtering. It examines each publication to find column list specifications and ensures consistency across publications - if different publications specify different column lists for the same table, an error is raised. The function handles special cases where "FOR ALL TABLES" or schema-based publications disable column filtering. When a column list includes all live (non-dropped, non-generated) columns, it optimizes by setting the column list to NULL, effectively disabling column filtering for that relation.

## Parameters / Member Variables
- `*data`: Pointer to PGOutputData structure containing plugin global state including memory contexts
- `*publications`: List of Publication structures that may contain column list definitions for this relation
- `*entry`: Pointer to RelationSyncEntry where the computed column bitmap will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [pgoutput_ensure_entry_cxt](pgoutput_ensure_entry_cxt.md)
  - [pub_collist_to_bitmapset](pub_collist_to_bitmapset.md)
  - RelationGetDescr
  - TupleDescAttr
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_free](../b/bms_free.md)
  - [bms_equal](../b/bms_equal.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - [RelationClose](../R/RelationClose.md)
- Called from (representative examples):
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- The function enforces that all publications must have identical column lists for the same table, raising an error if they differ
- Publications with "alltables" flag or schema-based publications implicitly disable column filtering
- Optimization: when column list includes all live columns, it's set to NULL to disable filtering overhead
- Column lists exclude dropped and generated columns from consideration
- Uses the entry's private memory context for bitmap allocations via pgoutput_ensure_entry_cxt
- Static function only accessible within pgoutput.c  
- Part of the lazy initialization pattern for relation synchronization entries
- Critical for implementing selective column replication in logical replication setups