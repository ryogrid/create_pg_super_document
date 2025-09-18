# pgoutput_column_list_init

## Location
src/backend/replication/pgoutput/pgoutput.c: 1041 - 1155

## Overview
Initializes column list filtering for a relation in the pgoutput logical replication plugin by building a bitmap of published columns from multiple publications.

## Definition


## Detailed Description
This function processes column list definitions from multiple publications for a specific relation and creates a unified column bitmap for logical replication filtering. It examines each publication to find column list specifications and ensures consistency across publications - if different publications specify different column lists for the same table, an error is raised. The function handles special cases where "FOR ALL TABLES" or schema-based publications disable column filtering. When a column list includes all live (non-dropped, non-generated) columns, it optimizes by setting the column list to NULL, effectively disabling column filtering for that relation.

## Parameters / Member Variables
- : Pointer to PGOutputData structure containing plugin global state including memory contexts
- : List of Publication structures that may contain column list definitions for this relation  
- : Pointer to RelationSyncEntry where the computed column bitmap will be stored

## Dependencies
- Functions called/Symbols referenced:
  - RelationIdGetRelation
  - SearchSysCache2
  - SysCacheGetAttr
  - pgoutput_ensure_entry_cxt
  - pub_collist_to_bitmapset
  - RelationGetDescr
  - TupleDescAttr
  - bms_num_members
  - bms_free
  - bms_equal
  - get_namespace_name
  - RelationGetNamespace
  - RelationClose
- Called from (representative examples):
  - get_rel_sync_entry

## Notes and Other Information
- The function enforces that all publications must have identical column lists for the same table, raising an error if they differ
- Publications with "alltables" flag or schema-based publications implicitly disable column filtering
- Optimization: when column list includes all live columns, it's set to NULL to disable filtering overhead
- Column lists exclude dropped and generated columns from consideration
- Uses the entry's private memory context for bitmap allocations via pgoutput_ensure_entry_cxt
- Static function only accessible within pgoutput.c  
- Part of the lazy initialization pattern for relation synchronization entries
- Critical for implementing selective column replication in logical replication setups