# finish_heap_swap

## Location
src/backend/commands/cluster.c: 1438 - 1635

## Overview
Completes the table reorganization process by swapping relation files, rebuilding indexes, cleaning up the transient table, and handling post-swap maintenance tasks.

## Definition


## Detailed Description
The `finish_heap_swap` function is the final stage of PostgreSQL's table reorganization operations, completing the process started by `make_new_heap` and `copy_table_data`. It performs the critical final steps:

1. **File Swapping**: Calls `swap_relation_files` to atomically swap the physical files between old and new heaps
2. **Cache Invalidation**: Invalidates system catalog caches for system relations to ensure consistency
3. **Index Rebuilding**: Rebuilds all indexes on the reorganized table with appropriate persistence settings
4. **Cleanup**: Drops the transient table that held the reorganized data
5. **Mapping Cleanup**: Removes temporary relation mappings for mapped relations
6. **TOAST Renaming**: Renames TOAST tables when using link-based swapping to maintain proper naming
7. **Missing Attributes**: Clears missing attribute information for non-catalog tables

The function includes special handling for pg_class itself, updating freeze information that couldn't be updated during the file swap.

## Parameters / Member Variables
- `OIDOldHeap`: OID of the original table being reorganized
- `OIDNewHeap`: OID of the temporary table containing reorganized data
- `is_system_catalog`: Boolean indicating if this is a system catalog table
- `swap_toast_by_content`: Boolean controlling TOAST table swapping method
- `check_constraints`: Boolean indicating whether to check constraints during reindexing
- `is_internal`: Boolean indicating if this is an internal operation
- `frozenXid`: Transaction ID to set as the new freeze cutoff
- `cutoffMulti`: MultiXact ID to set as the new cutoff
- `newrelpersistence`: Persistence characteristic for the reorganized table

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md): Updates progress reporting for clustering operations
  - [swap_relation_files](../s/swap_relation_files.md): Performs the atomic file swap between relations
  - [CacheInvalidateCatalog](../C/CacheInvalidateCatalog.md): Invalidates system catalog caches
  - [reindex_relation](../r/reindex_relation.md): Rebuilds all indexes on the reorganized table
  - SearchSysCacheCopy1/CatalogTupleUpdate: Updates pg_class for special cases
  - [performDeletion](../p/performDeletion.md): Drops the temporary table
  - [RelationMapRemoveMapping](../R/RelationMapRemoveMapping.md): Cleans up temporary relation mappings
  - [toast_get_valid_index](../t/toast_get_valid_index.md): Gets TOAST table indexes for renaming
  - [RenameRelationInternal](../R/RenameRelationInternal.md): Renames TOAST tables and indexes
  - [ResetRelRewrite](../R/ResetRelRewrite.md): Resets rewrite information for TOAST tables
  - [RelationClearMissing](../R/RelationClearMissing.md): Clears missing attribute information
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md): Table clustering operation
  - [refresh_by_heap_swap](../r/refresh_by_heap_swap.md): Materialized view refresh
  - [ATRewriteTables](../A/ATRewriteTables.md): ALTER TABLE rewrite operations

## Notes and Other Information
- Updates progress reporting throughout the operation for monitoring purposes
- Handles special case of pg_class by updating freeze information that swap_relation_files couldn't handle
- Uses appropriate reindex flags to control constraint checking and index persistence
- Performs cleanup of temporary relation mappings to avoid relmapper complaints
- Renames TOAST tables when using link-based swapping to maintain proper naming conventions
- Clears missing attribute settings for non-catalog tables to avoid inconsistencies
- Critical for the atomicity and consistency of table reorganization operations
- The function is non-static (public) as it's used by multiple table reorganization subsystems