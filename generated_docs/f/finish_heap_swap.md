# finish_heap_swap

## Location
[src/backend/commands/cluster.c:1438-1635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L1438-L1635)

## Overview
Completes the table reorganization process by swapping relation files, rebuilding indexes, cleaning up the transient table, and handling post-swap maintenance tasks.

## Definition

```c
enumber */
	object.classId = RelationRelationId;
```
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

## Simplified Source

```c
void
finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
                 bool is_system_catalog, bool swap_toast_by_content,
                 bool check_constraints, bool is_internal,
                 TransactionId frozenXid, MultiXactId cutoffMulti,
                 char newrelpersistence)
{
    ObjectAddress object;
    Oid mapped_tables[4];
    int reindex_flags;
    ReindexParams reindex_params = {0};

    // Report progress: swapping relation files
    pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
                                PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES);

    // Initialize mapped tables array
    memset(mapped_tables, 0, sizeof(mapped_tables));

    // Swap the physical files between old and new heaps
    swap_relation_files(OIDOldHeap, OIDNewHeap,
                       (OIDOldHeap == RelationRelationId),
                       swap_toast_by_content, is_internal,
                       frozenXid, cutoffMulti, mapped_tables);

    // Invalidate catalog caches for system catalogs
    if (is_system_catalog)
        CacheInvalidateCatalog(OIDOldHeap);

    // Prepare reindex flags
    reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
    if (check_constraints)
        reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;

    // Set index persistence based on table persistence
    if (newrelpersistence == RELPERSISTENCE_UNLOGGED)
        reindex_flags |= REINDEX_REL_FORCE_INDEXES_UNLOGGED;
    else if (newrelpersistence == RELPERSISTENCE_PERMANENT)
        reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;

    // Report progress: rebuilding indexes
    pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
                                PROGRESS_CLUSTER_PHASE_REBUILD_INDEX);

    // Rebuild all indexes on the swapped relation
    reindex_relation(NULL, OIDOldHeap, reindex_flags, &reindex_params);

    // Report progress: final cleanup
    pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
                                PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP);

    // Special handling for pg_class: update freeze information
    if (OIDOldHeap == RelationRelationId) {
        Relation relRelation;
        HeapTuple reltup;
        Form_pg_class relform;

        relRelation = table_open(RelationRelationId, RowExclusiveLock);
        reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDOldHeap));
        if (!HeapTupleIsValid(reltup))
            elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);

        relform = (Form_pg_class) GETSTRUCT(reltup);
        relform->relfrozenxid = frozenXid;
        relform->relminmxid = cutoffMulti;

        CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);
        table_close(relRelation, RowExclusiveLock);
    }

    // Drop the temporary heap relation
    object.classId = RelationRelationId;
    object.objectId = OIDNewHeap;
    object.objectSubId = 0;
    performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    // Clean up temporary relation mappings
    for (int i = 0; OidIsValid(mapped_tables[i]); i++)
        RelationMapRemoveMapping(mapped_tables[i]);

    // Rename TOAST tables if using link-based swapping
    if (!swap_toast_by_content) {
        Relation newrel = table_open(OIDOldHeap, NoLock);

        if (OidIsValid(newrel->rd_rel->reltoastrelid)) {
            Oid toastidx;
            char NewToastName[NAMEDATALEN];

            // Get TOAST index and rename toast table
            toastidx = toast_get_valid_index(newrel->rd_rel->reltoastrelid, NoLock);

            snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u", OIDOldHeap);
            RenameRelationInternal(newrel->rd_rel->reltoastrelid,
                                 NewToastName, true, false);

            // Rename TOAST index
            snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u_index", OIDOldHeap);
            RenameRelationInternal(toastidx, NewToastName, true, true);

            // Reset rewrite information for toast table
            CommandCounterIncrement();
            ResetRelRewrite(newrel->rd_rel->reltoastrelid);
        }
        relation_close(newrel, NoLock);
    }

    // Clear missing attribute settings for non-catalog tables
    if (!is_system_catalog) {
        Relation newrel = table_open(OIDOldHeap, NoLock);
        RelationClearMissing(newrel);
        relation_close(newrel, NoLock);
    }
}
```