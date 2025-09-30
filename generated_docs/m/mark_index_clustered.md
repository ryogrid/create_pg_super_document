# mark_index_clustered

## Location
[src/backend/commands/cluster.c:560-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L560-L632)

## Overview
Updates the pg_index system catalog to mark a specified index as the clustered index for a table, clearing the clustered flag from all other indexes on the same table.

## Definition

```c
void
mark_index_clustered(Relation rel, Oid indexOid, bool is_internal)
```
## Detailed Description
The mark_index_clustered function manages the indisclustered flag in the pg_index system catalog, which indicates which index (if any) a table is clustered on. This metadata is used by subsequent CLUSTER operations to determine the default clustering index when no specific index is specified.

The function performs several key operations:
1. **Validation**: Ensures the operation is not being applied to a partitioned table (not supported)
2. **Optimization**: Skips the operation if the target index is already marked as clustered
3. **Catalog Update**: Iterates through all indexes on the relation, clearing the indisclustered flag from existing clustered indexes and setting it on the new target index
4. **Hook Invocation**: Triggers post-alter hooks for each modified index to notify extensions and other components

When indexOid is InvalidOid, the function clears the clustered flag from all indexes, effectively removing any clustering designation from the table.

## Parameters / Member Variables
- : Relation structure representing the table whose indexes are being modified
- : OID of the index to mark as clustered, or InvalidOid to clear all clustered flags
- : Boolean flag indicating whether this is an internal operation (affects hook behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [get_index_isclustered](../g/get_index_isclustered.md)
  - [table_open](../t/table_open.md)/table_close
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHookArg
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md)
  - [ATExecClusterOn](../A/ATExecClusterOn.md)
  - [ATExecDropCluster](../A/ATExecDropCluster.md)

## Notes and Other Information
- Explicitly prevents marking indexes as clustered on partitioned tables since clustering is performed at the partition level
- Performs redundant validation on index validity even though this should have been checked earlier, following defensive programming practices
- Uses RowExclusiveLock on pg_index to ensure exclusive access during catalog updates
- Invokes object post-alter hooks for all processed indexes, not just the one being marked clustered, to ensure complete notification coverage
- The function is transactional and will be rolled back if the containing transaction fails

## Simplified Source

```c
void
mark_index_clustered(Relation rel, Oid indexOid, bool is_internal)
{
    HeapTuple indexTuple;
    Form_pg_index indexForm;
    Relation pg_index;
    ListCell *index;

    // Prevent clustering on partitioned tables
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot mark index clustered in partitioned table")));

    // Skip if the index is already marked clustered
    if (OidIsValid(indexOid)) {
        if (get_index_isclustered(indexOid))
            return;
    }

    // Open pg_index catalog for modification
    pg_index = table_open(IndexRelationId, RowExclusiveLock);

    // Process all indexes on the relation
    foreach(index, RelationGetIndexList(rel)) {
        Oid thisIndexOid = lfirst_oid(index);

        // Get the index tuple from system cache
        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(thisIndexOid));
        if (!HeapTupleIsValid(indexTuple))
            elog(ERROR, "cache lookup failed for index %u", thisIndexOid);

        indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

        // Clear clustered flag from previously clustered indexes
        if (indexForm->indisclustered) {
            indexForm->indisclustered = false;
            CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);
        }
        // Set clustered flag on the target index
        else if (thisIndexOid == indexOid) {
            // Validate index is usable for clustering
            if (!indexForm->indisvalid)
                elog(ERROR, "cannot cluster on invalid index %u", indexOid);

            indexForm->indisclustered = true;
            CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);
        }

        // Notify post-alter hooks for this index
        InvokeObjectPostAlterHookArg(IndexRelationId, thisIndexOid, 0,
                                    InvalidOid, is_internal);

        heap_freetuple(indexTuple);
    }

    table_close(pg_index, RowExclusiveLock);
}
```