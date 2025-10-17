# heap_drop_with_catalog

## Location
[src/backend/catalog/heap.c:1767-1946](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1767-L1946)

## Overview
heap_drop_with_catalog removes a specified relation from the system catalogs, handling the complete catalog cleanup for table drops including partitioning considerations and storage management.

## Definition

```c
void
heap_drop_with_catalog(Oid relid)
```
## Detailed Description
This function performs the comprehensive catalog cleanup required when dropping a relation. It handles special considerations for partitioned tables by acquiring necessary locks on parent and default partitions to prevent concurrent access issues. The function systematically removes entries from various system catalogs including pg_foreign_table (for foreign tables), pg_partitioned_table (for partitioned tables), handles partition constraint updates, schedules physical storage deletion, removes statistics, clears inheritance relationships, and performs cache invalidation. The function works in coordination with the dependency system and should typically be called through performDeletion() rather than directly.

## Parameters / Member Variables
- `relid`: OID of the relation to be dropped from the catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [get_partition_parent](../g/get_partition_parent.md)
  - [get_default_partition_oid](../g/get_default_partition_oid.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [relation_open](../r/relation_open.md)
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md)
  - [CheckTableForSerializableConflictIn](../C/CheckTableForSerializableConflictIn.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [RemovePartitionKeyByRelId](../R/RemovePartitionKeyByRelId.md)
  - [update_default_partition_oid](../u/update_default_partition_oid.md)
  - [RelationDropStorage](../R/RelationDropStorage.md)
  - [pgstat_drop_relation](../p/pgstat_drop_relation.md)
  - [relation_close](../r/relation_close.md)
  - [RemoveSubscriptionRel](../R/RemoveSubscriptionRel.md)
  - [remove_on_commit_action](../r/remove_on_commit_action.md)
  - [RelationForgetRelation](../R/RelationForgetRelation.md)
  - [RelationRemoveInheritance](../R/RelationRemoveInheritance.md)
  - [RemoveStatistics](../R/RemoveStatistics.md)
  - [DeleteAttributeTuples](../D/DeleteAttributeTuples.md)
  - [DeleteRelationTuple](../D/DeleteRelationTuple.md)
  - [CacheInvalidateRelcacheByRelid](../C/CacheInvalidateRelcacheByRelid.md)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md)

## Notes and Other Information
- Should not be called directly; use performDeletion() instead which handles dependency resolution
- For partitions, acquires AccessExclusiveLock on parent table to prevent concurrent queries using stale partition descriptors  
- Handles foreign table cleanup by removing pg_foreign_table entries
- For partitioned tables, removes pg_partitioned_table entries via RemovePartitionKeyByRelId
- Updates default partition OID if dropping the default partition itself
- Schedules physical storage unlinking at commit time for relations with storage
- Maintains AccessExclusiveLock until transaction commit to prevent concurrent access
- Performs comprehensive cache invalidation for partitioning hierarchy changes
- Removes subscription relations, ON COMMIT actions, inheritance, statistics, and attribute tuples
- Uses RELKIND_HAS_STORAGE macro to determine if storage cleanup is needed

## Simplified Source

```c
void
heap_drop_with_catalog(Oid relid)
{
    Relation rel;
    HeapTuple tuple;
    Oid parentOid = InvalidOid, defaultPartOid = InvalidOid;

    // Handle partition locking: if this is a partition, lock parent and default partition
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    if (((Form_pg_class) GETSTRUCT(tuple))->relispartition) {
        // Lock parent to prevent queries with stale partition descriptors
        parentOid = get_partition_parent(relid, true);
        LockRelationOid(parentOid, AccessExclusiveLock);

        // Lock default partition if it will be affected
        defaultPartOid = get_default_partition_oid(parentOid);
        if (OidIsValid(defaultPartOid) && relid != defaultPartOid)
            LockRelationOid(defaultPartOid, AccessExclusiveLock);
    }
    ReleaseSysCache(tuple);

    // Open the target relation with exclusive lock
    rel = relation_open(relid, AccessExclusiveLock);

    // Safety checks
    CheckTableNotInUse(rel, "DROP TABLE");
    CheckTableForSerializableConflictIn(rel);

    // Clean up type-specific catalog entries
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
        // Remove foreign table catalog entry
        Relation ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);
        HeapTuple fttuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(fttuple))
            elog(ERROR, "cache lookup failed for foreign table %u", relid);
        CatalogTupleDelete(ftrel, &fttuple->t_self);
        ReleaseSysCache(fttuple);
        table_close(ftrel, RowExclusiveLock);
    }

    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        RemovePartitionKeyByRelId(relid);

    // Update default partition OID if we're dropping the default partition
    if (relid == defaultPartOid)
        update_default_partition_oid(parentOid, InvalidOid);

    // Schedule physical storage deletion
    if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
        RelationDropStorage(rel);

    // Clean up statistics and close relation (keeping lock)
    pgstat_drop_relation(rel);
    relation_close(rel, NoLock);

    // Remove associated metadata
    RemoveSubscriptionRel(InvalidOid, relid);
    remove_on_commit_action(relid);
    RelationForgetRelation(relid);

    // Remove catalog entries
    RelationRemoveInheritance(relid);
    RemoveStatistics(relid, 0);
    DeleteAttributeTuples(relid);
    DeleteRelationTuple(relid);

    // Invalidate caches for partition hierarchy changes
    if (OidIsValid(parentOid)) {
        if (OidIsValid(defaultPartOid) && relid != defaultPartOid)
            CacheInvalidateRelcacheByRelid(defaultPartOid);
        CacheInvalidateRelcacheByRelid(parentOid);
    }
}
```