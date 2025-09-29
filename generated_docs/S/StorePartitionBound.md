# StorePartitionBound

## Location
[src/backend/catalog/heap.c:3532-3609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L3532-L3609)

## Overview
Updates the pg_class tuple of a relation to store its partition bound specification and marks it as a partition, handling both regular and default partition cases.

## Definition
```c
void StorePartitionBound(Relation rel, Relation parent, PartitionBoundSpec *bound)
```

## Detailed Description
This function performs the final step in creating a partition by updating the catalog to reflect the partition's bound specification. It handles multiple critical tasks:

1. **pg_class updates**: Stores the partition bound in relpartbound and sets relispartition to true
2. **Default partition handling**: Updates pg_partitioned_table if this is a default partition
3. **Cache management**: Invalidates parent and default partition caches to ensure consistency
4. **Constraint management**: Ensures proper invalidation for default partition constraint updates

The function converts the PartitionBoundSpec to a text representation for storage and handles special cases like resetting relhassubclass for regular tables that become partitions.

## Parameters / Member Variables
- `rel`: The relation being converted to a partition
- `parent`: The parent partitioned table
- `bound`: The partition bound specification containing the partition's boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - HeapTupleIsValid
  - elog
  - [SysCacheGetAttr](SysCacheGetAttr.md)
  - [nodeToString](../n/nodeToString.md)
  - CStringGetTextDatum
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
  - [update_default_partition_oid](../u/update_default_partition_oid.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [get_default_oid_from_partdesc](../g/get_default_oid_from_partdesc.md)
  - [CacheInvalidateRelcacheByRelid](../C/CacheInvalidateRelcacheByRelid.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)

## Notes and Other Information
- Asserts that the relation is not already marked as a partition
- Handles the special case of default partitions by updating pg_partitioned_table
- Resets relhassubclass for regular tables that become partitions
- Invalidates default partition cache since its constraint depends on all other partition bounds
- Uses CommandCounterIncrement to make changes visible before cache invalidation
- The bound specification is serialized using nodeToString for storage in relpartbound
- Critical for maintaining partition descriptor consistency across the system

## Simplified Source

```c
void
StorePartitionBound(Relation rel, Relation parent, PartitionBoundSpec *bound)
{
    Relation classRel;
    HeapTuple tuple, newtuple;
    Datum new_val[Natts_pg_class];
    bool new_null[Natts_pg_class], new_repl[Natts_pg_class];
    Oid defaultPartOid;

    // Open pg_class catalog and get relation tuple
    classRel = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", RelationGetRelid(rel));

    // Verify relation is not already a partition (debug only)
    Assert(!((Form_pg_class) GETSTRUCT(tuple))->relispartition);

    // Prepare tuple modification arrays
    memset(new_val, 0, sizeof(new_val));
    memset(new_null, false, sizeof(new_null));
    memset(new_repl, false, sizeof(new_repl));

    // Set partition bound as text representation
    new_val[Anum_pg_class_relpartbound - 1] = CStringGetTextDatum(nodeToString(bound));
    new_null[Anum_pg_class_relpartbound - 1] = false;
    new_repl[Anum_pg_class_relpartbound - 1] = true;

    // Create modified tuple
    newtuple = heap_modify_tuple(tuple, RelationGetDescr(classRel),
                                new_val, new_null, new_repl);

    // Mark as partition and reset subclass flag if needed
    ((Form_pg_class) GETSTRUCT(newtuple))->relispartition = true;
    if (rel->rd_rel->relkind == RELKIND_RELATION && rel->rd_rel->relhassubclass)
        ((Form_pg_class) GETSTRUCT(newtuple))->relhassubclass = false;

    // Update catalog and cleanup
    CatalogTupleUpdate(classRel, &newtuple->t_self, newtuple);
    heap_freetuple(newtuple);
    table_close(classRel, RowExclusiveLock);

    // Handle default partition special case
    if (bound->is_default)
        update_default_partition_oid(RelationGetRelid(parent),
                                    RelationGetRelid(rel));

    // Make changes visible
    CommandCounterIncrement();

    // Invalidate caches - default partition constraint depends on all bounds
    defaultPartOid = get_default_oid_from_partdesc(RelationGetPartitionDesc(parent, true));
    if (OidIsValid(defaultPartOid))
        CacheInvalidateRelcacheByRelid(defaultPartOid);

    CacheInvalidateRelcache(parent);
}
```