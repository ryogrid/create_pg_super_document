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
  - table_open
  - SearchSysCacheCopy1
  - HeapTupleIsValid
  - elog
  - [SysCacheGetAttr](SysCacheGetAttr.md)
  - [nodeToString](../n/nodeToString.md)
  - CStringGetTextDatum
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - table_close
  - [update_default_partition_oid](../u/update_default_partition_oid.md)
  - CommandCounterIncrement
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