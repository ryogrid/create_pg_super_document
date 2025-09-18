# IndexSetParentIndex

## Location
src/backend/commands/indexcmds.c: 4304 - 4435

## Overview
IndexSetParentIndex establishes or removes parent-child inheritance relationships between indexes by managing pg_inherits entries and associated dependency records for index partitioning.

## Definition
```c
void IndexSetParentIndex(Relation partitionIdx, Oid parentOid)
```

## Detailed Description
This function manages the inheritance relationship between a partition index and its parent index by manipulating the pg_inherits system catalog and associated dependency records. It handles both establishing new parent-child relationships (when parentOid is valid) and removing existing ones (when parentOid is InvalidOid).

The function performs several critical operations:
- Scans pg_inherits to find existing inheritance relationships for the given index
- Inserts or deletes pg_inherits tuples as needed to establish the correct parent-child relationship
- Updates the parent index's relhassubclass flag when a partition is added
- Updates the partition index's relispartition flag to reflect its partition status
- Manages pg_depend entries to track PARTITION_PRI and PARTITION_SEC dependencies between the partition index, parent index, and partition table

The function ensures catalog consistency by properly handling all the metadata associated with index partitioning relationships.

## Parameters / Member Variables
- `partitionIdx`: Relation structure representing the partition index that will have its parent relationship modified
- `parentOid`: OID of the parent index to establish inheritance with, or InvalidOid to remove existing inheritance

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [relation_close](../r/relation_close.md)
  - [StoreSingleInheritance](../S/StoreSingleInheritance.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [SetRelationHasSubclass](../S/SetRelationHasSubclass.md)
  - [update_relispartition](../u/update_relispartition.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - CommandCounterIncrement
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)

## Notes and Other Information
- The function is designed to work with both regular indexes (RELKIND_INDEX) and partitioned indexes (RELKIND_PARTITIONED_INDEX)
- Uses RowExclusiveLock on pg_inherits to ensure safe concurrent access during inheritance modifications
- The function handles three scenarios: creating new inheritance, removing existing inheritance, and no-op cases where the desired state already exists
- Includes error checking to detect corrupt catalog states where unexpected inheritance relationships exist
- Updates visibility through CommandCounterIncrement() to make changes available to subsequent operations in the same transaction
- Manages both primary (DEPENDENCY_PARTITION_PRI) and secondary (DEPENDENCY_PARTITION_SEC) partition dependencies
- Always updates the partition's relispartition status to maintain catalog consistency