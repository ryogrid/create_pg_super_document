# DetachPartitionFinalize

## Location
[src/backend/commands/tablecmds.c:19320-19645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19320-L19645)

## Overview
DetachPartitionFinalize performs the final cleanup operations when detaching a partition from its parent table, handling constraint removal, foreign key management, index detachment, and catalog updates.

## Definition
```c
static void DetachPartitionFinalize(Relation rel, Relation partRel, bool concurrent, Oid defaultPartOid)
```

## Detailed Description
This function completes the partition detachment process by performing comprehensive cleanup operations:

1. **Inheritance cleanup**: Removes pg_inherits row in concurrent mode (already done in non-concurrent mode)
2. **Trigger management**: Drops cloned triggers from the partition
3. **Foreign key handling**: Detaches inherited foreign keys, updates constraint relationships, and creates necessary action triggers
4. **Index detachment**: Removes parent-child relationships between indexes and their associated constraints
5. **Catalog updates**: Updates pg_class to mark the relation as no longer a partition and clears partition bounds
6. **Identity column cleanup**: Drops identity properties from all identity columns
7. **Cache invalidation**: Ensures all relation cache entries are properly invalidated

The function is designed to be separable from the main detach operation, allowing it to be run independently if the second transaction of concurrent detachment fails.

## Parameters / Member Variables
- `rel`: The parent partitioned table relation
- `partRel`: The partition relation being detached
- `concurrent`: Boolean indicating if this is part of a concurrent detachment operation
- `defaultPartOid`: OID of the default partition (if any) for special handling

## Dependencies
- Functions called/Symbols referenced:
  - [RemoveInheritance](../R/RemoveInheritance.md)
  - [DropClonedTriggersFromPartition](DropClonedTriggersFromPartition.md)
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md)
  - [ConstraintSetParentConstraint](../C/ConstraintSetParentConstraint.md)
  - [GetForeignKeyCheckTriggers](../G/GetForeignKeyCheckTriggers.md)
  - [TriggerSetParentTrigger](../T/TriggerSetParentTrigger.md)
  - [DeconstructFkConstraintRow](DeconstructFkConstraintRow.md)
  - [addFkRecurseReferenced](../a/addFkRecurseReferenced.md)
  - [GetParentedForeignKeyRefs](../G/GetParentedForeignKeyRefs.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md)
  - [get_relation_idx_constraint_oid](../g/get_relation_idx_constraint_oid.md)
  - [ATExecDropIdentity](../A/ATExecDropIdentity.md)
  - [update_default_partition_oid](../u/update_default_partition_oid.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
- Called from (representative examples):
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)
  - [ATExecDetachPartitionFinalize](../A/ATExecDetachPartitionFinalize.md)

## Notes and Other Information
- Handles complex foreign key constraint hierarchies by distinguishing between constraints inherited from parent vs. partition-specific constraints
- Carefully manages constraint parent-child relationships to avoid orphaned constraints
- Uses extensive catalog updates to ensure consistency across pg_class, pg_constraint, and other system catalogs
- Performs recursive cache invalidation for partitioned tables to ensure all descendant partitions are properly updated
- Designed to be crash-safe and can be run independently if needed during recovery scenarios