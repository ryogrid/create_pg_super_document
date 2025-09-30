# IndexSetParentIndex

## Location
[src/backend/commands/indexcmds.c:4304-4435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L4304-L4435)

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
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
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

## Simplified Source

```c
void IndexSetParentIndex(Relation partitionIdx, Oid parentOid)
{
    Relation pg_inherits;
    ScanKeyData key[2];
    SysScanDesc scan;
    Oid partRelid = RelationGetRelid(partitionIdx);
    HeapTuple tuple;
    bool fix_dependencies;

    // Ensure this is an index (regular or partitioned)
    Assert(partitionIdx->rd_rel->relkind == RELKIND_INDEX ||
           partitionIdx->rd_rel->relkind == RELKIND_PARTITIONED_INDEX);

    // Search for existing inheritance relationship in pg_inherits
    pg_inherits = relation_open(InheritsRelationId, RowExclusiveLock);

    // Set up scan keys to find inheritance record for this index
    ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(partRelid));
    ScanKeyInit(&key[1], Anum_pg_inherits_inhseqno,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(1));

    scan = systable_beginscan(pg_inherits, InheritsRelidSeqnoIndexId,
                             true, NULL, 2, key);
    tuple = systable_getnext(scan);

    if (!HeapTupleIsValid(tuple)) {
        // No existing inheritance record found
        if (parentOid == InvalidOid) {
            // No parent wanted and none exists - nothing to do
            fix_dependencies = false;
        } else {
            // Create new inheritance relationship
            StoreSingleInheritance(partRelid, parentOid, 1);
            fix_dependencies = true;
        }
    } else {
        // Existing inheritance record found
        Form_pg_inherits inhForm = (Form_pg_inherits) GETSTRUCT(tuple);

        if (parentOid == InvalidOid) {
            // Remove existing inheritance relationship
            CatalogTupleDelete(pg_inherits, &tuple->t_self);
            fix_dependencies = true;
        } else {
            // Check if existing parent matches desired parent
            if (inhForm->inhparent != parentOid) {
                elog(ERROR, "bogus pg_inherit row: inhrelid %u inhparent %u",
                     inhForm->inhrelid, inhForm->inhparent);
            }
            // Already correct - no changes needed
            fix_dependencies = false;
        }
    }

    // Clean up pg_inherits scan
    systable_endscan(scan);
    relation_close(pg_inherits, RowExclusiveLock);

    // Update parent index metadata if adding a partition
    if (OidIsValid(parentOid)) {
        LockRelationOid(parentOid, ShareUpdateExclusiveLock);
        SetRelationHasSubclass(parentOid, true);
    }

    // Update partition's relispartition flag
    update_relispartition(partRelid, OidIsValid(parentOid));

    // Update dependency records if inheritance changed
    if (fix_dependencies) {
        if (OidIsValid(parentOid)) {
            // Add partition dependencies
            ObjectAddress partIdx, parentIdx, partitionTbl;

            ObjectAddressSet(partIdx, RelationRelationId, partRelid);
            ObjectAddressSet(parentIdx, RelationRelationId, parentOid);
            ObjectAddressSet(partitionTbl, RelationRelationId,
                           partitionIdx->rd_index->indrelid);

            // Record dependencies: partition -> parent, partition -> table
            recordDependencyOn(&partIdx, &parentIdx, DEPENDENCY_PARTITION_PRI);
            recordDependencyOn(&partIdx, &partitionTbl, DEPENDENCY_PARTITION_SEC);
        } else {
            // Remove partition dependencies
            deleteDependencyRecordsForClass(RelationRelationId, partRelid,
                                          RelationRelationId,
                                          DEPENDENCY_PARTITION_PRI);
            deleteDependencyRecordsForClass(RelationRelationId, partRelid,
                                          RelationRelationId,
                                          DEPENDENCY_PARTITION_SEC);
        }

        // Make changes visible to subsequent operations
        CommandCounterIncrement();
    }
}
```