# TriggerSetParentTrigger

## Location
[src/backend/commands/trigger.c:1216-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1216-L1286)

## Overview
TriggerSetParentTrigger establishes or removes parent-child relationships between triggers in partitioned table hierarchies, managing the inheritance linkage and associated dependencies.

## Definition
```c
void TriggerSetParentTrigger(Relation trigRel,
                            Oid childTrigId,
                            Oid parentTrigId,
                            Oid childTableId)
```

## Detailed Description
TriggerSetParentTrigger manages the parent-child relationship between triggers in PostgreSQL's partitioned table system. When a parent trigger exists on a partitioned table, corresponding child triggers are created on each partition. This function either establishes the parent-child linkage by setting the tgparentid field in pg_trigger and creating DEPENDENCY_PARTITION_PRI and DEPENDENCY_PARTITION_SEC dependencies, or removes the linkage by clearing the parent ID and deleting the partition dependencies. This ensures that partition triggers are properly managed as part of the partitioned table's trigger hierarchy and that they cannot be independently dropped.

## Parameters / Member Variables
- `trigRel`: Open pg_trigger relation for catalog operations
- `childTrigId`: OID of the child trigger on the partition
- `parentTrigId`: OID of the parent trigger on the partitioned table (InvalidOid to remove linkage)
- `childTableId`: OID of the partition table that owns the child trigger

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - DEPENDENCY_PARTITION_PRI/DEPENDENCY_PARTITION_SEC
- Called from (representative examples):
  - [tryAttachPartitionForeignKey](../t/tryAttachPartitionForeignKey.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)

## Notes and Other Information
- Updates the tgparentid field in pg_trigger to establish inheritance relationship
- Creates two types of partition dependencies: primary (to parent trigger) and secondary (to child table)
- Prevents child triggers from being dropped independently of their parent
- Used during partition attach/detach operations to maintain trigger consistency
- Validates that a trigger doesn't already have a parent before setting one
- Can reverse the operation by passing InvalidOid as parentTrigId to remove linkage
- Part of PostgreSQL's partitioned table trigger inheritance system

## Simplified Source

```c
void TriggerSetParentTrigger(Relation trigRel,
                            Oid childTrigId,
                            Oid parentTrigId,
                            Oid childTableId) {
    SysScanDesc tgscan;
    ScanKeyData skey[1];
    Form_pg_trigger trigForm;
    HeapTuple tuple, newtup;
    ObjectAddress depender, referenced;

    // Find the child trigger record
    ScanKeyInit(&skey[0], Anum_pg_trigger_oid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(childTrigId));

    tgscan = systable_beginscan(trigRel, TriggerOidIndexId, true, NULL, 1, skey);
    tuple = systable_getnext(tgscan);

    if (!HeapTupleIsValid(tuple)) {
        elog(ERROR, "could not find tuple for trigger %u", childTrigId);
    }

    newtup = heap_copytuple(tuple);
    trigForm = (Form_pg_trigger) GETSTRUCT(newtup);

    if (OidIsValid(parentTrigId)) {
        // Setting parent relationship
        if (OidIsValid(trigForm->tgparentid)) {
            elog(ERROR, "trigger %u already has a parent trigger", childTrigId);
        }

        // Update the parent ID
        trigForm->tgparentid = parentTrigId;
        CatalogTupleUpdate(trigRel, &tuple->t_self, newtup);

        // Create partition dependencies
        ObjectAddressSet(depender, TriggerRelationId, childTrigId);
        ObjectAddressSet(referenced, TriggerRelationId, parentTrigId);
        recordDependencyOn(&depender, &referenced, DEPENDENCY_PARTITION_PRI);

        ObjectAddressSet(referenced, RelationRelationId, childTableId);
        recordDependencyOn(&depender, &referenced, DEPENDENCY_PARTITION_SEC);
    } else {
        // Removing parent relationship
        trigForm->tgparentid = InvalidOid;
        CatalogTupleUpdate(trigRel, &tuple->t_self, newtup);

        // Remove partition dependencies
        deleteDependencyRecordsForClass(TriggerRelationId, childTrigId,
                                       TriggerRelationId, DEPENDENCY_PARTITION_PRI);
        deleteDependencyRecordsForClass(TriggerRelationId, childTrigId,
                                       RelationRelationId, DEPENDENCY_PARTITION_SEC);
    }

    heap_freetuple(newtup);
    systable_endscan(tgscan);
}
```