# MarkInheritDetached

## Location
[src/backend/commands/tablecmds.c:16183-16265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16183-L16265)

## Overview
Marks a partition as pending detach in concurrent mode for ATExecDetachPartition, while ensuring no other partitions are already pending detach.

## Definition

```c
static void
MarkInheritDetached(Relation child_rel, Relation parent_rel)
```
## Detailed Description
MarkInheritDetached is a utility function used by the concurrent partition detachment process. It scans all inheritance entries for a given parent table to find the specified child partition and sets its inhdetachpending flag to true in the pg_inherits catalog. During this process, it also validates that no other partition of the same parent table is already marked as pending detach, as PostgreSQL allows only one concurrent detach operation per partitioned table at a time. The function operates under a RowExclusiveLock on the pg_inherits catalog to ensure consistency during the concurrent operation.

## Parameters / Member Variables
- : The partition relation that is being marked for detachment
- : The partitioned table relation from which the child is being detached

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [get_namespace_name](../g/get_namespace_name.md)
- Called from (representative examples):
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)

## Notes and Other Information
- Requires that the parent relation is a partitioned table (asserted with relkind check)
- Scans all partitions of the parent table to ensure only one detach operation is pending at a time
- Uses InheritsParentIndexId for efficient scanning of pg_inherits entries
- Sets the inhdetachpending flag in the pg_inherits catalog entry for the specified child partition
- Provides helpful error messages with suggestions to use FINALIZE if another partition is already pending detach
- Validates that the child relation is actually a partition of the specified parent before proceeding

## Simplified Source
```c
static void MarkInheritDetached(Relation child_rel, Relation parent_rel) {
    Relation catalogRelation;
    SysScanDesc scan;
    ScanKeyData key;
    HeapTuple inheritsTuple;
    bool found = false;

    Assert(parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

    // Scan all inheritance entries for this parent
    catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);
    ScanKeyInit(&key, Anum_pg_inherits_inhparent, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(parent_rel)));
    scan = systable_beginscan(catalogRelation, InheritsParentIndexId, true, NULL, 1, &key);

    while (HeapTupleIsValid(inheritsTuple = systable_getnext(scan))) {
        Form_pg_inherits inhForm = (Form_pg_inherits) GETSTRUCT(inheritsTuple);

        // Check if any other partition is already pending detach
        if (inhForm->inhdetachpending) {
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("partition \"%s\" already pending detach in partitioned table \"%s.%s\"",
                            get_rel_name(inhForm->inhrelid),
                            get_namespace_name(parent_rel->rd_rel->relnamespace),
                            RelationGetRelationName(parent_rel)),
                     errhint("Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation.")));
        }

        // Mark this child as pending detach if it's the target
        if (inhForm->inhrelid == RelationGetRelid(child_rel)) {
            HeapTuple newtup = heap_copytuple(inheritsTuple);
            ((Form_pg_inherits) GETSTRUCT(newtup))->inhdetachpending = true;

            CatalogTupleUpdate(catalogRelation, &inheritsTuple->t_self, newtup);
            found = true;
            heap_freetuple(newtup);
            // Continue scanning to check for other pending detaches
        }
    }

    systable_endscan(scan);
    table_close(catalogRelation, RowExclusiveLock);

    // Error if the child relation was not found as a partition
    if (!found) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("relation \"%s\" is not a partition of relation \"%s\"",
                        RelationGetRelationName(child_rel),
                        RelationGetRelationName(parent_rel))));
    }
}
```