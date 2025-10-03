# RemoveInheritance

## Location
[src/backend/commands/tablecmds.c:16266-16433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16266-L16433)

## Overview
Removes inheritance relationship between a child and parent table by adjusting column and constraint inheritance counters and removing catalog entries.

## Definition

```c
static void
RemoveInheritance(Relation child_rel, Relation parent_rel, bool expect_detached)
```
## Detailed Description
RemoveInheritance implements the core logic for breaking inheritance relationships between tables. It performs several critical operations: deletes the pg_inherits tuple, decrements attinhcount for inherited attributes and sets attislocal to true when the count reaches zero, similarly handles inherited check constraints by decrementing coninhcount and setting conislocal appropriately, and removes dependency entries between the child and parent relations. The function maintains PostgreSQL's inheritance semantics where once a column becomes local (attislocal=true), it remains local even if inheritance is re-established later, preventing unexpected data loss from automatic column drops.

## Parameters / Member Variables
- `child_rel`: The child relation from which inheritance is being removed
- `parent_rel`: The parent relation being removed from the inheritance hierarchy
- `expect_detached`: Flag passed to DeleteInheritsTuple indicating whether the inheritance tuple is expected to be marked as detached
## Dependencies
- Functions called/Symbols referenced:
  - [DeleteInheritsTuple](../D/DeleteInheritsTuple.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [SearchSysCacheExistsAttName](../S/SearchSysCacheExistsAttName.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [drop_parent_dependency](../d/drop_parent_dependency.md)
  - child_dependency_type
  - InvokeObjectPostAlterHookArg
- Called from (representative examples):
  - [ATExecDropInherit](../A/ATExecDropInherit.md)
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)

## Notes and Other Information
- Used by both ATExecDropInherit (regular inheritance) and ATExecDetachPartition (partition detachment)
- Handles different error messages for partitioned tables vs regular inheritance relationships
- Processes both attributes and check constraints, ensuring their inheritance counters are properly decremented
- Uses name matching for constraint inheritance removal, assuming expression matching follows
- Sets attislocal/conislocal to true when inheritance count reaches zero, ensuring columns/constraints become permanently local
- Invokes post-alter hooks with the parent relation OID as auxiliary information
- Maintains catalog consistency by operating under RowExclusiveLock on affected system catalogs

## Simplified Source
```c
static void RemoveInheritance(Relation child_rel, Relation parent_rel, bool expect_detached) {
    Relation catalogRelation;
    SysScanDesc scan;
    ScanKeyData key[3];
    HeapTuple attributeTuple, constraintTuple;
    List *parent_constraint_names;
    bool found;
    bool is_partitioning = (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

    // Remove the inheritance tuple from pg_inherits
    found = DeleteInheritsTuple(RelationGetRelid(child_rel),
                               RelationGetRelid(parent_rel),
                               expect_detached,
                               RelationGetRelationName(child_rel));
    if (!found) {
        if (is_partitioning) {
            ereport(ERROR, "relation is not a partition of relation");
        } else {
            ereport(ERROR, "relation is not a parent of relation");
        }
    }

    // Process child columns: decrement inheritance count and set local flag
    catalogRelation = table_open(AttributeRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(child_rel)));
    scan = systable_beginscan(catalogRelation, AttributeRelidNumIndexId, true, NULL, 1, key);

    while (HeapTupleIsValid(attributeTuple = systable_getnext(scan))) {
        Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);

        // Skip dropped columns or non-inherited columns
        if (att->attisdropped || att->attinhcount <= 0) {
            continue;
        }

        // Check if this column exists in parent
        if (SearchSysCacheExistsAttName(RelationGetRelid(parent_rel), NameStr(att->attname))) {
            HeapTuple copyTuple = heap_copytuple(attributeTuple);
            Form_pg_attribute copy_att = (Form_pg_attribute) GETSTRUCT(copyTuple);

            copy_att->attinhcount--;
            if (copy_att->attinhcount == 0) {
                copy_att->attislocal = true; // Column becomes local when no longer inherited
            }

            CatalogTupleUpdate(catalogRelation, &copyTuple->t_self, copyTuple);
            heap_freetuple(copyTuple);
        }
    }

    systable_endscan(scan);
    table_close(catalogRelation, RowExclusiveLock);

    // Get list of parent's check constraint names
    catalogRelation = table_open(ConstraintRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(parent_rel)));
    scan = systable_beginscan(catalogRelation, ConstraintRelidTypidNameIndexId, true, NULL, 1, key);

    parent_constraint_names = NIL;
    while (HeapTupleIsValid(constraintTuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(constraintTuple);
        if (con->contype == CONSTRAINT_CHECK) {
            parent_constraint_names = lappend(parent_constraint_names, pstrdup(NameStr(con->conname)));
        }
    }
    systable_endscan(scan);

    // Process child constraints: decrement inheritance count for matching names
    ScanKeyInit(&key[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(child_rel)));
    scan = systable_beginscan(catalogRelation, ConstraintRelidTypidNameIndexId, true, NULL, 1, key);

    while (HeapTupleIsValid(constraintTuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(constraintTuple);
        bool name_match = false;

        if (con->contype != CONSTRAINT_CHECK) {
            continue;
        }

        // Check if constraint name matches any parent constraint
        foreach_ptr(char, parent_name, parent_constraint_names) {
            if (strcmp(NameStr(con->conname), parent_name) == 0) {
                name_match = true;
                break;
            }
        }

        if (name_match) {
            HeapTuple copyTuple = heap_copytuple(constraintTuple);
            Form_pg_constraint copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);

            if (copy_con->coninhcount <= 0) {
                elog(ERROR, "relation has non-inherited constraint");
            }

            copy_con->coninhcount--;
            if (copy_con->coninhcount == 0) {
                copy_con->conislocal = true; // Constraint becomes local
            }

            CatalogTupleUpdate(catalogRelation, &copyTuple->t_self, copyTuple);
            heap_freetuple(copyTuple);
        }
    }

    systable_endscan(scan);
    table_close(catalogRelation, RowExclusiveLock);

    // Remove dependency between child and parent
    drop_parent_dependency(RelationGetRelid(child_rel), RelationRelationId,
                          RelationGetRelid(parent_rel), child_dependency_type(is_partitioning));

    // Invoke post-alter hook
    InvokeObjectPostAlterHookArg(InheritsRelationId, RelationGetRelid(child_rel), 0,
                                RelationGetRelid(parent_rel), false);
}
```