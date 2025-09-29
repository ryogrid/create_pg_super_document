# tryAttachPartitionForeignKey

## Location
[src/backend/commands/tablecmds.c:11071-11288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11071-L11288)

## Overview
tryAttachPartitionForeignKey attempts to attach an existing foreign key constraint in a partition to a parent constraint, avoiding the need to create duplicate constraints.

## Definition

```c
static bool
tryAttachPartitionForeignKey(ForeignKeyCacheInfo *fk,
							 Oid partRelid,
							 Oid parentConstrOid,
							 int numfks,
							 AttrNumber *mapped_conkey,
							 AttrNumber *confkey,
							 Oid *conpfeqop,
							 Oid parentInsTrigger,
							 Oid parentUpdTrigger,
							 Relation trigrel)
```
## Detailed Description
This function compares an existing foreign key constraint in a partition (represented by ForeignKeyCacheInfo) with a parent constraint to determine if they are equivalent and can be linked. If they match, it establishes the parent-child relationship between the constraints and performs necessary cleanup.

The function performs a comprehensive comparison of constraint properties including:
- Referenced relation OID and number of key columns
- Column mappings (referencing and referenced columns)
- Operator equality functions
- Constraint attributes (deferrability, validation status, actions, etc.)

If the constraints are equivalent, the function:
1. Removes redundant action triggers from the partition (since parent triggers handle the partition)
2. Establishes the parent-child constraint relationship using ConstraintSetParentConstraint
3. Links partition check triggers to parent triggers using TriggerSetParentTrigger
4. If the referenced table is partitioned, removes extra constraint and trigger records that are no longer needed

## Parameters / Member Variables
- : ForeignKeyCacheInfo structure containing details of the partition's existing FK constraint
- : OID of the partition relation
- : OID of the parent constraint to potentially attach to
- : Number of foreign key columns
- : Array of referencing column numbers mapped to partition's column layout
- : Array of referenced column numbers
- : Array of equality operator OIDs for foreign key columns
- : OID of parent's insert trigger
- : OID of parent's update trigger  
- : Open relation handle for pg_trigger catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Look up constraint tuples in system cache
  - [ConstraintSetParentConstraint](../C/ConstraintSetParentConstraint.md): Establish parent-child constraint relationship
  - [GetForeignKeyCheckTriggers](../G/GetForeignKeyCheckTriggers.md): Retrieve check trigger OIDs from partition constraint
  - [TriggerSetParentTrigger](../T/TriggerSetParentTrigger.md): Link partition triggers to parent triggers
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)/deleteDependencyRecordsForSpecific: Remove dependency records
  - [performDeletion](../p/performDeletion.md)/performMultipleDeletions: Delete redundant triggers and constraints
  - [get_rel_relkind](../g/get_rel_relkind.md): Check if referenced table is partitioned
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Make catalog changes visible

- Called from:
  - [CloneFkReferencing](../C/CloneFkReferencing.md): During partition FK constraint cloning process
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md): During recursive FK constraint processing

## Notes and Other Information
- Returns true if attachment successful, false if constraints are not compatible
- Performs extensive validation to ensure constraints are truly equivalent before attachment
- Handles cleanup of redundant triggers and constraints automatically
- Critical optimization that avoids creating duplicate FK constraints during partition operations
- Includes special handling for partitioned referenced tables to clean up extra constraint records
- Uses multiple CommandCounterIncrement calls to ensure catalog visibility during multi-step operations

## Simplified Source

```c
static bool tryAttachPartitionForeignKey(ForeignKeyCacheInfo *fk, Oid partRelid,
                                        Oid parentConstrOid, int numfks,
                                        AttrNumber *mapped_conkey, AttrNumber *confkey,
                                        Oid *conpfeqop, Oid parentInsTrigger,
                                        Oid parentUpdTrigger, Relation trigrel) {
    HeapTuple parentConstrTup, partcontup;
    Form_pg_constraint parentConstr, partConstr;
    ScanKeyData key;
    SysScanDesc scan;
    HeapTuple trigtup;
    Oid insertTriggerOid, updateTriggerOid;

    // Look up parent constraint
    parentConstrTup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parentConstrOid));
    if (!HeapTupleIsValid(parentConstrTup))
        elog(ERROR, "cache lookup failed for constraint %u", parentConstrOid);
    parentConstr = (Form_pg_constraint) GETSTRUCT(parentConstrTup);

    // Quick compatibility checks
    if (fk->confrelid != parentConstr->confrelid || fk->nkeys != numfks) {
        ReleaseSysCache(parentConstrTup);
        return false;
    }

    // Check column and operator compatibility
    for (int i = 0; i < numfks; i++) {
        if (fk->conkey[i] != mapped_conkey[i] ||
            fk->confkey[i] != confkey[i] ||
            fk->conpfeqop[i] != conpfeqop[i]) {
            ReleaseSysCache(parentConstrTup);
            return false;
        }
    }

    // Look up partition constraint details
    partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(fk->conoid));
    if (!HeapTupleIsValid(partcontup))
        elog(ERROR, "cache lookup failed for constraint %u", fk->conoid);
    partConstr = (Form_pg_constraint) GETSTRUCT(partcontup);

    // Check constraint properties compatibility
    if (OidIsValid(partConstr->conparentid) ||
        !partConstr->convalidated ||
        partConstr->condeferrable != parentConstr->condeferrable ||
        partConstr->condeferred != parentConstr->condeferred ||
        partConstr->confupdtype != parentConstr->confupdtype ||
        partConstr->confdeltype != parentConstr->confdeltype ||
        partConstr->confmatchtype != parentConstr->confmatchtype) {
        ReleaseSysCache(parentConstrTup);
        ReleaseSysCache(partcontup);
        return false;
    }

    ReleaseSysCache(partcontup);
    ReleaseSysCache(parentConstrTup);

    // Constraints are compatible - proceed with attachment

    // Remove redundant action triggers from partition
    ScanKeyInit(&key, Anum_pg_trigger_tgconstraint, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(fk->conoid));
    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, NULL, 1, &key);

    while ((trigtup = systable_getnext(scan)) != NULL) {
        Form_pg_trigger trgform = (Form_pg_trigger) GETSTRUCT(trigtup);
        ObjectAddress trigger;

        if (trgform->tgconstrrelid != fk->conrelid)
            continue;
        if (trgform->tgrelid != fk->confrelid)
            continue;

        // Remove dependency and delete the trigger
        deleteDependencyRecordsFor(TriggerRelationId, trgform->oid, false);
        CommandCounterIncrement();

        ObjectAddressSet(trigger, TriggerRelationId, trgform->oid);
        performDeletion(&trigger, DROP_RESTRICT, 0);
        CommandCounterIncrement();
    }
    systable_endscan(scan);

    // Set parent constraint relationship
    ConstraintSetParentConstraint(fk->conoid, parentConstrOid, partRelid);

    // Attach partition check triggers to parent triggers
    GetForeignKeyCheckTriggers(trigrel, fk->conoid, fk->confrelid, fk->conrelid,
                              &insertTriggerOid, &updateTriggerOid);

    TriggerSetParentTrigger(trigrel, insertTriggerOid, parentInsTrigger, partRelid);
    TriggerSetParentTrigger(trigrel, updateTriggerOid, parentUpdTrigger, partRelid);

    // Clean up extra constraints/triggers if referenced table is partitioned
    if (get_rel_relkind(fk->confrelid) == RELKIND_PARTITIONED_TABLE) {
        // Remove subsidiary constraint records and their triggers
        // (implementation simplified - involves scanning and deleting child constraints)
    }

    CommandCounterIncrement();
    return true;
}
```