# ATExecAlterConstrRecurse

## Location
[src/backend/commands/tablecmds.c:11553-11703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11553-L11703)

## Overview
ATExecAlterConstrRecurse is a recursive subroutine of ATExecAlterConstraint that performs the actual constraint modification work, including updating constraint and trigger catalog entries and recursively processing child constraints in partitioned tables.

## Definition

```c
static bool
ATExecAlterConstrRecurse(Constraint *cmdcon, Relation conrel, Relation tgrel,
						 Relation rel, HeapTuple contuple, List **otherrelids,
						 LOCKMODE lockmode)
```
## Detailed Description
This function handles the core logic of constraint alteration by updating both constraint and trigger catalog entries. It modifies the deferrability and initial deferred status of foreign key constraints in the pg_constraint catalog, then updates the corresponding triggers in pg_trigger that implement the constraint. The function also handles partitioned tables by recursively processing all child constraints to ensure consistency across the partition hierarchy.

Key operations include:
1. Updates the constraint tuple in pg_constraint if attributes have changed
2. Scans and updates all related triggers that implement the constraint
3. Tracks other relations involved for cache invalidation
4. Recursively processes child constraints in partitioned table hierarchies
5. Returns whether any changes were actually made

## Parameters / Member Variables
- `*cmdcon`: The constraint specification containing new attribute values
- `conrel`: Open relation handle for the pg_constraint catalog
- `tgrel`: Open relation handle for the pg_trigger catalog
- `rel`: The relation containing the constraint being altered
- `contuple`: The constraint tuple from pg_constraint being modified
- `**otherrelids`: List to collect OIDs of other relations with affected triggers
- `lockmode`: Lock mode to use when opening child relations
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - [ATExecAlterConstrRecurse](ATExecAlterConstrRecurse.md) (recursive self-call)
- Called from (representative examples):
  - [ATExecAlterConstraint](ATExecAlterConstraint.md) (main constraint alteration function)
  - [ATExecAlterConstrRecurse](ATExecAlterConstrRecurse.md) (recursive calls for child constraints)

## Notes and Other Information
- Only updates specific trigger types (RI_FKey_noaction_del, RI_FKey_noaction_upd, RI_FKey_check_ins, RI_FKey_check_upd)
- Uses stack depth checking to prevent overflow during deep recursion
- Must recurse even when constraint values are already correct to handle partitions that may have been altered locally
- Collects OIDs of other relations for cache invalidation to maintain consistency
- Returns true if any actual changes were made to the constraint or its triggers

## Simplified Source

```c
static bool ATExecAlterConstrRecurse(Constraint *cmdcon, Relation conrel, Relation tgrel,
                                    Relation rel, HeapTuple contuple, List **otherrelids,
                                    LOCKMODE lockmode) {
    Form_pg_constraint currcon;
    Oid conoid;
    Oid refrelid;
    bool changed = false;

    // Prevent stack overflow in deep recursion
    check_stack_depth();

    currcon = (Form_pg_constraint) GETSTRUCT(contuple);
    conoid = currcon->oid;
    refrelid = currcon->confrelid;

    // Update constraint if deferrability attributes have changed
    if (currcon->condeferrable != cmdcon->deferrable ||
        currcon->condeferred != cmdcon->initdeferred) {

        // Update pg_constraint tuple
        HeapTuple copyTuple = heap_copytuple(contuple);
        Form_pg_constraint copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);
        copy_con->condeferrable = cmdcon->deferrable;
        copy_con->condeferred = cmdcon->initdeferred;
        CatalogTupleUpdate(conrel, &copyTuple->t_self, copyTuple);

        InvokeObjectPostAlterHook(ConstraintRelationId, conoid, 0);
        heap_freetuple(copyTuple);
        changed = true;

        // Invalidate relcache to make changes visible
        CacheInvalidateRelcache(rel);

        // Update related triggers in pg_trigger
        ScanKeyData tgkey;
        SysScanDesc tgscan;
        HeapTuple tgtuple;

        ScanKeyInit(&tgkey, Anum_pg_trigger_tgconstraint, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(conoid));
        tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true, NULL, 1, &tgkey);

        while (HeapTupleIsValid(tgtuple = systable_getnext(tgscan))) {
            Form_pg_trigger tgform = (Form_pg_trigger) GETSTRUCT(tgtuple);

            // Track other relations for cache invalidation
            if (tgform->tgrelid != RelationGetRelid(rel))
                *otherrelids = list_append_unique_oid(*otherrelids, tgform->tgrelid);

            // Only update specific FK trigger types
            if (tgform->tgfoid != F_RI_FKEY_NOACTION_DEL &&
                tgform->tgfoid != F_RI_FKEY_NOACTION_UPD &&
                tgform->tgfoid != F_RI_FKEY_CHECK_INS &&
                tgform->tgfoid != F_RI_FKEY_CHECK_UPD)
                continue;

            // Update trigger deferrability
            HeapTuple tgCopyTuple = heap_copytuple(tgtuple);
            Form_pg_trigger copy_tg = (Form_pg_trigger) GETSTRUCT(tgCopyTuple);
            copy_tg->tgdeferrable = cmdcon->deferrable;
            copy_tg->tginitdeferred = cmdcon->initdeferred;
            CatalogTupleUpdate(tgrel, &tgCopyTuple->t_self, tgCopyTuple);

            InvokeObjectPostAlterHook(TriggerRelationId, tgform->oid, 0);
            heap_freetuple(tgCopyTuple);
        }

        systable_endscan(tgscan);
    }

    // Handle partitioned tables - recurse to child constraints
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
        get_rel_relkind(refrelid) == RELKIND_PARTITIONED_TABLE) {

        ScanKeyData pkey;
        SysScanDesc pscan;
        HeapTuple childtup;

        ScanKeyInit(&pkey, Anum_pg_constraint_conparentid, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(conoid));

        pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, NULL, 1, &pkey);

        while (HeapTupleIsValid(childtup = systable_getnext(pscan))) {
            Form_pg_constraint childcon = (Form_pg_constraint) GETSTRUCT(childtup);
            Relation childrel = table_open(childcon->conrelid, lockmode);

            // Recursively process child constraint
            ATExecAlterConstrRecurse(cmdcon, conrel, tgrel, childrel, childtup,
                                    otherrelids, lockmode);

            table_close(childrel, NoLock);
        }

        systable_endscan(pscan);
    }

    return changed;
}
```