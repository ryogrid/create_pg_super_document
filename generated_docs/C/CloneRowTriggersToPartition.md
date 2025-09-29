# CloneRowTriggersToPartition

## Location
[src/backend/commands/tablecmds.c:18984-19140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L18984-L19140)

## Overview
CloneRowTriggersToPartition clones row-level triggers from a parent partitioned table to a newly attached partition, excluding internal triggers and statement-level triggers.

## Definition
```c
static void CloneRowTriggersToPartition(Relation parent, Relation partition)
```

## Detailed Description
This function is responsible for replicating appropriate triggers from a partitioned table to its partitions during partition attachment or relation definition. The function performs selective trigger cloning with several important considerations:

**Trigger Selection:**
- Only processes row-level triggers (excludes statement-level triggers)
- Skips internal triggers since they are handled by constraint cloning mechanisms
- Only handles BEFORE and AFTER triggers (validates trigger types)

**Trigger Reconstruction:**
The function reads trigger metadata from pg_trigger and reconstructs each trigger by:
1. **WHEN clause processing**: Transforms trigger WHEN conditions using map_partition_varattnos to adjust attribute references for both OLD and NEW row variables
2. **Column list mapping**: Converts column attribute numbers to column names for column-specific triggers  
3. **Argument reconstruction**: Rebuilds trigger function arguments from the stored bytea format
4. **Constraint relationship**: Preserves constraint trigger relationships when applicable

**Memory Management:**
Uses a per-tuple memory context that is reset after each trigger to prevent memory leaks during bulk operations.

The function creates new triggers on the partition using CreateTriggerFiringOn, maintaining the same behavior characteristics (timing, events, deferability) as the parent triggers.

## Parameters / Member Variables
- `parent`: The parent partitioned table relation from which to clone triggers
- `partition`: The partition relation where triggers should be created

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, AllocSetContextCreate
  - [heap_getattr](../h/heap_getattr.md), stringToNode, TextDatumGetCString
  - [map_partition_varattnos](../m/map_partition_varattnos.md) (for OLD and NEW variable mapping)
  - [makeString](../m/makeString.md), DatumGetByteaPP, makeNode
  - [CreateTriggerFiringOn](CreateTriggerFiringOn.md), MemoryContextReset, MemoryContextDelete
- Called from (representative examples):
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)
  - [DefineRelation](../D/DefineRelation.md)
  - child_dependency_type

## Notes and Other Information
- Static function used as a subroutine during partition setup operations
- Uses RowExclusiveLock on pg_trigger to ensure consistent trigger metadata access
- Handles both constraint triggers (tgconstraint) and regular triggers appropriately
- Maps variable attribute numbers in WHEN clauses to account for different column orders between parent and partition
- Does not clone transition table triggers (transitionRels set to NIL) as they are not currently supported on partitions
- Preserves trigger enablement state, deferability, and timing characteristics from the parent
- Uses ALLOCSET_SMALL_SIZES for memory context since trigger definitions are typically small
- Critical for maintaining trigger behavior consistency across the partition hierarchy

## Simplified Source

```c
static void CloneRowTriggersToPartition(Relation parent, Relation partition) {
    Relation pg_trigger = table_open(TriggerRelationId, RowExclusiveLock);
    ScanKeyData key;
    SysScanDesc scan;
    HeapTuple tuple;

    // Scan for triggers on the parent relation
    ScanKeyInit(&key, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(parent)));
    scan = systable_beginscan(pg_trigger, TriggerRelidNameIndexId,
                             true, NULL, 1, &key);

    MemoryContext perTupCxt = AllocSetContextCreate(CurrentMemoryContext,
                                                   "clone trig", ALLOCSET_SMALL_SIZES);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_trigger trigForm = (Form_pg_trigger) GETSTRUCT(tuple);

        // Skip statement-level and internal triggers
        if (!TRIGGER_FOR_ROW(trigForm->tgtype) || trigForm->tgisinternal)
            continue;

        // Only handle BEFORE and AFTER triggers
        if (!TRIGGER_FOR_BEFORE(trigForm->tgtype) && !TRIGGER_FOR_AFTER(trigForm->tgtype))
            elog(ERROR, "unexpected trigger \"%s\" found", NameStr(trigForm->tgname));

        MemoryContext oldcxt = MemoryContextSwitchTo(perTupCxt);

        // Process WHEN clause if present
        Node *qual = NULL;
        if (heap_getattr(tuple, Anum_pg_trigger_tgqual, RelationGetDescr(pg_trigger), &isnull)) {
            Datum value = heap_getattr(tuple, Anum_pg_trigger_tgqual,
                                      RelationGetDescr(pg_trigger), &isnull);
            if (!isnull) {
                qual = stringToNode(TextDatumGetCString(value));
                // Map attribute numbers for OLD and NEW variables
                qual = (Node *) map_partition_varattnos((List *) qual, PRS2_OLD_VARNO,
                                                       partition, parent);
                qual = (Node *) map_partition_varattnos((List *) qual, PRS2_NEW_VARNO,
                                                       partition, parent);
            }
        }

        // Build column list from attribute numbers
        List *cols = NIL;
        if (trigForm->tgattr.dim1 > 0) {
            for (int i = 0; i < trigForm->tgattr.dim1; i++) {
                Form_pg_attribute col = TupleDescAttr(parent->rd_att,
                                                     trigForm->tgattr.values[i] - 1);
                cols = lappend(cols, makeString(pstrdup(NameStr(col->attname))));
            }
        }

        // Reconstruct trigger arguments
        List *trigargs = NIL;
        if (trigForm->tgnargs > 0) {
            Datum value = heap_getattr(tuple, Anum_pg_trigger_tgargs,
                                      RelationGetDescr(pg_trigger), &isnull);
            char *p = (char *) VARDATA_ANY(DatumGetByteaPP(value));

            for (int i = 0; i < trigForm->tgnargs; i++) {
                trigargs = lappend(trigargs, makeString(pstrdup(p)));
                p += strlen(p) + 1;
            }
        }

        // Create trigger statement and execute
        CreateTrigStmt *trigStmt = makeNode(CreateTrigStmt);
        trigStmt->replace = false;
        trigStmt->isconstraint = OidIsValid(trigForm->tgconstraint);
        trigStmt->trigname = NameStr(trigForm->tgname);
        trigStmt->relation = NULL;
        trigStmt->funcname = NULL;
        trigStmt->args = trigargs;
        trigStmt->row = true;
        trigStmt->timing = trigForm->tgtype & TRIGGER_TYPE_TIMING_MASK;
        trigStmt->events = trigForm->tgtype & TRIGGER_TYPE_EVENT_MASK;
        trigStmt->columns = cols;
        trigStmt->whenClause = NULL;
        trigStmt->transitionRels = NIL;
        trigStmt->deferrable = trigForm->tgdeferrable;
        trigStmt->initdeferred = trigForm->tginitdeferred;
        trigStmt->constrrel = NULL;

        CreateTriggerFiringOn(trigStmt, NULL, RelationGetRelid(partition),
                             trigForm->tgconstrrelid, InvalidOid, InvalidOid,
                             trigForm->tgfoid, trigForm->oid, qual,
                             false, true, trigForm->tgenabled);

        MemoryContextSwitchTo(oldcxt);
        MemoryContextReset(perTupCxt);
    }

    MemoryContextDelete(perTupCxt);
    systable_endscan(scan);
    table_close(pg_trigger, RowExclusiveLock);
}
```