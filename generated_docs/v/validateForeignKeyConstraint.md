# validateForeignKeyConstraint

## Location
[src/backend/commands/tablecmds.c:12241-12337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12241-L12337)

## Overview
Validates that all existing rows in a table satisfy a newly proposed foreign key constraint by checking referential integrity against the referenced table.

## Definition

```c
structure;
```
## Detailed Description
This function performs a comprehensive validation of existing table data against a proposed foreign key constraint. It employs a two-phase validation strategy: first attempting an optimized LEFT JOIN query approach through RI_Initial_Check(), and if that approach is not feasible, falling back to a tuple-by-tuple validation method. The tuple-by-tuple method simulates INSERT trigger execution for each existing row, calling RI_FKey_check_ins() to verify that each row's foreign key values have corresponding references in the primary key table.

The function uses proper memory management with a per-tuple memory context to prevent memory leaks during large table scans, and includes interrupt checking to allow for query cancellation during long-running validations.

## Parameters / Member Variables
- : Name of the foreign key constraint being validated
- : The referencing relation (table containing the foreign key)
- : The referenced relation (table containing the primary key)
- : OID of the unique index supporting the primary key constraint  
- : OID of the constraint being validated

## Dependencies
- Functions called/Symbols referenced:
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_FKey_check_ins](../R/RI_FKey_check_ins.md)
  - [table_beginscan](../t/table_beginscan.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - [table_endscan](../t/table_endscan.md)
  - [RegisterSnapshot](../R/RegisterSnapshot.md)
  - [GetLatestSnapshot](../G/GetLatestSnapshot.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
  - AllocSetContextCreate
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)
- Called from (representative examples):
  - [ATRewriteTables](../A/ATRewriteTables.md)

## Notes and Other Information
- Uses optimized LEFT JOIN validation when possible, falls back to trigger-based validation
- Employs proper transaction isolation with snapshot management
- Includes memory context management to prevent memory bloat during large scans
- Supports query cancellation through CHECK_FOR_INTERRUPTS()
- Part of the table rewriting process during ALTER TABLE operations
- Simulates INSERT trigger behavior to validate foreign key constraints on existing data

## Simplified Source

```c
static void
validateForeignKeyConstraint(char *conname,
                             Relation rel,
                             Relation pkrel,
                             Oid pkindOid,
                             Oid constraintOid)
{
    TupleTableSlot *slot;
    TableScanDesc scan;
    Trigger trig = {0};
    Snapshot snapshot;
    MemoryContext perTupCxt;

    ereport(DEBUG1, (errmsg_internal("validating foreign key constraint \"%s\"", conname)));

    // Build trigger structure for validation
    trig.tgoid = InvalidOid;
    trig.tgname = conname;
    trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
    trig.tgisinternal = true;
    trig.tgconstrrelid = RelationGetRelid(pkrel);
    trig.tgconstrindid = pkindOid;
    trig.tgconstraint = constraintOid;
    trig.tgdeferrable = false;
    trig.tginitdeferred = false;

    // Try optimized LEFT JOIN validation first
    if (RI_Initial_Check(&trig, rel, pkrel))
        return;

    // Fall back to tuple-by-tuple validation
    snapshot = RegisterSnapshot(GetLatestSnapshot());
    slot = table_slot_create(rel, NULL);
    scan = table_beginscan(rel, snapshot, 0, NULL);

    // Create per-tuple memory context to prevent memory bloat
    perTupCxt = AllocSetContextCreate(CurrentMemoryContext,
                                      "validateForeignKeyConstraint",
                                      ALLOCSET_SMALL_SIZES);

    // Validate each tuple as if it were being inserted
    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        LOCAL_FCINFO(fcinfo, 0);
        TriggerData trigdata = {0};

        CHECK_FOR_INTERRUPTS();

        // Set up trigger call context
        MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));

        trigdata.type = T_TriggerData;
        trigdata.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW;
        trigdata.tg_relation = rel;
        trigdata.tg_trigtuple = ExecFetchSlotHeapTuple(slot, false, NULL);
        trigdata.tg_trigslot = slot;
        trigdata.tg_trigger = &trig;

        fcinfo->context = (Node *) &trigdata;

        // Call foreign key check function
        RI_FKey_check_ins(fcinfo);

        MemoryContextReset(perTupCxt);
    }

    // Clean up resources
    MemoryContextDelete(perTupCxt);
    table_endscan(scan);
    UnregisterSnapshot(snapshot);
    ExecDropSingleTupleTableSlot(slot);
}
```