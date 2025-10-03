# GetForeignKeyActionTriggers

## Location
[src/backend/commands/tablecmds.c:11289-11349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11289-L11349)

## Overview
GetForeignKeyActionTriggers retrieves the delete and update action triggers associated with a foreign key constraint on the referenced (primary key) side.

## Definition

```c
static void
GetForeignKeyActionTriggers(Relation trigrel,
							Oid conoid, Oid confrelid, Oid conrelid,
							Oid *deleteTriggerOid,
							Oid *updateTriggerOid)
```
## Detailed Description
This function searches the pg_trigger catalog to find the action triggers (ON DELETE and ON UPDATE) that belong to a specific foreign key constraint. Action triggers are created on the referenced table (primary key side) and are responsible for enforcing the foreign key's cascade, restrict, or set null actions when the referenced row is modified or deleted.

The function performs a catalog scan looking for triggers that:
1. Belong to the specified constraint (tgconstraint = conoid)
2. Are located on the referenced table (tgrelid = confrelid)
3. Reference the constraining table (tgconstrrelid = conrelid)
4. Are classified as primary key (action) triggers using RI_FKey_trigger_type
5. Handle either DELETE or UPDATE events

The function ensures that exactly one delete trigger and one update trigger are found, as every foreign key constraint must have both types of action triggers on the referenced side.

## Parameters / Member Variables
- `trigrel`: Open relation handle for the pg_trigger catalog
- `conoid`: OID of the foreign key constraint to search for
- `confrelid`: OID of the referenced table (where action triggers reside)
- `conrelid`: OID of the referencing table (constrained table)
- `*deleteTriggerOid`: Output parameter for the ON DELETE action trigger OID
- `*updateTriggerOid`: Output parameter for the ON UPDATE action trigger OID
## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md): Initialize scan key for catalog search
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext/systable_endscan: Scan pg_trigger catalog
  - [RI_FKey_trigger_type](../R/RI_FKey_trigger_type.md): Determine trigger type (action vs check)
  - TRIGGER_FOR_DELETE/TRIGGER_FOR_UPDATE: Check trigger event type
  - elog: Report errors if triggers not found

- Called from:
  - [CloneFkReferenced](../C/CloneFkReferenced.md): When cloning FK constraints to partitions on the referenced side

## Notes and Other Information
- Action triggers execute on the referenced table when rows are deleted or updated
- These triggers implement the foreign key's CASCADE, RESTRICT, SET NULL, or SET DEFAULT actions
- The function validates that exactly one delete and one update trigger exist
- Uses TriggerConstraintIndexId for efficient catalog scanning
- In assert-enabled builds, continues scanning to detect duplicate triggers
- Part of PostgreSQL's referential integrity enforcement system
- These trigger OIDs are typically used as parent triggers when creating similar triggers on partitions

## Simplified Source

```c
static void GetForeignKeyActionTriggers(Relation trigrel, Oid conoid, Oid confrelid, Oid conrelid,
                                       Oid *deleteTriggerOid, Oid *updateTriggerOid) {
    ScanKeyData key;
    SysScanDesc scan;
    HeapTuple trigtup;

    // Initialize output parameters
    *deleteTriggerOid = *updateTriggerOid = InvalidOid;

    // Set up scan to find triggers for this constraint
    ScanKeyInit(&key, Anum_pg_trigger_tgconstraint, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(conoid));

    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, NULL, 1, &key);

    // Scan through all triggers for this constraint
    while ((trigtup = systable_getnext(scan)) != NULL) {
        Form_pg_trigger trgform = (Form_pg_trigger) GETSTRUCT(trigtup);

        // Filter for triggers on the correct relations
        if (trgform->tgconstrrelid != conrelid)
            continue;
        if (trgform->tgrelid != confrelid)
            continue;

        // Only look at action triggers on the PK side
        if (RI_FKey_trigger_type(trgform->tgfoid) != RI_TRIGGER_PK)
            continue;

        // Identify DELETE or UPDATE triggers
        if (TRIGGER_FOR_DELETE(trgform->tgtype)) {
            Assert(*deleteTriggerOid == InvalidOid);
            *deleteTriggerOid = trgform->oid;
        } else if (TRIGGER_FOR_UPDATE(trgform->tgtype)) {
            Assert(*updateTriggerOid == InvalidOid);
            *updateTriggerOid = trgform->oid;
        }

        // Early exit if both triggers found (unless in assert build)
        #ifndef USE_ASSERT_CHECKING
        if (OidIsValid(*deleteTriggerOid) && OidIsValid(*updateTriggerOid))
            break;
        #endif
    }

    // Verify both triggers were found
    if (!OidIsValid(*deleteTriggerOid))
        elog(ERROR, "could not find ON DELETE action trigger of foreign key constraint %u", conoid);
    if (!OidIsValid(*updateTriggerOid))
        elog(ERROR, "could not find ON UPDATE action trigger of foreign key constraint %u", conoid);

    systable_endscan(scan);
}
```