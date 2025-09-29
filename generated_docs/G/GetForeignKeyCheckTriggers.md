# GetForeignKeyCheckTriggers

## Location
[src/backend/commands/tablecmds.c:11350-11415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11350-L11415)

## Overview
GetForeignKeyCheckTriggers retrieves the insert and update check triggers associated with a foreign key constraint on the referencing (foreign key) side.

## Definition

```c
static void
GetForeignKeyCheckTriggers(Relation trigrel,
						   Oid conoid, Oid confrelid, Oid conrelid,
						   Oid *insertTriggerOid,
						   Oid *updateTriggerOid)
```
## Detailed Description
This function searches the pg_trigger catalog to find the check triggers (ON INSERT and ON UPDATE) that belong to a specific foreign key constraint. Check triggers are created on the referencing table (foreign key side) and are responsible for validating that inserted or updated foreign key values exist in the referenced table.

The function performs a catalog scan looking for triggers that:
1. Belong to the specified constraint (tgconstraint = conoid)
2. Are located on the referencing table (tgrelid = conrelid)
3. Reference the referenced table (tgconstrrelid = confrelid)
4. Are classified as foreign key (check) triggers using RI_FKey_trigger_type
5. Handle either INSERT or UPDATE events

The function ensures that exactly one insert trigger and one update trigger are found, as every foreign key constraint must have both types of check triggers on the referencing side to validate foreign key values.

## Parameters / Member Variables
- : Open relation handle for the pg_trigger catalog
- : OID of the foreign key constraint to search for
- : OID of the referenced table
- : OID of the referencing table (where check triggers reside)
- : Output parameter for the ON INSERT check trigger OID
- : Output parameter for the ON UPDATE check trigger OID

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md): Initialize scan key for catalog search
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext/systable_endscan: Scan pg_trigger catalog
  - [RI_FKey_trigger_type](../R/RI_FKey_trigger_type.md): Determine trigger type (check vs action)
  - TRIGGER_FOR_INSERT/TRIGGER_FOR_UPDATE: Check trigger event type
  - elog: Report errors if triggers not found

- Called from:
  - [CloneFkReferencing](../C/CloneFkReferencing.md): When cloning FK constraints to partitions on the referencing side
  - [tryAttachPartitionForeignKey](../t/tryAttachPartitionForeignKey.md): When attaching existing partition FK constraints to parent
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md): During partition detachment operations

## Notes and Other Information
- Check triggers execute on the referencing table when rows are inserted or updated
- These triggers validate that foreign key values exist in the referenced table
- The function validates that exactly one insert and one update trigger exist
- Uses TriggerConstraintIndexId for efficient catalog scanning
- In assert-enabled builds, continues scanning to detect duplicate triggers
- Check triggers are the counterpart to action triggers (which reside on the referenced table)
- These trigger OIDs are used for establishing parent-child relationships during partition operations
- Part of PostgreSQL's referential integrity enforcement system

## Simplified Source

```c
static void GetForeignKeyCheckTriggers(Relation trigrel,
                                     Oid conoid, Oid confrelid, Oid conrelid,
                                     Oid *insertTriggerOid,
                                     Oid *updateTriggerOid)
{
    ScanKeyData key;
    SysScanDesc scan;
    HeapTuple trigtup;

    // Initialize output parameters
    *insertTriggerOid = *updateTriggerOid = InvalidOid;

    // Set up scan key to find triggers for this constraint
    ScanKeyInit(&key, Anum_pg_trigger_tgconstraint,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(conoid));

    // Scan pg_trigger catalog for constraint triggers
    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true,
                             NULL, 1, &key);

    while ((trigtup = systable_getnext(scan)) != NULL) {
        Form_pg_trigger trgform = (Form_pg_trigger) GETSTRUCT(trigtup);

        // Skip triggers not on the correct tables
        if (trgform->tgconstrrelid != confrelid) continue;
        if (trgform->tgrelid != conrelid) continue;

        // Only look at "check" triggers on the FK side
        if (RI_FKey_trigger_type(trgform->tgfoid) != RI_TRIGGER_FK)
            continue;

        // Identify INSERT or UPDATE check triggers
        if (TRIGGER_FOR_INSERT(trgform->tgtype)) {
            Assert(*insertTriggerOid == InvalidOid);
            *insertTriggerOid = trgform->oid;
        }
        else if (TRIGGER_FOR_UPDATE(trgform->tgtype)) {
            Assert(*updateTriggerOid == InvalidOid);
            *updateTriggerOid = trgform->oid;
        }

        // Early exit if both triggers found (except in assert builds)
        #ifndef USE_ASSERT_CHECKING
        if (OidIsValid(*insertTriggerOid) && OidIsValid(*updateTriggerOid))
            break;
        #endif
    }

    systable_endscan(scan);

    // Verify both triggers were found
    if (!OidIsValid(*insertTriggerOid))
        elog(ERROR, "could not find ON INSERT check triggers of foreign key constraint %u", conoid);
    if (!OidIsValid(*updateTriggerOid))
        elog(ERROR, "could not find ON UPDATE check triggers of foreign key constraint %u", conoid);
}
```