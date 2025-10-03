# ATDetachCheckNoForeignKeyRefs

## Location
[src/backend/commands/tablecmds.c:20181-20229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L20181-L20229)

## Overview
Verifies that foreign keys pointing to a partitioned table will not become invalid during a DETACH PARTITION operation, raising an error if any referenced values exist.

## Definition

```c
static void
ATDetachCheckNoForeignKeyRefs(Relation partition)
```
## Detailed Description
This function is called during the DETACH PARTITION operation to ensure referential integrity. It checks all foreign key constraints that reference the partition being detached to verify that no foreign key references would become invalid after the partition is detached. The function retrieves all parented foreign key references to the partition and for each constraint, it performs a referential integrity check using the RI_PartitionRemove_Check function. If any referenced values exist that would become orphaned after detaching the partition, an error is raised to prevent the operation.

## Parameters / Member Variables
- : The relation (partition) that is being detached from its parent table

## Dependencies
- Functions called/Symbols referenced:
  - [GetParentedForeignKeyRefs](../G/GetParentedForeignKeyRefs.md)
  - Form_pg_constraint
  - [Trigger](../T/Trigger.md)
  - ShareLock
  - TRIGGER_FIRES_ON_ORIGIN
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
- Called from (representative examples):
  - [ATExecDetachPartition](ATExecDetachPartition.md)

## Notes and Other Information
- This is a static function within tablecmds.c, used exclusively during partition detachment operations
- The function holds a ShareLock on the referencing table to prevent data changes until commit
- It creates a temporary trigger structure to pass to RI_PartitionRemove_Check for validation
- This check is essential for maintaining referential integrity when detaching partitions that are referenced by foreign keys

## Simplified Source
```c
static void ATDetachCheckNoForeignKeyRefs(Relation partition) {
    List *constraints;
    ListCell *cell;

    // Get all foreign key constraints that reference this partition
    constraints = GetParentedForeignKeyRefs(partition);

    foreach(cell, constraints) {
        Oid constraintOid = lfirst_oid(cell);
        HeapTuple tuple;
        Form_pg_constraint constraintForm;
        Relation referencing_table;
        Trigger trig = {0};

        // Look up the constraint details
        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
        if (!HeapTupleIsValid(tuple)) {
            elog(ERROR, "cache lookup failed for constraint %u", constraintOid);
        }
        constraintForm = (Form_pg_constraint) GETSTRUCT(tuple);

        Assert(OidIsValid(constraintForm->conparentid));
        Assert(constraintForm->confrelid == RelationGetRelid(partition));

        // Lock the referencing table to prevent data changes
        referencing_table = table_open(constraintForm->conrelid, ShareLock);

        // Set up trigger structure for referential integrity check
        trig.tgoid = InvalidOid;
        trig.tgname = NameStr(constraintForm->conname);
        trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
        trig.tgisinternal = true;
        trig.tgconstrrelid = RelationGetRelid(partition);
        trig.tgconstrindid = constraintForm->conindid;
        trig.tgconstraint = constraintForm->oid;
        trig.tgdeferrable = false;
        trig.tginitdeferred = false;

        // Check for referential integrity violations
        RI_PartitionRemove_Check(&trig, referencing_table, partition);

        ReleaseSysCache(tuple);
        table_close(referencing_table, NoLock); // Keep lock until commit
    }
}
```