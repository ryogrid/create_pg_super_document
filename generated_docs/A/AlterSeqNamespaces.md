# AlterSeqNamespaces

## Location
[src/backend/commands/tablecmds.c:17437-17521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17437-L17521)

## Overview
A static function that relocates all identity and SERIAL-column sequences associated with a relation when the table is moved to a different namespace.

## Definition
```c
static void AlterSeqNamespaces(Relation classRel, Relation rel,
                              Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved,
                              LOCKMODE lockmode)
```

## Detailed Description
This function handles the namespace migration of sequences that have auto or internal dependencies on table columns (typically SERIAL and IDENTITY sequences). It scans the pg_depend catalog to find sequences with the appropriate dependency relationships, validates each sequence, and uses AlterRelationNamespaceInternal to perform the actual namespace change. The function maintains proper locking throughout the operation and ensures sequences maintain their dependency relationships with the parent table.

## Parameters / Member Variables
- `classRel`: Pre-opened and write-locked pg_class relation for catalog operations
- `rel`: The parent relation whose sequences are being moved
- `oldNspOid`: Object identifier of the source namespace
- `newNspOid`: Object identifier of the destination namespace
- `objsMoved`: Collection tracking objects already processed to prevent duplicates
- `lockmode`: Lock mode to acquire on sequence relations during processing

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [relation_open](../r/relation_open.md)
  - RelationGetForm
  - [relation_close](../r/relation_close.md)
  - [AlterRelationNamespaceInternal](AlterRelationNamespaceInternal.md)
  - [systable_endscan](../s/systable_endscan.md)
- Called from (representative examples):
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)

## Notes and Other Information
- Focuses specifically on SERIAL and IDENTITY sequences with auto/internal dependencies
- Performs thorough validation to ensure only actual sequences are processed
- Sequences no longer have pg_type entries, simplifying the migration process
- Maintains lock consistency by keeping sequence locks until transaction end
- Static function scope limits usage to within tablecmds.c
- Critical for maintaining referential integrity during ALTER TABLE SET SCHEMA operations

## Simplified Source

```c
static void
AlterSeqNamespaces(Relation classRel, Relation rel,
                  Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved,
                  LOCKMODE lockmode)
{
    Relation    depRel;
    SysScanDesc scan;
    ScanKeyData key[2];
    HeapTuple   tup;

    // Open pg_depend to find sequences with auto dependencies on this table
    depRel = table_open(DependRelationId, AccessShareLock);

    // Setup scan keys to find dependencies on this relation
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));

    scan = systable_beginscan(depRel, DependReferenceIndexId, true,
                              NULL, 2, key);

    // Process each dependency entry
    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);
        Relation    seqRel;

        // Only process auto/internal dependencies on columns
        if (depForm->refobjsubid == 0 ||
            depForm->classid != RelationRelationId ||
            depForm->objsubid != 0 ||
            !(depForm->deptype == DEPENDENCY_AUTO ||
              depForm->deptype == DEPENDENCY_INTERNAL))
            continue;

        // Open the potentially dependent object
        seqRel = relation_open(depForm->objid, lockmode);

        // Skip if not actually a sequence
        if (RelationGetForm(seqRel)->relkind != RELKIND_SEQUENCE)
        {
            relation_close(seqRel, lockmode);
            continue;
        }

        // Move the sequence to the new namespace
        AlterRelationNamespaceInternal(classRel, depForm->objid,
                                       oldNspOid, newNspOid,
                                       true, objsMoved);

        // Note: sequences no longer have pg_type entries
        Assert(RelationGetForm(seqRel)->reltype == InvalidOid);

        relation_close(seqRel, NoLock);
    }

    systable_endscan(scan);
    relation_close(depRel, AccessShareLock);
}
```