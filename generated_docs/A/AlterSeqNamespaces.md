# AlterSeqNamespaces

## Location
src/backend/commands/tablecmds.c: 17437 - 17521

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
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - relation_open
  - RelationGetForm
  - relation_close
  - AlterRelationNamespaceInternal
  - systable_endscan
- Called from (representative examples):
  - AlterTableNamespaceInternal

## Notes and Other Information
- Focuses specifically on SERIAL and IDENTITY sequences with auto/internal dependencies
- Performs thorough validation to ensure only actual sequences are processed
- Sequences no longer have pg_type entries, simplifying the migration process
- Maintains lock consistency by keeping sequence locks until transaction end
- Static function scope limits usage to within tablecmds.c
- Critical for maintaining referential integrity during ALTER TABLE SET SCHEMA operations