# deleteDependencyRecordsForClass

## Location
src/backend/catalog/pg_depend.c: 352 - 398

## Overview
Deletes all dependency records for a specific object that depend on objects of a given class and dependency type, returning the number of records deleted.

## Definition
```c
long deleteDependencyRecordsForClass(Oid classId, Oid objectId, Oid refclassId, char deptype)
```

## Detailed Description
This function is a specialized variant of deleteDependencyRecordsFor that removes dependency records based on both the depender object (identified by classId and objectId) and additional filtering criteria for the dependee class and dependency type. It's particularly useful when revoking object properties that are expressed through dependency records, such as extension membership or constraint relationships.

The function performs a systematic scan of the pg_depend catalog table, locating all records where:
- The depender matches the specified classId and objectId
- The dependee belongs to the specified refclassId
- The dependency relationship matches the specified deptype

Each matching record is deleted from the catalog, and the function returns a count of the total number of records removed.

## Parameters / Member Variables
- `classId`: OID of the catalog table containing the depender object
- `objectId`: OID of the specific depender object
- `refclassId`: OID of the catalog table that the dependee objects must belong to
- `deptype`: Character code specifying the type of dependency relationship to delete

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - GETSTRUCT
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - [SysScanDesc](../S/SysScanDesc.md)
  - Form_pg_depend

- Called from (representative examples):
  - [index_constraint_create](../i/index_constraint_create.md) (src/backend/catalog/index.c:1932)
  - [ConstraintSetParentConstraint](../C/ConstraintSetParentConstraint.md) (src/backend/catalog/pg_constraint.c:878,881)
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md) (src/backend/commands/extension.c:3244)
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md) (src/backend/commands/indexcmds.c:4418,4421)
  - [TriggerSetParentTrigger](../T/TriggerSetParentTrigger.md) (src/backend/commands/trigger.c:1270,1273)

## Notes and Other Information
- The function acquires a RowExclusiveLock on the pg_depend relation during the operation
- Uses DependDependerIndexId for efficient scanning based on depender classId and objectId
- Returns the actual count of deleted records, which can be useful for validation or logging purposes
- This is commonly used in scenarios involving parent-child relationships where specific dependency types need to be severed