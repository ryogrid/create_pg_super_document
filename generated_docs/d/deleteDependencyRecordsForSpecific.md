# deleteDependencyRecordsForSpecific

## Location
src/backend/catalog/pg_depend.c: 399 - 457

## Overview
Deletes all dependency records that match exact depender and dependee object specifications with a specific dependency type, returning the count of deleted records.

## Definition
```c
long deleteDependencyRecordsForSpecific(Oid classId, Oid objectId, char deptype, Oid refclassId, Oid refobjectId)
```

## Detailed Description
This function provides precise deletion of dependency records by matching both the complete depender object specification (classId, objectId) and the complete dependee object specification (refclassId, refobjectId), along with a specific dependency type. This level of specificity makes it ideal for scenarios where you need to remove exact dependency relationships between two known objects.

The function scans the pg_depend catalog table using the depender object as the primary search key, then filters results to find records that also match the specified dependee object and dependency type. This approach is more specific than deleteDependencyRecordsForClass, as it targets individual dependee objects rather than entire classes of objects.

## Parameters / Member Variables
- `classId`: OID of the catalog table containing the depender object
- `objectId`: OID of the specific depender object
- `deptype`: Character code specifying the type of dependency relationship to delete
- `refclassId`: OID of the catalog table containing the dependee object
- `refobjectId`: OID of the specific dependee object

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
  - [ExecAlterObjectDependsStmt](../E/ExecAlterObjectDependsStmt.md) (src/backend/commands/alter.c:493)
  - [tryAttachPartitionForeignKey](../t/tryAttachPartitionForeignKey.md) (src/backend/commands/tablecmds.c:11244)

## Notes and Other Information
- Provides the most specific form of dependency record deletion, requiring exact matches for both depender and dependee objects
- Uses the same scanning strategy as deleteDependencyRecordsForClass but with additional filtering on the specific dependee object
- Particularly useful in scenarios involving ALTER ... [NO] DEPENDS ON commands where precise dependency relationships need to be established or removed
- The function's precision makes it suitable for operations that modify specific object-to-object relationships without affecting broader dependency patterns