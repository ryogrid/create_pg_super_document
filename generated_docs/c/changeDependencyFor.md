# changeDependencyFor

## Location
src/backend/catalog/pg_depend.c: 458 - 565

## Overview
Adjusts dependency records to point from a specific referencing object to a different referenced object of the same type, handling special cases for pinned objects.

## Definition
```c
long changeDependencyFor(Oid classId, Oid objectId, Oid refClassId, Oid oldRefObjectId, Oid newRefObjectId)
```

## Detailed Description
This function modifies existing dependency records to redirect them from one referenced object to another while maintaining the same referencing object. It's particularly useful in scenarios like namespace changes, object renames, or when objects are replaced with equivalent ones.

The function handles several special cases involving pinned objects (system objects that cannot be dropped):
- If both old and new objects are pinned, no action is needed (returns 1 for success)  
- If only the old object is pinned, creates a new normal dependency record for the new object
- If only the new object is pinned, deletes the existing dependency record
- For normal cases, updates the existing records to point to the new object

The function processes all matching dependency records, including those with subobject references, ensuring complete redirection of dependencies.

## Parameters / Member Variables
- `classId`: OID of the catalog table containing the referencing object
- `objectId`: OID of the referencing object whose dependencies need to be updated
- `refClassId`: OID of the catalog table containing both old and new referenced objects
- `oldRefObjectId`: OID of the current referenced object to be replaced
- `newRefObjectId`: OID of the new referenced object to point dependencies to

## Dependencies
- Functions called/Symbols referenced:
  - isObjectPinned
  - recordDependencyOn
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - GETSTRUCT
  - CatalogTupleDelete
  - heap_copytuple
  - CatalogTupleUpdate
  - heap_freetuple
  - systable_endscan
  - table_close
  - SysScanDesc
  - Form_pg_depend
  - DEPENDENCY_NORMAL

- Called from (representative examples):
  - AlterObjectNamespace_internal (src/backend/commands/alter.c:811)
  - swap_relation_files (src/backend/commands/cluster.c:1275,1283)
  - AlterExtensionNamespace (src/backend/commands/extension.c:2971)
  - AlterFunction (src/backend/commands/functioncmds.c:1449)
  - AlterRelationNamespaceInternal (src/backend/commands/tablecmds.c:17365)

## Notes and Other Information
- Returns the number of records updated; zero indicates a potential problem since at least one record should normally exist
- Assumes NORMAL dependency type when creating new records for previously pinned objects
- Handles all subobject references automatically without requiring explicit objsubid parameters  
- Used extensively in DDL operations that change object relationships, such as ALTER ... SET SCHEMA commands
- The function's logic ensures that the dependency system remains consistent even when dealing with the complexities of pinned vs. unpinned objects