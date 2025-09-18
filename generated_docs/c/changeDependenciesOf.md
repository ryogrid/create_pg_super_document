# changeDependenciesOf

## Location
src/backend/catalog/pg_depend.c: 566 - 621

## Overview
Adjusts all dependency records to redirect them from an old referencing object to a new referencing object of the same type, effectively transferring all dependencies.

## Definition
```c
long changeDependenciesOf(Oid classId, Oid oldObjectId, Oid newObjectId)
```

## Detailed Description
This function performs a bulk transfer of dependency ownership by updating all dependency records where a specific object acts as the depender (referencing object). It changes the source of all dependencies from the old object to the new object while preserving all other aspects of the dependency relationships, including the referenced objects, dependency types, and subobject references.

The function systematically scans the pg_depend catalog to find all records where the old object is the depender, then updates each record to point to the new object as the depender. This operation is typically used during object replacement scenarios where a new object needs to inherit all the dependency relationships of an existing object.

## Parameters / Member Variables
- `classId`: OID of the catalog table containing both old and new referencing objects
- `oldObjectId`: OID of the existing referencing object whose dependencies will be transferred
- `newObjectId`: OID of the new referencing object that will inherit all dependencies

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - heap_copytuple
  - GETSTRUCT
  - CatalogTupleUpdate
  - heap_freetuple
  - systable_endscan
  - table_close
  - SysScanDesc
  - Form_pg_depend

- Called from (representative examples):
  - index_concurrently_swap (src/backend/catalog/index.c:1788,1791)

## Notes and Other Information
- Updates all dependency records originating from the old object, regardless of dependency type or target
- Returns the total number of records updated, which indicates how many dependencies were transferred
- Primarily used in concurrent index operations where a new index needs to assume all dependencies of an existing index
- The function preserves the complete dependency relationship structure, only changing the identity of the depender object
- Does not handle pinned object considerations like changeDependencyFor, as it deals with transferring existing relationships rather than creating/modifying individual dependencies